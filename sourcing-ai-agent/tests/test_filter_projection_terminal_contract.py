"""FF-SCHEMA tests for the filter-projection product terminal contract family module.

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

from sourcing_agent.action_result_schema import (
    ActionResultActionOwner,
    ActionResultSchemaError,
    ActionResultSpec,
)
from sourcing_agent.agent_runtime_namespace_ref import (
    AGENT_RUNTIME_NAMESPACE_REF_CONTRACT_DIGEST,
    AgentRuntimeNamespaceRefError,
    agent_runtime_namespace_ref_public_record,
    canonical_json,
    mint_agent_runtime_namespace_ref,
    strict_json_loads,
)
from sourcing_agent.cohort_selection import cohort_selection_digest, cohort_selection_registry_digest
from sourcing_agent.filter_projection_terminal_contract import (
    CONTRACT_SCHEMAS,
    FILTER_PROJECTION_DEFERRED_REASON_PRECEDENCE,
    FILTER_PROJECTION_FRESHNESS_REF_V1_CONTRACT_DIGEST,
    FILTER_PROJECTION_FRESHNESS_REF_V1_ORDERED_FIELDS,
    FILTER_PROJECTION_FRESHNESS_REF_V1_OWNER,
    FILTER_PROJECTION_FRESHNESS_REF_V1_SCHEMA,
    FILTER_PROJECTION_FRESHNESS_REF_V1_SCHEMA_VERSION,
    FILTER_PROJECTION_MASKED_ABSENCE_OWNER_REF_V1_CONTRACT_DIGEST,
    FILTER_PROJECTION_MASKED_ABSENCE_OWNER_REF_V1_ORDERED_FIELDS,
    FILTER_PROJECTION_MASKED_ABSENCE_OWNER_REF_V1_OWNER,
    FILTER_PROJECTION_MASKED_ABSENCE_OWNER_REF_V1_SCHEMA,
    FILTER_PROJECTION_MASKED_ABSENCE_OWNER_REF_V1_SCHEMA_VERSION,
    FILTER_PROJECTION_NOT_READY_OWNER_REF_V1_CONTRACT_DIGEST,
    FILTER_PROJECTION_NOT_READY_OWNER_REF_V1_ORDERED_FIELDS,
    FILTER_PROJECTION_NOT_READY_OWNER_REF_V1_OWNER,
    FILTER_PROJECTION_NOT_READY_OWNER_REF_V1_SCHEMA,
    FILTER_PROJECTION_NOT_READY_OWNER_REF_V1_SCHEMA_VERSION,
    FILTER_PROJECTION_PRODUCT_REF_V1_CONTRACT_DIGEST,
    FILTER_PROJECTION_PRODUCT_REF_V1_ORDERED_FIELDS,
    FILTER_PROJECTION_PRODUCT_REF_V1_OWNER,
    FILTER_PROJECTION_PRODUCT_REF_V1_SCHEMA,
    FILTER_PROJECTION_PRODUCT_REF_V1_SCHEMA_VERSION,
    FILTER_PROJECTION_PRODUCT_TERMINAL_V1_CONTRACT_DIGEST,
    FILTER_PROJECTION_PRODUCT_TERMINAL_V1_ORDERED_FIELDS,
    FILTER_PROJECTION_PRODUCT_TERMINAL_V1_OWNER,
    FILTER_PROJECTION_PRODUCT_TERMINAL_V1_SCHEMA,
    FILTER_PROJECTION_PRODUCT_TERMINAL_V1_SCHEMA_VERSION,
    FILTER_PROJECTION_READINESS_PREREQUISITE_SET,
    FILTER_PROJECTION_READINESS_REF_V1_CONTRACT_DIGEST,
    FILTER_PROJECTION_READINESS_REF_V1_ORDERED_FIELDS,
    FILTER_PROJECTION_READINESS_REF_V1_OWNER,
    FILTER_PROJECTION_READINESS_REF_V1_SCHEMA,
    FILTER_PROJECTION_READINESS_REF_V1_SCHEMA_VERSION,
    FILTER_PROJECTION_RESULT_SERIALIZER_V3_CONTRACT_DIGEST,
    FILTER_PROJECTION_RESULT_SERIALIZER_V3_OWNER_NAME,
    FILTER_PROJECTION_RESULT_SERIALIZER_V3_REVISION,
    FILTER_PROJECTION_RESULT_V3_CONTRACT_DIGEST,
    FILTER_PROJECTION_RESULT_V3_DEFERRED_ROOT_FIELDS,
    FILTER_PROJECTION_RESULT_V3_FIELD_VALUE_ROLES,
    FILTER_PROJECTION_RESULT_V3_MASKED_ROOT_CONSTANTS,
    FILTER_PROJECTION_RESULT_V3_MASKED_ROOT_FIELDS,
    FILTER_PROJECTION_RESULT_V3_MAX_SERIALIZED_BYTES,
    FILTER_PROJECTION_RESULT_V3_ORDERED_FIELDS,
    FILTER_PROJECTION_RESULT_V3_OWNER,
    FILTER_PROJECTION_RESULT_V3_SCHEMA,
    FILTER_PROJECTION_RESULT_V3_SCHEMA_VERSION,
    FILTER_PROJECTION_RESULT_V3_SUCCESS_ROOT_FIELDS,
    FILTER_PROJECTION_RESULT_V3_VARIANTS,
    FILTER_PROJECTION_STALE_OWNER_REF_V1_CONTRACT_DIGEST,
    FILTER_PROJECTION_STALE_OWNER_REF_V1_ORDERED_FIELDS,
    FILTER_PROJECTION_STALE_OWNER_REF_V1_OWNER,
    FILTER_PROJECTION_STALE_OWNER_REF_V1_SCHEMA,
    FILTER_PROJECTION_STALE_OWNER_REF_V1_SCHEMA_VERSION,
    FILTER_PROJECTION_SUCCESS_OWNER_REF_V1_CONTRACT_DIGEST,
    FILTER_PROJECTION_SUCCESS_OWNER_REF_V1_ORDERED_FIELDS,
    FILTER_PROJECTION_SUCCESS_OWNER_REF_V1_OWNER,
    FILTER_PROJECTION_SUCCESS_OWNER_REF_V1_SCHEMA,
    FILTER_PROJECTION_SUCCESS_OWNER_REF_V1_SCHEMA_VERSION,
    REJECTED_FILTER_PROJECTION_LITERALS,
    RETAINED_FILTER_PROJECTION_HISTORY_LITERALS,
    RETAINED_LOOKUP_FORBIDDEN,
    FilterProjectionTerminalContractError,
    compute_filter_projection_freshness_ref_digest,
    compute_filter_projection_owner_ref_digest,
    compute_filter_projection_product_terminal_core_digest,
    compute_filter_projection_product_terminal_digest,
    compute_filter_projection_readiness_prerequisite_set_digest,
    compute_filter_projection_readiness_ref_digest,
    filter_projection_result_v3_variant_fields,
    parse_filter_projection_freshness_ref,
    parse_filter_projection_masked_absence_owner_ref,
    parse_filter_projection_not_ready_owner_ref,
    parse_filter_projection_product_ref,
    parse_filter_projection_product_terminal,
    parse_filter_projection_readiness_ref,
    parse_filter_projection_result_v3,
    parse_filter_projection_stale_owner_ref,
    parse_filter_projection_success_owner_ref,
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
    "filter_projection_product_terminal.v1": (
        FILTER_PROJECTION_PRODUCT_TERMINAL_V1_SCHEMA_VERSION,
        FILTER_PROJECTION_PRODUCT_TERMINAL_V1_OWNER,
        FILTER_PROJECTION_PRODUCT_TERMINAL_V1_ORDERED_FIELDS,
        FILTER_PROJECTION_PRODUCT_TERMINAL_V1_SCHEMA,
        FILTER_PROJECTION_PRODUCT_TERMINAL_V1_CONTRACT_DIGEST,
    ),
    "filter_projection_freshness_ref.v1": (
        FILTER_PROJECTION_FRESHNESS_REF_V1_SCHEMA_VERSION,
        FILTER_PROJECTION_FRESHNESS_REF_V1_OWNER,
        FILTER_PROJECTION_FRESHNESS_REF_V1_ORDERED_FIELDS,
        FILTER_PROJECTION_FRESHNESS_REF_V1_SCHEMA,
        FILTER_PROJECTION_FRESHNESS_REF_V1_CONTRACT_DIGEST,
    ),
    "filter_projection_readiness_ref.v1": (
        FILTER_PROJECTION_READINESS_REF_V1_SCHEMA_VERSION,
        FILTER_PROJECTION_READINESS_REF_V1_OWNER,
        FILTER_PROJECTION_READINESS_REF_V1_ORDERED_FIELDS,
        FILTER_PROJECTION_READINESS_REF_V1_SCHEMA,
        FILTER_PROJECTION_READINESS_REF_V1_CONTRACT_DIGEST,
    ),
    "filter_projection_success_owner_ref.v1": (
        FILTER_PROJECTION_SUCCESS_OWNER_REF_V1_SCHEMA_VERSION,
        FILTER_PROJECTION_SUCCESS_OWNER_REF_V1_OWNER,
        FILTER_PROJECTION_SUCCESS_OWNER_REF_V1_ORDERED_FIELDS,
        FILTER_PROJECTION_SUCCESS_OWNER_REF_V1_SCHEMA,
        FILTER_PROJECTION_SUCCESS_OWNER_REF_V1_CONTRACT_DIGEST,
    ),
    "filter_projection_stale_owner_ref.v1": (
        FILTER_PROJECTION_STALE_OWNER_REF_V1_SCHEMA_VERSION,
        FILTER_PROJECTION_STALE_OWNER_REF_V1_OWNER,
        FILTER_PROJECTION_STALE_OWNER_REF_V1_ORDERED_FIELDS,
        FILTER_PROJECTION_STALE_OWNER_REF_V1_SCHEMA,
        FILTER_PROJECTION_STALE_OWNER_REF_V1_CONTRACT_DIGEST,
    ),
    "filter_projection_not_ready_owner_ref.v1": (
        FILTER_PROJECTION_NOT_READY_OWNER_REF_V1_SCHEMA_VERSION,
        FILTER_PROJECTION_NOT_READY_OWNER_REF_V1_OWNER,
        FILTER_PROJECTION_NOT_READY_OWNER_REF_V1_ORDERED_FIELDS,
        FILTER_PROJECTION_NOT_READY_OWNER_REF_V1_SCHEMA,
        FILTER_PROJECTION_NOT_READY_OWNER_REF_V1_CONTRACT_DIGEST,
    ),
    "filter_projection_masked_absence_owner_ref.v1": (
        FILTER_PROJECTION_MASKED_ABSENCE_OWNER_REF_V1_SCHEMA_VERSION,
        FILTER_PROJECTION_MASKED_ABSENCE_OWNER_REF_V1_OWNER,
        FILTER_PROJECTION_MASKED_ABSENCE_OWNER_REF_V1_ORDERED_FIELDS,
        FILTER_PROJECTION_MASKED_ABSENCE_OWNER_REF_V1_SCHEMA,
        FILTER_PROJECTION_MASKED_ABSENCE_OWNER_REF_V1_CONTRACT_DIGEST,
    ),
    "filter_projection_result_v3": (
        FILTER_PROJECTION_RESULT_V3_SCHEMA_VERSION,
        FILTER_PROJECTION_RESULT_V3_OWNER,
        FILTER_PROJECTION_RESULT_V3_ORDERED_FIELDS,
        FILTER_PROJECTION_RESULT_V3_SCHEMA,
        FILTER_PROJECTION_RESULT_V3_CONTRACT_DIGEST,
    ),
}
# Referenced contract pinned beside the family; never adopted or retyped.
REFERENCED = {
    "filter_projection_product_ref.v1": (
        FILTER_PROJECTION_PRODUCT_REF_V1_SCHEMA_VERSION,
        FILTER_PROJECTION_PRODUCT_REF_V1_OWNER,
        FILTER_PROJECTION_PRODUCT_REF_V1_ORDERED_FIELDS,
        FILTER_PROJECTION_PRODUCT_REF_V1_SCHEMA,
        FILTER_PROJECTION_PRODUCT_REF_V1_CONTRACT_DIGEST,
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


def _terminal_core() -> dict[str, Any]:
    """Canonical product-terminal fields 1-23 through terminal_generation (first terminal: no predecessor)."""

    return {
        "schema_version": "filter_projection_product_terminal.v1",
        "owner": "serving_projection_owner",
        "product_terminal_id": "terminal-1",
        "workspace_id": "ws-1",
        "acquisition_run_id": "run-1",
        "operation_run_id": "op-1",
        "workflow_run_id": "wf-1",
        "execution_commit_id": "commit-1",
        "execution_commit_digest": _digest("commit"),
        "projection_id": "proj-1",
        "projection_version": 1,
        "membership_revision": "membership-rev-1",
        "route_kind": "run_scope",
        "route_key": "route-1",
        "route_revision_token": "route-token-1",
        "provider_mode": "simulate",
        "runtime_namespace_ref": _namespace_ref(),
        "candidate_set_digest": _digest("candidate-set"),
        "candidate_count": 2,
        "visible_member_count": 2,
        "excluded_member_count": 0,
        "terminal_generation": 1,
    }


def _freshness_ref(core: dict[str, Any], core_digest: str) -> dict[str, Any]:
    record = {
        "schema_version": "filter_projection_freshness_ref.v1",
        "owner": "serving_projection_owner",
        "status": "fresh",
        "product_terminal_id": core["product_terminal_id"],
        "terminal_core_digest": core_digest,
        "route_kind": core["route_kind"],
        "route_key": core["route_key"],
        "route_revision_token": core["route_revision_token"],
        "membership_revision": core["membership_revision"],
        "candidate_set_digest": core["candidate_set_digest"],
    }
    record["freshness_ref_digest"] = compute_filter_projection_freshness_ref_digest(record)
    return record


def _readiness_ref(core: dict[str, Any], core_digest: str) -> dict[str, Any]:
    record = {
        "schema_version": "filter_projection_readiness_ref.v1",
        "owner": "serving_projection_owner",
        "status": "ready",
        "reason": "",
        "product_terminal_id": core["product_terminal_id"],
        "terminal_core_digest": core_digest,
        "projection_state": "serving",
        "membership_revision": core["membership_revision"],
        "candidate_set_digest": core["candidate_set_digest"],
        "candidate_count": core["candidate_count"],
        "visible_member_count": core["visible_member_count"],
        "row_ready_count": core["visible_member_count"],
        "prerequisite_set_digest": compute_filter_projection_readiness_prerequisite_set_digest(),
    }
    record["readiness_ref_digest"] = compute_filter_projection_readiness_ref_digest(record)
    return record


def _terminal() -> dict[str, Any]:
    core = _terminal_core()
    core_digest = compute_filter_projection_product_terminal_core_digest(core)
    record = {
        **core,
        "terminal_core_digest": core_digest,
        "freshness_ref": _freshness_ref(core, core_digest),
        "readiness_ref": _readiness_ref(core, core_digest),
    }
    record["terminal_digest"] = compute_filter_projection_product_terminal_digest(record)
    return record


def _owner_ref_pins() -> dict[str, Any]:
    return {
        "result_schema_version": "filter_projection_result_v3",
        "result_contract_digest": FILTER_PROJECTION_RESULT_V3_CONTRACT_DIGEST,
        "serializer_owner_name": FILTER_PROJECTION_RESULT_SERIALIZER_V3_OWNER_NAME,
        "serializer_revision": FILTER_PROJECTION_RESULT_SERIALIZER_V3_REVISION,
        "serializer_contract_digest": FILTER_PROJECTION_RESULT_SERIALIZER_V3_CONTRACT_DIGEST,
    }


def _success_owner_ref() -> dict[str, Any]:
    terminal = _terminal()
    record = {
        "schema_version": "filter_projection_success_owner_ref.v1",
        "logical_occurrence_digest": _digest("occurrence"),
        "result_slot_id": "slot-1",
        "slot_generation": 1,
        "product_terminal_id": terminal["product_terminal_id"],
        "terminal_digest": terminal["terminal_digest"],
        "terminal_generation": terminal["terminal_generation"],
        "projection_id": terminal["projection_id"],
        "membership_revision": terminal["membership_revision"],
        "execution_commit_digest": terminal["execution_commit_digest"],
        "candidate_set_digest": terminal["candidate_set_digest"],
        "freshness_ref_digest": terminal["freshness_ref"]["freshness_ref_digest"],
        "readiness_ref_digest": terminal["readiness_ref"]["readiness_ref_digest"],
        **_owner_ref_pins(),
    }
    record["owner_ref_digest"] = compute_filter_projection_owner_ref_digest(record)
    return record


def _stale_owner_ref() -> dict[str, Any]:
    record = {
        "schema_version": "filter_projection_stale_owner_ref.v1",
        "logical_occurrence_digest": _digest("occurrence"),
        "result_slot_id": "slot-1",
        "slot_generation": 1,
        "requested_target_digest": _digest("requested-target"),
        "requested_terminal_id": "terminal-0",
        "requested_terminal_digest": _digest("requested-terminal"),
        "successor_terminal_id": "terminal-1",
        "successor_terminal_digest": _digest("successor-terminal"),
        "route_revision_token": "route-token-1",
        **_owner_ref_pins(),
    }
    record["owner_ref_digest"] = compute_filter_projection_owner_ref_digest(record)
    return record


def _not_ready_owner_ref() -> dict[str, Any]:
    record = {
        "schema_version": "filter_projection_not_ready_owner_ref.v1",
        "logical_occurrence_digest": _digest("occurrence"),
        "result_slot_id": "slot-1",
        "slot_generation": 1,
        "requested_target_digest": _digest("requested-target"),
        "projection_id": "proj-1",
        "projection_version": 1,
        "projection_state": "building",
        "membership_revision": "membership-rev-1",
        "route_digest": _digest("route"),
        "reason": "projection_publication_pending",
        "retryable": True,
        "reselection_required": False,
        **_owner_ref_pins(),
    }
    record["owner_ref_digest"] = compute_filter_projection_owner_ref_digest(record)
    return record


def _masked_absence_owner_ref() -> dict[str, Any]:
    record = {
        "schema_version": "filter_projection_masked_absence_owner_ref.v1",
        "logical_occurrence_digest": _digest("occurrence"),
        "result_slot_id": "slot-1",
        "slot_generation": 1,
        "reason": "projection_not_found",
        **_owner_ref_pins(),
    }
    record["owner_ref_digest"] = compute_filter_projection_owner_ref_digest(record)
    return record


def _product_ref() -> dict[str, Any]:
    terminal = _terminal()
    return {
            "schema_version": "filter_projection_product_ref.v1",
            "projection_id": terminal["projection_id"],
            "membership_revision": terminal["membership_revision"],
            "terminal_digest": terminal["terminal_digest"],
        }


def _cohort_selection() -> dict[str, Any]:
    return {
        "schema_version": "cohort_selection.v1",
        "role_bucket_ids": ["engineering"],
        "employment_statuses": ["current"],
        "role_match": "any",
        "source": "user_explicit",
    }


def _candidate_item(index: int) -> dict[str, Any]:
    return {
        "candidate_ref": _digest(f"candidate-{index}"),
        "display_name": f"Candidate {index}",
        "headline": "Staff Engineer",
        "public_profile_url": f"https://example.com/profile/candidate-{index}",
        "employment_statuses": ["current"],
        "role_bucket_ids": ["engineering"],
    }


def _success_root() -> dict[str, Any]:
    terminal = _terminal()
    selection = _cohort_selection()
    return {
            "variant": "success",
            "status": "ready",
            "projection_ref": _product_ref(),
            "cohort_selection": selection,
            "cohort_selection_registry_version": "cohort_selection.registry.v1",
            "cohort_selection_registry_digest": cohort_selection_registry_digest(),
            "cohort_selection_digest": cohort_selection_digest(selection),
            "execution_commit_digest": terminal["execution_commit_digest"],
            "candidate_set_digest": terminal["candidate_set_digest"],
            "freshness": terminal["freshness_ref"],
            "readiness": terminal["readiness_ref"],
            "provider_mode": "simulate",
            "runtime_namespace_ref": agent_runtime_namespace_ref_public_record(terminal["runtime_namespace_ref"]),
            "requested_lane_coverage": {
                "status": "complete",
                "requested_lane_count": 2,
                "completed_lane_count": 2,
                "missing_lane_count": 0,
            },
            "lane_summaries": [
                {
                    "lane_id": "lane-0",
                    "employment_status": "current",
                    "role_bucket_id": "engineering",
                    "coverage_status": "complete",
                    "result_count": 1,
                },
                {
                    "lane_id": "lane-1",
                    "employment_status": "current",
                    "role_bucket_id": "engineering",
                    "coverage_status": "complete",
                    "result_count": 1,
                },
            ],
            "offset": 0,
            "limit": 250,
            "total_count": 2,
            "returned_count": 2,
            "truncated": False,
            "candidates": [_candidate_item(1), _candidate_item(2)],
        }


def _deferred_root() -> dict[str, Any]:
    return {
            "variant": "deferred",
            "status": "not_ready",
            "reason": "projection_candidate_set_empty",
            "retryable": False,
            "reselection_required": True,
            "requested_target_ref": {
                "projection_id": "proj-1",
                "membership_revision": "membership-rev-1",
            },
            "decision_ref": _digest("decision"),
        }


def _stale_deferred_root() -> dict[str, Any]:
    return {
            "variant": "deferred",
            "status": "stale",
            "reason": "projection_state_not_serving",
            "retryable": True,
            "reselection_required": False,
            "requested_target_ref": {
                "projection_id": "proj-1",
                "membership_revision": "membership-rev-1",
                "requested_terminal_id": "terminal-0",
                "requested_terminal_digest": _digest("requested-terminal"),
                "route_revision_token": "route-token-0",
            },
            "decision_ref": _digest("decision"),
        }


def _masked_root() -> dict[str, Any]:
    return {
            "variant": "error",
            "status": "failed",
            "reason": "projection_not_found",
            "retryable": False,
            "decision_ref": _digest("decision"),
        }


def _expect_invalid(parser: Any, record: Any, **kwargs: Any) -> None:
    # Plain dicts are not a decode boundary; hostile record shapes go through
    # the text path so validation (not the boundary) is what rejects them.
    payload = canonical_json(record) if type(record) is dict else record
    with pytest.raises(FilterProjectionTerminalContractError):
        parser(payload, **kwargs)


def _v3_result_spec() -> ActionResultSpec:
    """Build the real decision-locked V3 ActionResultSpec from the manifest fingerprint record."""

    record = _manifest()["result_v3_slot_contract"]["canonical_fingerprints"]["result"]["record"]
    return ActionResultSpec(
        tool_name=record["tool_name"],
        tool_kind=record["tool_kind"],
        owner_binding=ActionResultActionOwner(action_type=record["owner_binding"]["action_type"]),
        result_schema_version=record["result_schema_version"],
        serializer_owner=record["serializer_owner"],
        serializer_revision=record["serializer_revision"],
        serializer_contract=deepcopy(record["serializer_contract"]),
        validator_owner=record["validator_owner"],
        variant_schemas=deepcopy(record["variant_schemas"]),
        field_provenance=deepcopy(record["field_provenance"]),
        field_value_roles=deepcopy(record["field_value_roles"]),
        max_serialized_bytes=record["max_serialized_bytes"],
        max_items=record["max_items"],
        max_depth=record["max_depth"],
        artifact_ref_schemes=tuple(record["artifact_ref_schemes"]),
        interpretation_contract_version=record["interpretation_contract"]["schema_version"],
        externally_controlled_identifier_paths=tuple(record["externally_controlled_identifier_paths"]),
    )


def _expect_spec_rejection(spec: ActionResultSpec, record: Any) -> None:
    with pytest.raises(ActionResultSchemaError):
        spec.serialize(json.loads(canonical_json(record)))


def test_constants_match_manifest_exactly() -> None:
    rows = _manifest_rows()
    for literal, (version, owner, _ordered, _schema, _digest_constant) in {**ADOPTED, **REFERENCED}.items():
        assert version == literal
        assert rows[literal]["literal"] == literal
        assert owner == rows[literal]["contract_owner"], literal
    owners = _manifest()["owners"]
    assert FILTER_PROJECTION_PRODUCT_TERMINAL_V1_OWNER == owners["product_terminal_writer"] == "serving_projection_owner"
    assert FILTER_PROJECTION_FRESHNESS_REF_V1_OWNER == owners["freshness_readiness_owner"]
    assert FILTER_PROJECTION_SUCCESS_OWNER_REF_V1_OWNER == owners["owner_ref_owner"] == "projection_search_service"
    assert FILTER_PROJECTION_RESULT_V3_OWNER == owners["result_v3_route_owner"] == "filter_projection"
    assert FILTER_PROJECTION_PRODUCT_REF_V1_OWNER == "serving_projection_owner"


def test_schema_objects_and_digests_match_manifest() -> None:
    rows = _manifest_rows()
    for literal, (_version, _owner, _ordered, schema, digest_constant) in {**ADOPTED, **REFERENCED}.items():
        row = rows[literal]
        assert json.loads(json.dumps(schema)) == row["schema"], literal
        assert _sha256_canonical(row["schema"]) == row["contract_digest"], literal
        assert digest_constant == row["contract_digest"], literal
    serializer_row = rows["filter_projection_result_serializer_v3"]
    assert FILTER_PROJECTION_RESULT_SERIALIZER_V3_CONTRACT_DIGEST == serializer_row["contract_digest"]
    assert FILTER_PROJECTION_RESULT_SERIALIZER_V3_OWNER_NAME == "projection_search_service.filter_projection_result_serializer_v3"
    assert FILTER_PROJECTION_RESULT_SERIALIZER_V3_REVISION == "filter_projection_result_serializer_v3"
    # the embedded value-role map is byte-identical to the decision-locked
    # ActionResultSpec fingerprint map (action_result_positive_value_roles_v3)
    fingerprint_roles = _manifest()["result_v3_slot_contract"]["canonical_fingerprints"]["result"]["record"][
        "field_value_roles"
    ]
    assert json.loads(json.dumps(FILTER_PROJECTION_RESULT_V3_FIELD_VALUE_ROLES)) == fingerprint_roles


def test_manifest_module_field_order_consistency() -> None:
    rows = _manifest_rows()
    for literal, (_version, _owner, ordered, schema, _digest_constant) in {**ADOPTED, **REFERENCED}.items():
        row = rows[literal]
        assert list(ordered) == row["ordered_fields"], literal
        # The schema descriptor name sequence matches the manifest exactly,
        # including the V3 variant-discriminator duplicates.
        assert [field["name"] for field in schema["fields"]] == [
            field["name"] for field in row["schema"]["fields"]
        ], literal
        if literal != "filter_projection_result_v3":
            assert [field["name"] for field in schema["fields"]] == row["ordered_fields"], literal
        else:
            assert sorted(set(row["ordered_fields"])) == sorted(row["ordered_fields"])
            assert set(row["ordered_fields"]) == {field["name"] for field in row["schema"]["fields"]}
    assert set(CONTRACT_SCHEMAS) == set(ADOPTED) | set(REFERENCED)


def test_result_v3_roots_match_slot_contract() -> None:
    slot = _manifest()["result_v3_slot_contract"]
    assert slot["result_schema_version"] == FILTER_PROJECTION_RESULT_V3_SCHEMA_VERSION
    assert tuple(slot["success_root_fields"]) == FILTER_PROJECTION_RESULT_V3_SUCCESS_ROOT_FIELDS
    assert tuple(slot["deferred_root_fields"]) == FILTER_PROJECTION_RESULT_V3_DEFERRED_ROOT_FIELDS
    assert tuple(slot["masked_root_fields"]) == FILTER_PROJECTION_RESULT_V3_MASKED_ROOT_FIELDS
    assert slot["masked_root_constants"] == FILTER_PROJECTION_RESULT_V3_MASKED_ROOT_CONSTANTS
    for variant, root in (
        ("success", FILTER_PROJECTION_RESULT_V3_SUCCESS_ROOT_FIELDS),
        ("deferred", FILTER_PROJECTION_RESULT_V3_DEFERRED_ROOT_FIELDS),
        ("error", FILTER_PROJECTION_RESULT_V3_MASKED_ROOT_FIELDS),
    ):
        assert [field["name"] for field in filter_projection_result_v3_variant_fields(variant)] == list(root)
    assert tuple(FILTER_PROJECTION_RESULT_V3_VARIANTS) == ("success", "deferred", "error")
    records = _manifest()["product_terminal_records"]
    assert tuple(records["deferred_reason_precedence"]) == FILTER_PROJECTION_DEFERRED_REASON_PRECEDENCE
    assert tuple(records["readiness_ref"]["prerequisite_set"]) == FILTER_PROJECTION_READINESS_PREREQUISITE_SET
    zero_member = records["zero_member_rule"]
    assert zero_member["reason"] == FILTER_PROJECTION_DEFERRED_REASON_PRECEDENCE[0] == "projection_candidate_set_empty"
    assert zero_member["outcome"] == "deferred not_ready"
    assert zero_member["retryable"] is False
    assert zero_member["reselection_required"] is True


def test_namespace_and_family_ref_pins_are_exact() -> None:
    rows = _manifest_rows()
    assert AGENT_RUNTIME_NAMESPACE_REF_CONTRACT_DIGEST == rows["agent_runtime_namespace_ref.v1"]["contract_digest"]
    terminal_ref = next(
        field for field in FILTER_PROJECTION_PRODUCT_TERMINAL_V1_SCHEMA["fields"] if field["name"] == "runtime_namespace_ref"
    )
    assert terminal_ref["ref"] == "agent_runtime_namespace_ref.v1"
    assert terminal_ref["ref_digest"] == AGENT_RUNTIME_NAMESPACE_REF_CONTRACT_DIGEST
    for name, target in (("freshness_ref", "filter_projection_freshness_ref.v1"), ("readiness_ref", "filter_projection_readiness_ref.v1")):
        descriptor = next(field for field in FILTER_PROJECTION_PRODUCT_TERMINAL_V1_SCHEMA["fields"] if field["name"] == name)
        assert descriptor["ref"] == target
        assert descriptor["ref_digest"] == rows[target]["contract_digest"]
    v3_projection_ref = next(
        field
        for field in FILTER_PROJECTION_RESULT_V3_SCHEMA["fields"]
        if field["name"] == "projection_ref"
    )
    assert v3_projection_ref["ref"] == "filter_projection_product_ref.v1"
    assert v3_projection_ref["ref_digest"] == FILTER_PROJECTION_PRODUCT_REF_V1_CONTRACT_DIGEST



def test_parse_round_trips() -> None:
    fixtures = [
        (parse_filter_projection_freshness_ref, _terminal()["freshness_ref"], {}),
        (parse_filter_projection_readiness_ref, _terminal()["readiness_ref"], {}),
        (parse_filter_projection_product_terminal, _terminal(), {}),
        (parse_filter_projection_success_owner_ref, _success_owner_ref(), {}),
        (parse_filter_projection_stale_owner_ref, _stale_owner_ref(), {}),
        (parse_filter_projection_not_ready_owner_ref, _not_ready_owner_ref(), {}),
        (parse_filter_projection_masked_absence_owner_ref, _masked_absence_owner_ref(), {}),
        (parse_filter_projection_product_ref, _product_ref(), {}),
        (parse_filter_projection_result_v3, _success_root(), {}),
        (parse_filter_projection_result_v3, _deferred_root(), {}),
        (parse_filter_projection_result_v3, _stale_deferred_root(), {}),
        (parse_filter_projection_result_v3, _masked_root(), {}),
        (parse_filter_projection_result_v3, _success_root(), {"variant": "success"}),
        (parse_filter_projection_result_v3, _deferred_root(), {"variant": "deferred"}),
        (parse_filter_projection_result_v3, _masked_root(), {"variant": "error"}),
    ]
    for parser, record, kwargs in fixtures:
        parsed = parser(canonical_json(record), **kwargs)  # text boundary
        assert parsed == record
        assert parser(parsed, **kwargs) == record  # decoder-provenance round trip
        assert parser(canonical_json(parsed), **kwargs) == record


def test_terminal_digest_chain_matches_manifest_equalities() -> None:
    terminal = _terminal()
    core_fields = FILTER_PROJECTION_PRODUCT_TERMINAL_V1_ORDERED_FIELDS[:23]
    assert core_fields[-1] == "terminal_generation"
    # predecessor_terminal_digest is absent for the first terminal of a projection.
    assert "predecessor_terminal_digest" not in terminal
    covered = {field: terminal[field] for field in core_fields if field in terminal}
    core_digest = _sha256_canonical(covered)
    assert compute_filter_projection_product_terminal_core_digest(terminal) == core_digest
    assert terminal["terminal_core_digest"] == core_digest
    # core equality: both nested refs carry the exact terminal core digest.
    assert terminal["freshness_ref"]["terminal_core_digest"] == core_digest
    assert terminal["readiness_ref"]["terminal_core_digest"] == core_digest
    # envelope equality: terminal_digest recomputes over every preceding field.
    envelope_covered = {key: value for key, value in terminal.items() if key != "terminal_digest"}
    assert terminal["terminal_digest"] == compute_filter_projection_product_terminal_digest(terminal)
    assert terminal["terminal_digest"] == _sha256_canonical(envelope_covered)
    # serialized projection_ref pins the same envelope digest.
    assert _product_ref()["terminal_digest"] == terminal["terminal_digest"]
    equality = _manifest()["product_terminal_records"]["terminal_digest_equality"]
    assert "terminal_core_digest" in equality["core"]
    assert "terminal_digest" in equality["envelope"]


def test_readiness_prerequisite_set_digest_matches_independent_recomputation() -> None:
    expected = _sha256_canonical(list(FILTER_PROJECTION_READINESS_PREREQUISITE_SET))
    assert compute_filter_projection_readiness_prerequisite_set_digest() == expected
    terminal = _terminal()
    readiness = terminal["readiness_ref"]
    assert readiness["prerequisite_set_digest"] == expected
    assert compute_filter_projection_readiness_ref_digest(readiness) == readiness["readiness_ref_digest"]
    freshness = terminal["freshness_ref"]
    assert compute_filter_projection_freshness_ref_digest(freshness) == freshness["freshness_ref_digest"]


def _reteminal(terminal: dict[str, Any], **overrides: Any) -> dict[str, Any]:
    """Rebuild a terminal with fully recomputed nested and envelope digests after mutation."""

    record = {**terminal, **overrides}
    record["terminal_digest"] = compute_filter_projection_product_terminal_digest(record)
    return record


def test_product_terminal_fail_closed() -> None:
    terminal = _terminal()
    hostile = [
        {**terminal, "unexpected": "field"},
        {**terminal, "predecessor_terminal_digest": ""},  # empty-string alias forbidden
        {key: value for key, value in terminal.items() if key != "terminal_digest"},
        {**terminal, "schema_version": "filter_projection_publication_terminal.v1"},  # rejected literal
        {**terminal, "owner": "projection_search_service"},
        {**terminal, "route_kind": "collection_authoritative"},
        {**terminal, "provider_mode": "live"},
        {**terminal, "candidate_count": 0},
        {**terminal, "candidate_count": 1001},
        {**terminal, "visible_member_count": 1},  # must equal candidate_count
        {**terminal, "excluded_member_count": 1},  # exact constant 0
        {**terminal, "projection_version": 0},
        {**terminal, "terminal_generation": True},
        {**terminal, "membership_revision": ""},
        {**terminal, "terminal_core_digest": "0" * 64},
        {**terminal, "terminal_digest": "0" * 64},
        {**terminal, "freshness_ref": {**terminal["freshness_ref"], "terminal_core_digest": "0" * 64}},
        {**terminal, "readiness_ref": {**terminal["readiness_ref"], "status": "not_ready"}},
    ]
    for record in hostile:
        _expect_invalid(parse_filter_projection_product_terminal, canonical_json(record))
    # a successor terminal binds the exact predecessor digest.
    successor = {**_terminal_core(), "predecessor_terminal_digest": terminal["terminal_digest"], "terminal_generation": 2}
    core_digest = compute_filter_projection_product_terminal_core_digest(successor)
    successor = {
        **successor,
        "terminal_core_digest": core_digest,
        "freshness_ref": _freshness_ref(successor, core_digest),
        "readiness_ref": _readiness_ref(successor, core_digest),
    }
    successor["terminal_digest"] = compute_filter_projection_product_terminal_digest(successor)
    assert parse_filter_projection_product_terminal(canonical_json(successor)) == successor


def test_terminal_nested_ref_field_binding() -> None:
    """F3: nested freshness/readiness refs are field-bound to the enclosing terminal (fully re-digested probes)."""

    terminal = _terminal()
    freshness = terminal["freshness_ref"]
    readiness = terminal["readiness_ref"]
    for drifted, wrong in (
        ("route_key", "route-2"),
        ("route_revision_token", "route-token-2"),
        ("membership_revision", "membership-rev-2"),
        ("candidate_set_digest", _digest("other-set")),
        ("product_terminal_id", "terminal-2"),
    ):
        mutated_freshness = {**freshness, drifted: wrong}
        mutated_freshness["freshness_ref_digest"] = compute_filter_projection_freshness_ref_digest(mutated_freshness)
        _expect_invalid(
            parse_filter_projection_product_terminal,
            _reteminal(terminal, freshness_ref=mutated_freshness),
        )
    for drifted, wrong in (
        ("membership_revision", "membership-rev-2"),
        ("candidate_set_digest", _digest("other-set")),
        ("product_terminal_id", "terminal-2"),
        ("candidate_count", 1),
        ("visible_member_count", 1),
        ("row_ready_count", 0),  # status=ready requires row_ready_count == visible_member_count
    ):
        mutated_readiness = {**readiness, drifted: wrong}
        mutated_readiness["readiness_ref_digest"] = compute_filter_projection_readiness_ref_digest(mutated_readiness)
        _expect_invalid(
            parse_filter_projection_product_terminal,
            _reteminal(terminal, readiness_ref=mutated_readiness),
        )
    # terminal-side count drift is rejected even when the readiness ref matches its own core copy
    drifted_core = {**_terminal_core(), "candidate_count": 3, "visible_member_count": 3}
    core_digest = compute_filter_projection_product_terminal_core_digest(drifted_core)
    drifted = {
        **drifted_core,
        "terminal_core_digest": core_digest,
        "freshness_ref": _freshness_ref(drifted_core, core_digest),
        "readiness_ref": _readiness_ref(drifted_core, core_digest),
    }
    drifted["terminal_digest"] = compute_filter_projection_product_terminal_digest(drifted)
    assert parse_filter_projection_product_terminal(canonical_json(drifted)) == drifted
    assert parse_filter_projection_product_terminal(canonical_json(_terminal())) == _terminal()


def test_freshness_and_readiness_refs_fail_closed() -> None:
    terminal = _terminal()
    freshness = terminal["freshness_ref"]
    readiness = terminal["readiness_ref"]
    for record, parser in (
        ({**freshness, "unexpected": "field"}, parse_filter_projection_freshness_ref),
        ({**freshness, "status": "stale"}, parse_filter_projection_freshness_ref),
        ({**freshness, "route_kind": "global"}, parse_filter_projection_freshness_ref),
        ({**freshness, "freshness_ref_digest": "0" * 64}, parse_filter_projection_freshness_ref),
        ({**freshness, "route_revision_token": ""}, parse_filter_projection_freshness_ref),
        ({**readiness, "unexpected": "field"}, parse_filter_projection_readiness_ref),
        ({**readiness, "status": "not_ready"}, parse_filter_projection_readiness_ref),
        ({**readiness, "reason": "projection_members_not_ready"}, parse_filter_projection_readiness_ref),
        ({**readiness, "projection_state": "building"}, parse_filter_projection_readiness_ref),
        ({**readiness, "candidate_count": 0}, parse_filter_projection_readiness_ref),
        ({**readiness, "row_ready_count": -1}, parse_filter_projection_readiness_ref),
        ({**readiness, "prerequisite_set_digest": "0" * 64}, parse_filter_projection_readiness_ref),
        ({**readiness, "readiness_ref_digest": "0" * 64}, parse_filter_projection_readiness_ref),
    ):
        _expect_invalid(parser, canonical_json(record))


def test_owner_refs_fail_closed() -> None:
    success = _success_owner_ref()
    stale = _stale_owner_ref()
    not_ready = _not_ready_owner_ref()
    masked = _masked_absence_owner_ref()
    for parser, record in (
        (parse_filter_projection_success_owner_ref, {**success, "unexpected": "field"}),
        (parse_filter_projection_success_owner_ref, {**success, "slot_generation": 0}),
        (parse_filter_projection_success_owner_ref, {**success, "terminal_generation": 1.0}),
        (parse_filter_projection_success_owner_ref, {**success, "result_schema_version": "filter_projection_result_v2"}),
        (parse_filter_projection_success_owner_ref, {**success, "result_contract_digest": "0" * 64}),
        (parse_filter_projection_success_owner_ref, {**success, "serializer_contract_digest": "0" * 64}),
        (parse_filter_projection_success_owner_ref, {**success, "serializer_owner_name": "other.serializer"}),
        (parse_filter_projection_success_owner_ref, {**success, "owner_ref_digest": "0" * 64}),
        (parse_filter_projection_success_owner_ref, {**success, "projection_id": "proj-2"}),
        (parse_filter_projection_stale_owner_ref, {**stale, "requested_terminal_digest": "bad"}),
        (parse_filter_projection_stale_owner_ref, {**stale, "route_revision_token": ""}),
        (parse_filter_projection_stale_owner_ref, {**stale, "owner_ref_digest": "0" * 64}),
        (parse_filter_projection_not_ready_owner_ref, {**not_ready, "reason": "projection_not_found"}),
        (parse_filter_projection_not_ready_owner_ref, {**not_ready, "reason": "projection_members_not_ready", "retryable": "yes"}),
        (parse_filter_projection_not_ready_owner_ref, {**not_ready, "projection_version": 0}),
        (parse_filter_projection_not_ready_owner_ref, {**not_ready, "reselection_required": 1}),
        (parse_filter_projection_masked_absence_owner_ref, {**masked, "product_terminal_id": "terminal-1"}),
        (parse_filter_projection_masked_absence_owner_ref, {**masked, "workspace_id": "ws-1"}),
        (parse_filter_projection_masked_absence_owner_ref, {**masked, "reason": "projection_members_not_ready"}),
        (parse_filter_projection_masked_absence_owner_ref, {**masked, "owner_ref_digest": "0" * 64}),
    ):
        _expect_invalid(parser, canonical_json(record))


def test_result_v3_variant_discrimination_fail_closed() -> None:
    success = _success_root()
    deferred = _deferred_root()
    masked = _masked_root()
    # each root validates exactly under its own variant.
    assert parse_filter_projection_result_v3(canonical_json(success), variant="success") == success
    assert parse_filter_projection_result_v3(canonical_json(deferred), variant="deferred") == deferred
    assert parse_filter_projection_result_v3(canonical_json(_stale_deferred_root()), variant="deferred") == _stale_deferred_root()
    assert parse_filter_projection_result_v3(canonical_json(masked), variant="error") == masked
    hostile = [
        ({**success, "reason": "projection_not_found"}, {}),  # success root is closed: no deferred field
        ({**success, "status": "stale"}, {}),
        ({**success, "variant": "deferred"}, {}),  # discriminator and field set must agree
        ({key: value for key, value in success.items() if key != "candidates"}, {}),
        ({**success, "provider_mode": "live"}, {}),
        ({**success, "limit": 251}, {}),
        ({**success, "offset": -1}, {}),
        ({**success, "total_count": 1001}, {}),
        ({**success, "truncated": "false"}, {}),
        ({**deferred, "candidates": []}, {}),  # deferred root is closed: no success field
        ({**deferred, "status": "ready"}, {}),
        ({**deferred, "reason": "projection_not_found"}, {}),
        ({**deferred, "retryable": "no"}, {}),
        ({**deferred, "requested_target_ref": {"projection_id": "proj-1"}}, {}),  # membership_revision required
        ({**deferred, "requested_target_ref": {**deferred["requested_target_ref"], "extra": "key"}}, {}),
        ({**masked, "reselection_required": True}, {}),  # masked root is closed: deferred-only field
        ({**masked, "status": "not_ready"}, {}),
        ({**masked, "reason": "projection_members_not_ready"}, {}),
        ({**masked, "retryable": True}, {}),  # exact constant false
        ({**masked, "decision_ref": "bad"}, {}),
        ({**success, "variant": "unknown"}, {}),
        ({"variant": "success"}, {}),
        (success, {"variant": "deferred"}),  # explicit variant must match the record
    ]
    for record, kwargs in hostile:
        _expect_invalid(parse_filter_projection_result_v3, canonical_json(record), **kwargs)
    _expect_invalid(parse_filter_projection_result_v3, "success")
    with pytest.raises(FilterProjectionTerminalContractError):
        filter_projection_result_v3_variant_fields("unknown")


def test_result_v3_nested_objects_fail_closed() -> None:
    success = _success_root()
    candidate = success["candidates"][0]
    lane_summary = success["lane_summaries"][0]
    hostile = [
        {**success, "projection_ref": {**success["projection_ref"], "schema_version": "filter_projection_product_ref.v2"}},
        {**success, "projection_ref": {**success["projection_ref"], "terminal_digest": "bad"}},
        {**success, "cohort_selection": {**success["cohort_selection"], "role_match": "any|all"}},
        {**success, "cohort_selection": {**success["cohort_selection"], "source": "inferred"}},
        {**success, "cohort_selection": {**success["cohort_selection"], "extra": "key"}},
        {**success, "freshness": {**success["freshness"], "freshness_ref_digest": "0" * 64}},
        {**success, "readiness": {**success["readiness"], "status": "not_ready"}},
        {**success, "runtime_namespace_ref": {**success["runtime_namespace_ref"], "workspace_id": "ws-1"}},
        {**success, "runtime_namespace_ref": {**success["runtime_namespace_ref"], "ref_digest": "bad"}},
        {**success, "requested_lane_coverage": {**success["requested_lane_coverage"], "status": "missing"}},
        {**success, "requested_lane_coverage": {**success["requested_lane_coverage"], "requested_lane_count": 0}},
        {**success, "lane_summaries": [{**lane_summary, "coverage_status": "unknown"}]},
        {**success, "lane_summaries": [{**lane_summary, "lane_id": "x" * 201}]},
        {**success, "candidates": [{**candidate, "display_name": ""}, success["candidates"][1]]},
        {**success, "candidates": [{**candidate, "public_profile_url": "http://example.com/x"}, success["candidates"][1]]},
        {**success, "candidates": [{**candidate, "employment_statuses": []}, success["candidates"][1]]},
        {**success, "candidates": [{**candidate, "role_bucket_ids": ["sales"]}, success["candidates"][1]]},
        {
            **success,
            "candidates": [
                {key: value for key, value in candidate.items() if key != "headline"},
                success["candidates"][1],
            ],
        },
    ]
    for record in hostile:
        _expect_invalid(parse_filter_projection_result_v3, canonical_json(record))
    # candidate public_profile_url is optional; absence is the closed variant.
    without_url = _success_root()
    del without_url["candidates"][0]["public_profile_url"]
    assert parse_filter_projection_result_v3(canonical_json(without_url)) == without_url


def test_result_v3_page_and_selection_equations() -> None:
    """F4: canonical selection digest, registry pin, v3_page_equations, and freshness/readiness parity."""

    success = _success_root()
    # returned_count = len(candidates) = min(limit, max(0, total_count - offset)); truncated equation
    for drifted, wrong in (
        ("returned_count", 1),
        ("returned_count", 0),
        ("offset", 1),  # min(250, max(0, 2-1)) = 1 != returned 2
        ("total_count", 3),  # min(250, max(0, 3-0)) = 3 != returned 2
        ("truncated", True),  # 0 + 2 < 2 is False
        ("limit", 1),  # min(1, 2) = 1 != returned 2
    ):
        _expect_invalid(parse_filter_projection_result_v3, canonical_json({**success, drifted: wrong}))
    # the sibling cohort_selection_digest equals cohort_selection_digest(cohort_selection)
    mutated_selection = {**success["cohort_selection"], "role_match": "all"}
    _expect_invalid(
        parse_filter_projection_result_v3,
        canonical_json({**success, "cohort_selection": mutated_selection}),
    )
    other_digest = _sha256_canonical(success["cohort_selection"])
    _expect_invalid(
        parse_filter_projection_result_v3,
        canonical_json({**success, "cohort_selection_digest": other_digest}),
    )
    _expect_invalid(
        parse_filter_projection_result_v3,
        canonical_json({**success, "cohort_selection_registry_digest": "0" * 64}),
    )
    # freshness/readiness parity with the root and with each other
    freshness = success["freshness"]
    readiness = success["readiness"]
    for mutated_ref, key in (
        ({**freshness, "candidate_set_digest": _digest("other-set")}, "freshness"),
        ({**readiness, "candidate_set_digest": _digest("other-set")}, "readiness"),
        ({**freshness, "terminal_core_digest": _digest("other-core")}, "freshness"),
        ({**readiness, "product_terminal_id": "terminal-2"}, "readiness"),
        ({**freshness, "membership_revision": "membership-rev-2"}, "freshness"),
        ({**readiness, "membership_revision": "membership-rev-2"}, "readiness"),
    ):
        digest_field = "freshness_ref_digest" if key == "freshness" else "readiness_ref_digest"
        compute = (
            compute_filter_projection_freshness_ref_digest
            if key == "freshness"
            else compute_filter_projection_readiness_ref_digest
        )
        mutated_ref[digest_field] = compute(mutated_ref)
        _expect_invalid(parse_filter_projection_result_v3, canonical_json({**success, key: mutated_ref}))
    # the corrected positive fixture is itself equation-exact
    assert success["returned_count"] == len(success["candidates"]) == 2
    assert success["cohort_selection_digest"] == cohort_selection_digest(success["cohort_selection"])
    assert parse_filter_projection_result_v3(canonical_json(success)) == success


def test_deferred_requested_target_shape() -> None:
    """F4: stale-only terminal fields are present exactly when status=stale and absent for not_ready."""

    stale = _stale_deferred_root()
    target = stale["requested_target_ref"]
    for missing in ("requested_terminal_id", "requested_terminal_digest", "route_revision_token"):
        hostile = {
            **stale,
            "requested_target_ref": {key: value for key, value in target.items() if key != missing},
        }
        _expect_invalid(parse_filter_projection_result_v3, canonical_json(hostile))
    not_ready = _deferred_root()
    for extra, wrong in (
        ("requested_terminal_id", "terminal-0"),
        ("requested_terminal_digest", _digest("requested-terminal")),
        ("route_revision_token", "route-token-0"),
    ):
        hostile = {
            **not_ready,
            "requested_target_ref": {**not_ready["requested_target_ref"], extra: wrong},
        }
        _expect_invalid(parse_filter_projection_result_v3, canonical_json(hostile))
    assert parse_filter_projection_result_v3(canonical_json(stale)) == stale
    assert parse_filter_projection_result_v3(canonical_json(not_ready)) == not_ready


def test_decode_boundary_duplicate_keys_and_plain_dicts() -> None:
    """F5: duplicate keys are rejected at decode; plain dicts never reach validation."""

    terminal = _terminal()
    canonical = canonical_json(terminal)
    duplicated = canonical.replace('"candidate_count":2', '"candidate_count":2,"candidate_count":3', 1)
    assert duplicated != canonical
    with pytest.raises(AgentRuntimeNamespaceRefError):
        strict_json_loads(duplicated)
    # the parse text boundary rejects the same duplicates with the family error
    with pytest.raises(FilterProjectionTerminalContractError):
        parse_filter_projection_product_terminal(duplicated)
    # a collapsed duplicate decode can never be laundered through a plain dict
    collapsed = json.loads(duplicated)
    assert collapsed["candidate_count"] == 3
    with pytest.raises(FilterProjectionTerminalContractError):
        parse_filter_projection_product_terminal(collapsed)
    nested = canonical_json(_success_root()).replace(
        '"role_match":"any"', '"role_match":"any","role_match":"all"', 1
    )
    with pytest.raises(AgentRuntimeNamespaceRefError):
        strict_json_loads(nested)
    with pytest.raises(FilterProjectionTerminalContractError):
        parse_filter_projection_result_v3(nested)
    assert parse_filter_projection_product_terminal(strict_json_loads(canonical)) == terminal
    for parser, record in (
        (parse_filter_projection_freshness_ref, _terminal()["freshness_ref"]),
        (parse_filter_projection_readiness_ref, _terminal()["readiness_ref"]),
        (parse_filter_projection_product_terminal, _terminal()),
        (parse_filter_projection_success_owner_ref, _success_owner_ref()),
        (parse_filter_projection_stale_owner_ref, _stale_owner_ref()),
        (parse_filter_projection_not_ready_owner_ref, _not_ready_owner_ref()),
        (parse_filter_projection_masked_absence_owner_ref, _masked_absence_owner_ref()),
        (parse_filter_projection_product_ref, _product_ref()),
    ):
        with pytest.raises(FilterProjectionTerminalContractError):
            parser(dict(record))
    with pytest.raises(FilterProjectionTerminalContractError):
        parse_filter_projection_result_v3(dict(_success_root()))
    with pytest.raises(FilterProjectionTerminalContractError):
        parse_filter_projection_result_v3(json.loads(canonical_json(_masked_root())))


def test_terminal_namespace_ref_tenant_mode_binding() -> None:
    """F2r3: a product terminal never carries a runtime namespace ref for another workspace or mode."""

    terminal = _terminal()
    for foreign_kwargs in (
        {"workspace_id": "ws-foreign", "provider_mode": "simulate"},
        {"workspace_id": "ws-1", "provider_mode": "scripted"},
        {"workspace_id": "ws-foreign", "provider_mode": "scripted"},
    ):
        foreign_ref = mint_agent_runtime_namespace_ref(
            namespace_ref_id="ns-foreign",
            policy_revision="policy-rev-1",
            generation=1,
            **foreign_kwargs,
        )
        drifted_core = {**_terminal_core(), "runtime_namespace_ref": foreign_ref}
        core_digest = compute_filter_projection_product_terminal_core_digest(drifted_core)
        drifted = {
            **drifted_core,
            "terminal_core_digest": core_digest,
            "freshness_ref": _freshness_ref(drifted_core, core_digest),
            "readiness_ref": _readiness_ref(drifted_core, core_digest),
        }
        drifted["terminal_digest"] = compute_filter_projection_product_terminal_digest(drifted)
        _expect_invalid(parse_filter_projection_product_terminal, drifted)
    assert parse_filter_projection_product_terminal(canonical_json(terminal)) == terminal


def test_readiness_counts_canonical_for_every_consumer() -> None:
    """F6r3: row_ready_count == visible_member_count == candidate_count inside the readiness parser itself."""

    readiness = _terminal()["readiness_ref"]
    for drifted, wrong in (
        ("row_ready_count", 0),
        ("row_ready_count", 1),
        ("visible_member_count", 1),
        ("candidate_count", 1),
    ):
        hostile = {**readiness, drifted: wrong}
        hostile["readiness_ref_digest"] = compute_filter_projection_readiness_ref_digest(hostile)
        # standalone readiness parsing rejects unready rows
        _expect_invalid(parse_filter_projection_readiness_ref, hostile)
        # and the V3 success parser inherits the same fence through the nested ref
        success = _success_root()
        _expect_invalid(parse_filter_projection_result_v3, {**success, "readiness": hostile})
    assert parse_filter_projection_readiness_ref(canonical_json(readiness)) == readiness


def test_result_v3_lane_coverage_derivation_table() -> None:
    """F7r3: status and counts can never contradict (requested = completed + missing; exact status rows)."""

    success = _success_root()
    coverage = success["requested_lane_coverage"]
    assert coverage == {"status": "complete", "requested_lane_count": 2, "completed_lane_count": 2, "missing_lane_count": 0}
    hostile_cases = [
        {**coverage, "status": "complete", "completed_lane_count": 0, "missing_lane_count": 2},  # the review probe
        {**coverage, "status": "complete", "completed_lane_count": 1, "missing_lane_count": 1},
        {**coverage, "status": "partial", "completed_lane_count": 2, "missing_lane_count": 0},
        {**coverage, "status": "partial", "completed_lane_count": 0, "missing_lane_count": 2},
        {**coverage, "status": "partial", "completed_lane_count": 1, "missing_lane_count": 2},  # 1+2 != requested 2
        {**coverage, "status": "unavailable", "completed_lane_count": 1, "missing_lane_count": 1},
        {**coverage, "status": "unavailable", "completed_lane_count": 0, "missing_lane_count": 1},
        {**coverage, "requested_lane_count": 3},  # 3 != 2 + 0
    ]
    for hostile_coverage in hostile_cases:
        _expect_invalid(
            parse_filter_projection_result_v3,
            {**success, "requested_lane_coverage": hostile_coverage},
        )
    # every consistent row of the derivation table passes, with the bound
    # per-lane summary evidence to match
    complete_summary = {"lane_id": "lane-0", "employment_status": "current", "role_bucket_id": "engineering", "coverage_status": "complete", "result_count": 1}
    missing_summary = {"lane_id": "lane-1", "employment_status": "current", "role_bucket_id": "engineering", "coverage_status": "missing", "result_count": 0}
    for good_coverage, good_summaries in (
        (
            {"status": "complete", "requested_lane_count": 2, "completed_lane_count": 2, "missing_lane_count": 0},
            [{**complete_summary, "lane_id": "lane-0"}, {**complete_summary, "lane_id": "lane-1"}],
        ),
        (
            {"status": "partial", "requested_lane_count": 2, "completed_lane_count": 1, "missing_lane_count": 1},
            [complete_summary, missing_summary],
        ),
        (
            {"status": "unavailable", "requested_lane_count": 2, "completed_lane_count": 0, "missing_lane_count": 2},
            [{**missing_summary, "lane_id": "lane-0"}, {**missing_summary, "lane_id": "lane-1"}],
        ),
    ):
        good = {**success, "requested_lane_coverage": good_coverage, "lane_summaries": good_summaries}
        assert parse_filter_projection_result_v3(canonical_json(good)) == good


def test_result_v3_page_candidate_refs_are_unique() -> None:
    """F8r3: the returned page is a window of the unique member set M; duplicate candidate_ref fails."""

    success = _success_root()
    duplicated = {
        **success,
        "candidates": [success["candidates"][0], success["candidates"][0]],
    }
    assert duplicated["candidates"][0]["candidate_ref"] == duplicated["candidates"][1]["candidate_ref"]
    assert duplicated["returned_count"] == len(duplicated["candidates"]) == 2
    _expect_invalid(parse_filter_projection_result_v3, duplicated)
    assert parse_filter_projection_result_v3(canonical_json(success)) == success


def test_public_namespace_projection_flows_through_real_v3_spec() -> None:
    """F1r4: the trusted-output projection is exact built-in JSON and flows through the real V3 spec."""

    spec = _v3_result_spec()
    success = _success_root()
    public = success["runtime_namespace_ref"]

    def _assert_builtin_types(value: Any) -> None:
        assert type(value) in (dict, list, str, int, bool, type(None)), type(value)
        if isinstance(value, dict):
            for key, child in value.items():
                assert type(key) is str
                _assert_builtin_types(child)
        elif isinstance(value, list):
            for child in value:
                _assert_builtin_types(child)

    _assert_builtin_types(public)
    # the helper output flows from the FF-SCHEMA parser through the real V3
    # ActionResultSpec with no conversion layer
    assert parse_filter_projection_result_v3(canonical_json(success)) == success
    serialized = spec.serialize(json.loads(canonical_json(success)))
    assert json.loads(serialized) == json.loads(canonical_json(success))
    # the old dict-subclass shape is exactly what the canonical validator rejects
    subclassed = {**success, "runtime_namespace_ref": _DictSubclass(public)}
    with pytest.raises(ActionResultSchemaError, match="action_result_not_strict_json"):
        spec.serialize(subclassed)


class _DictSubclass(dict):
    pass


def test_value_role_policy_differential_vs_action_result_spec() -> None:
    """F2r4: parser and serializer agree on every decision-locked value role."""

    spec = _v3_result_spec()
    success = _success_root()
    # positive: parser and serializer accept the same canonical root
    assert parse_filter_projection_result_v3(canonical_json(success)) == success
    spec.serialize(json.loads(canonical_json(success)))
    candidate = success["candidates"][0]
    lane_summary = success["lane_summaries"][0]
    # identifier role: path-like values are rejected by both layers
    for hostile in (
        {**success, "lane_summaries": [{**lane_summary, "lane_id": "/tmp/private.json"}, success["lane_summaries"][1]]},
        {**success, "runtime_namespace_ref": {**success["runtime_namespace_ref"], "namespace_ref_id": "/Users/me/private"}},
        {**success, "projection_ref": {**success["projection_ref"], "projection_id": "https://example.com/x"}},
        {**success, "lane_summaries": [{**lane_summary, "lane_id": "has space"}, success["lane_summaries"][1]]},
    ):
        _expect_invalid(parse_filter_projection_result_v3, hostile)
        _expect_spec_rejection(spec, hostile)
    # display_text role: whitespace-only display data is rejected by both layers
    for hostile in (
        {**success, "candidates": [{**candidate, "display_name": "   "}, success["candidates"][1]]},
        {**success, "candidates": [{**candidate, "headline": "\t\n "}, success["candidates"][1]]},
    ):
        _expect_invalid(parse_filter_projection_result_v3, hostile)
        _expect_spec_rejection(spec, hostile)
    # web_url role: a bare scheme is not a URL in either layer
    hostile = {**success, "candidates": [{**candidate, "public_profile_url": "https://"}, success["candidates"][1]]}
    _expect_invalid(parse_filter_projection_result_v3, hostile)
    _expect_spec_rejection(spec, hostile)
    # control role: closed const/enum values reject in both layers
    hostile = {**success, "lane_summaries": [{**lane_summary, "role_bucket_id": "sales"}, success["lane_summaries"][1]]}
    _expect_invalid(parse_filter_projection_result_v3, hostile)
    _expect_spec_rejection(spec, hostile)


def _sized_success_root(target: int) -> dict[str, Any]:
    """Build a success root whose canonical UTF-8 size is exactly ``target`` bytes."""

    root = _success_root()

    def _with_counts(candidates: list[dict[str, Any]]) -> dict[str, Any]:
        return {
            **root,
            "candidates": candidates,
            "returned_count": len(candidates),
            "total_count": len(candidates),
        }

    def _pad_list(candidates: list[dict[str, Any]], delta: int) -> None:
        for candidate in reversed(candidates):
            for field, maximum in (("display_name", 500), ("headline", 1000)):
                if delta == 0:
                    return
                take = min(delta, maximum - len(candidate[field]))
                candidate[field] = candidate[field] + "x" * take
                delta -= take
        raise AssertionError(f"not enough display room to tune {target} bytes")

    candidates: list[dict[str, Any]] = []
    index = 100
    while True:
        candidate = {
            "candidate_ref": _digest(f"candidate-{index}"),
            "display_name": "A" * 250,
            "headline": "h" * 500,
            "employment_statuses": ["current"],
            "role_bucket_ids": ["engineering"],
        }
        base = len(canonical_json(_with_counts([*candidates, candidate])).encode("utf-8"))
        delta = target - base
        if delta < 0:
            # the fresh candidate overshot: distribute the remaining padding
            # across the already-accumulated candidates
            assert candidates, "root alone unexpectedly exceeds the target size"
            previous_delta = target - len(canonical_json(_with_counts(candidates)).encode("utf-8"))
            assert previous_delta > 0
            _pad_list(candidates, previous_delta)
            tuned = _with_counts(candidates)
            assert len(canonical_json(tuned).encode("utf-8")) == target
            return tuned
        if delta == 0:
            return _with_counts([*candidates, candidate])
        if delta <= 1498:
            _pad_list([*candidates, candidate], delta)
            tuned = _with_counts([*candidates, candidate])
            assert len(canonical_json(tuned).encode("utf-8")) == target
            return tuned
        candidates.append(candidate)
        index += 1
        assert index < 250, "size tuning must stay inside the model-safe item bounds"


def test_result_v3_serialized_bytes_limit() -> None:
    """F3r4: the canonical 64-KiB model-safety limit binds parser and serializer identically."""

    manifest = _manifest()
    record = manifest["result_v3_slot_contract"]["canonical_fingerprints"]["result"]["record"]
    assert FILTER_PROJECTION_RESULT_V3_MAX_SERIALIZED_BYTES == record["max_serialized_bytes"] == 65536
    exact = _sized_success_root(65536)
    assert len(canonical_json(exact).encode("utf-8")) == 65536
    assert parse_filter_projection_result_v3(canonical_json(exact)) == exact
    over = _sized_success_root(65537)
    assert len(canonical_json(over).encode("utf-8")) == 65537
    _expect_invalid(parse_filter_projection_result_v3, over)
    # maximum-page regression: a fully shape-valid 250-candidate page exceeds the limit
    big = _success_root()
    big["candidates"] = [_candidate_item(index) for index in range(250)]
    for candidate in big["candidates"]:
        candidate["headline"] = "h" * 1000
    big["returned_count"] = 250
    big["total_count"] = 250
    assert len(canonical_json(big).encode("utf-8")) > 65536
    _expect_invalid(parse_filter_projection_result_v3, big)
    # the authoritative serializer agrees on the boundary
    spec = _v3_result_spec()
    spec.serialize(json.loads(canonical_json(exact)))
    with pytest.raises(ActionResultSchemaError, match="action_result_serialized_bytes_exceeded"):
        spec.serialize(json.loads(canonical_json(over)))


def test_zero_member_deferred_tuple_is_exact() -> None:
    """F4r4: projection_candidate_set_empty is exactly not_ready/retryable=false/reselection_required=true."""

    deferred = _deferred_root()
    assert deferred["reason"] == "projection_candidate_set_empty"
    assert deferred["status"] == "not_ready"
    assert deferred["retryable"] is False
    assert deferred["reselection_required"] is True
    assert parse_filter_projection_result_v3(canonical_json(deferred)) == deferred
    # impossible retry/reselection policies for an empty candidate set fail closed
    _expect_invalid(parse_filter_projection_result_v3, {**deferred, "retryable": True})
    _expect_invalid(parse_filter_projection_result_v3, {**deferred, "reselection_required": False})
    # an empty candidate set is never a stale outcome
    stale_target = {
        **deferred["requested_target_ref"],
        "requested_terminal_id": "terminal-0",
        "requested_terminal_digest": _digest("requested-terminal"),
        "route_revision_token": "route-token-0",
    }
    _expect_invalid(
        parse_filter_projection_result_v3,
        {**deferred, "status": "stale", "requested_target_ref": stale_target},
    )
    # other deferred reasons keep their owner-decided policy fields
    other = {
        **deferred,
        "reason": "projection_members_not_ready",
        "retryable": True,
        "reselection_required": False,
    }
    assert parse_filter_projection_result_v3(canonical_json(other)) == other


def test_coverage_counts_bound_to_lane_summary_evidence() -> None:
    """F5r4: coverage counts derive from unique per-lane summaries, one per requested lane."""

    success = _success_root()
    summaries = success["lane_summaries"]
    # zero summaries cannot evidence two completed lanes
    _expect_invalid(parse_filter_projection_result_v3, {**success, "lane_summaries": []})
    # duplicate lane identities are not per-lane evidence
    duplicated = [summaries[0], summaries[0]]
    _expect_invalid(parse_filter_projection_result_v3, {**success, "lane_summaries": duplicated})
    # fewer or more summaries than requested lanes contradict the coverage counts
    _expect_invalid(parse_filter_projection_result_v3, {**success, "lane_summaries": summaries[:1]})
    extra = {**summaries[0], "lane_id": "lane-2"}
    _expect_invalid(parse_filter_projection_result_v3, {**success, "lane_summaries": [*summaries, extra]})
    # counts must derive from summary states: one missing summary is one missing lane
    missing_summary = {**summaries[1], "coverage_status": "missing", "result_count": 0}
    _expect_invalid(
        parse_filter_projection_result_v3,
        {**success, "lane_summaries": [summaries[0], missing_summary]},
    )
    # the consistent partial row passes with matching evidence
    partial = {
        **success,
        "requested_lane_coverage": {
            "status": "partial",
            "requested_lane_count": 2,
            "completed_lane_count": 1,
            "missing_lane_count": 1,
        },
        "lane_summaries": [summaries[0], missing_summary],
    }
    assert parse_filter_projection_result_v3(canonical_json(partial)) == partial
    assert success["requested_lane_coverage"]["requested_lane_count"] == len(success["lane_summaries"]) == 2
    assert parse_filter_projection_result_v3(canonical_json(success)) == success


def test_retained_v2_history_never_retyped() -> None:
    manifest = _manifest()
    gates = manifest["retention_replay_migration_gates"]
    retained = set(gates["retained_literals"])
    assert set(RETAINED_FILTER_PROJECTION_HISTORY_LITERALS) <= retained
    assert set(REJECTED_FILTER_PROJECTION_LITERALS) == {"filter_projection_publication_terminal.v1"}
    assert "filter_projection_publication_terminal.v1" in gates["rejected_literals"]
    assert list(RETAINED_LOOKUP_FORBIDDEN) == gates["lookup_forbidden"]
    owned = set(ADOPTED) | set(REFERENCED)
    assert not (set(RETAINED_FILTER_PROJECTION_HISTORY_LITERALS) & owned)
    assert not (set(REJECTED_FILTER_PROJECTION_LITERALS) & owned)
    for literal in RETAINED_FILTER_PROJECTION_HISTORY_LITERALS:
        assert _git_grep_paths_at_base(literal) != [], literal
    for literal in owned:
        assert _git_grep_paths_at_base(literal) == [], literal


def test_base_commit_is_ancestor_of_head() -> None:
    merge_base = subprocess.run(
        ["git", "-C", str(REPO_ROOT), "merge-base", "--is-ancestor", BASE_COMMIT, "HEAD"],
        check=False,
    )
    assert merge_base.returncode == 0, "pinned packet base must be an ancestor of HEAD"
