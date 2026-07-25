"""FF-SCHEMA tests for the ``agent_runtime_namespace_ref.v1`` contract module.

Every expectation is recomputed from the canonical decision manifest
(``filter_projection_lineage_fixed_forward_decision_v1.json``); the manifest is
the only design source.  Collision checks scan tracked Git content at the
pinned packet base commit, so they are stable both before and after this lane
lands and after later waves adopt the module.
"""

from __future__ import annotations

import hashlib
import json
import re
import subprocess
from pathlib import Path
from typing import Any

import pytest

from sourcing_agent.agent_runtime_namespace_ref import (
    AGENT_RUNTIME_NAMESPACE_REF_CONTRACT_DIGEST,
    AGENT_RUNTIME_NAMESPACE_REF_ORDERED_FIELDS,
    AGENT_RUNTIME_NAMESPACE_REF_OWNER,
    AGENT_RUNTIME_NAMESPACE_REF_PROVIDER_MODES,
    AGENT_RUNTIME_NAMESPACE_REF_PUBLIC_FIELDS,
    AGENT_RUNTIME_NAMESPACE_REF_SCHEMA,
    AGENT_RUNTIME_NAMESPACE_REF_SCHEMA_VERSION,
    RETAINED_LOOKUP_FORBIDDEN,
    RETAINED_PATH_BEARING_HISTORY_LITERALS,
    RETAINED_V1_CAPABILITY_LITERAL,
    AgentRuntimeNamespaceRefError,
    CanonicalJsonObject,
    agent_runtime_namespace_ref_public_record,
    assert_not_retained_path_bearing_history,
    canonical_json,
    compute_agent_runtime_namespace_ref_digest,
    contract_digest,
    mint_agent_runtime_namespace_ref,
    parse_agent_runtime_namespace_ref,
    strict_json_loads,
    validate_agent_runtime_namespace_ref,
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


def _manifest() -> dict[str, Any]:
    value = json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))
    assert type(value) is dict
    return value


def _manifest_row(literal: str) -> dict[str, Any]:
    return next(row for row in _manifest()["contract_digests"] if row["literal"] == literal)


def _recompute(schema: dict[str, Any]) -> str:
    blob = json.dumps(schema, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def _git_output(*args: str) -> str:
    result = subprocess.run(
        ["git", "-C", str(REPO_ROOT), *args],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, (args, result.stderr)
    return result.stdout


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


def _valid_ref_record() -> dict[str, Any]:
    return mint_agent_runtime_namespace_ref(
        namespace_ref_id="ns-1",
        workspace_id="ws-1",
        provider_mode="simulate",
        policy_revision="policy-rev-1",
        generation=1,
    )


def _expect_invalid(record: Any) -> None:
    # Plain dicts are not a decode boundary; hostile record shapes go through
    # the text path so validation (not the boundary) is what rejects them.
    payload = canonical_json(record) if type(record) is dict else record
    with pytest.raises(AgentRuntimeNamespaceRefError):
        parse_agent_runtime_namespace_ref(payload)
    with pytest.raises(AgentRuntimeNamespaceRefError):
        validate_agent_runtime_namespace_ref(payload)


def test_constants_match_manifest_exactly() -> None:
    row = _manifest_row("agent_runtime_namespace_ref.v1")
    assert AGENT_RUNTIME_NAMESPACE_REF_SCHEMA_VERSION == row["literal"] == "agent_runtime_namespace_ref.v1"
    assert AGENT_RUNTIME_NAMESPACE_REF_OWNER == row["contract_owner"] == "agent_runtime_namespace_registry"
    assert list(AGENT_RUNTIME_NAMESPACE_REF_ORDERED_FIELDS) == row["ordered_fields"]
    envelope = _manifest()["runtime_namespace_capability_envelope"]["namespace_ref"]
    assert envelope["schema_version"] == AGENT_RUNTIME_NAMESPACE_REF_SCHEMA_VERSION
    assert envelope["contract_owner"] == AGENT_RUNTIME_NAMESPACE_REF_OWNER
    assert envelope["ordered_fields"] == list(AGENT_RUNTIME_NAMESPACE_REF_ORDERED_FIELDS)
    assert envelope["public_fields"] == list(AGENT_RUNTIME_NAMESPACE_REF_PUBLIC_FIELDS)
    assert envelope["ref_digest_rule"] == "ref_digest covers the first seven fields"
    provider_mode = next(
        field for field in row["schema"]["fields"] if field["name"] == "provider_mode"
    )
    assert list(AGENT_RUNTIME_NAMESPACE_REF_PROVIDER_MODES) == provider_mode["enum"] == ["simulate", "scripted"]


def test_schema_object_and_digest_match_manifest() -> None:
    row = _manifest_row("agent_runtime_namespace_ref.v1")
    # The module schema object is byte-identical to the manifest row schema.
    assert json.loads(canonical_json(AGENT_RUNTIME_NAMESPACE_REF_SCHEMA)) == row["schema"]
    # Digest equality: manifest pin, module constant, and a fresh recomputation all agree.
    assert _recompute(row["schema"]) == row["contract_digest"]
    assert AGENT_RUNTIME_NAMESPACE_REF_CONTRACT_DIGEST == row["contract_digest"]
    assert contract_digest(AGENT_RUNTIME_NAMESPACE_REF_SCHEMA) == row["contract_digest"]
    assert re.fullmatch(r"[0-9a-f]{64}", AGENT_RUNTIME_NAMESPACE_REF_CONTRACT_DIGEST)


def test_manifest_module_field_order_consistency() -> None:
    row = _manifest_row("agent_runtime_namespace_ref.v1")
    manifest_names = [field["name"] for field in row["schema"]["fields"]]
    module_names = [field["name"] for field in AGENT_RUNTIME_NAMESPACE_REF_SCHEMA["fields"]]
    assert module_names == manifest_names == row["ordered_fields"]
    assert tuple(module_names) == AGENT_RUNTIME_NAMESPACE_REF_ORDERED_FIELDS


def test_mint_validate_parse_round_trip() -> None:
    record = _valid_ref_record()
    assert list(record) == list(AGENT_RUNTIME_NAMESPACE_REF_ORDERED_FIELDS)
    assert record["schema_version"] == AGENT_RUNTIME_NAMESPACE_REF_SCHEMA_VERSION
    assert record["owner"] == AGENT_RUNTIME_NAMESPACE_REF_OWNER
    expected_digest = hashlib.sha256(
        canonical_json({field: record[field] for field in AGENT_RUNTIME_NAMESPACE_REF_ORDERED_FIELDS[:-1]}).encode(
            "utf-8"
        )
    ).hexdigest()
    assert record["ref_digest"] == expected_digest
    assert compute_agent_runtime_namespace_ref_digest(record) == expected_digest
    parsed = parse_agent_runtime_namespace_ref(record)
    assert parsed == record
    assert parse_agent_runtime_namespace_ref(parsed) == record
    assert canonical_json(parsed) == canonical_json(record)
    validate_agent_runtime_namespace_ref(record)


def test_mint_rejects_invalid_inputs_fail_closed() -> None:
    for bad_kwargs in (
        {"namespace_ref_id": ""},
        {"workspace_id": ""},
        {"provider_mode": "live"},
        {"provider_mode": "replay"},
        {"policy_revision": ""},
        {"generation": 0},
        {"generation": -1},
        {"generation": 1.0},
        {"generation": True},
        {"generation": "1"},
    ):
        kwargs = {
            "namespace_ref_id": "ns-1",
            "workspace_id": "ws-1",
            "provider_mode": "scripted",
            "policy_revision": "policy-rev-1",
            "generation": 1,
        }
        kwargs.update(bad_kwargs)
        with pytest.raises(AgentRuntimeNamespaceRefError):
            mint_agent_runtime_namespace_ref(**kwargs)


def test_public_record_is_exact_public_subset() -> None:
    record = _valid_ref_record()
    public = agent_runtime_namespace_ref_public_record(record)
    assert public == {"schema_version": "agent_runtime_namespace_ref.v1", "namespace_ref_id": "ns-1", "ref_digest": record["ref_digest"]}
    assert set(AGENT_RUNTIME_NAMESPACE_REF_PUBLIC_FIELDS) == set(public)
    # trusted output: exact built-in JSON types, never a dict subclass, so the
    # decision-locked downstream validators accept it without conversion.
    assert type(public) is dict
    assert all(type(value) is str for value in public.values())
    # workspace, path, lifecycle, provider_mode, policy_revision, generation never cross the boundary.
    for private in ("workspace_id", "provider_mode", "policy_revision", "generation", "owner"):
        assert private not in public
    assert public["ref_digest"] == record["ref_digest"]


def test_parse_fail_closed_on_hostile_mutations() -> None:
    record = _valid_ref_record()
    mutations = [
        {**record, "tenant_hint": "ws-1"},  # unknown extra field fails closed
        {key: value for key, value in record.items() if key != "workspace_id"},  # missing required field
        {**record, "schema_version": "agent_runtime_namespace_ref.v2"},
        {**record, "owner": "cohort_runtime"},
        {**record, "provider_mode": "live"},
        {**record, "provider_mode": "SIMULATE"},
        {**record, "generation": 0},
        {**record, "generation": True},
        {**record, "generation": 1.0},
        {**record, "namespace_ref_id": ""},
        {**record, "ref_digest": record["ref_digest"].upper()},
        {**record, "ref_digest": record["ref_digest"][:-1]},
        {**record, "ref_digest": "g" * 64},
        {**record, "ref_digest": "0" * 64},  # well-formed but does not recompute
        {**record, "policy_revision": "policy-rev-2"},  # copied digest is never proof
    ]
    for hostile in mutations:
        _expect_invalid(hostile)
    for hostile in ("ns-1", None, ["ns-1"], 1, True):
        _expect_invalid(hostile)


def test_decode_boundary_rejects_duplicates_and_collapsed_decodes() -> None:
    record = _valid_ref_record()
    canonical = canonical_json(record)
    # duplicate keys are rejected at decode at any depth (manifest canonical-JSON rule)
    duplicate_generation = canonical.replace(
        '"generation":1', '"generation":true,"generation":1', 1
    )
    assert '"generation":true,"generation":1' in duplicate_generation
    with pytest.raises(AgentRuntimeNamespaceRefError):
        strict_json_loads(duplicate_generation)
    # the parse text boundary invokes the same strict decoder
    with pytest.raises(AgentRuntimeNamespaceRefError):
        parse_agent_runtime_namespace_ref(duplicate_generation)
    with pytest.raises(AgentRuntimeNamespaceRefError):
        strict_json_loads(canonical.replace('"owner":"agent_runtime_namespace_registry"', '"owner":"a","owner":"b"', 1))
    # non-JSON constants are rejected
    for payload in ('{"x": NaN}', '{"x": Infinity}', '{"x": -Infinity}'):
        with pytest.raises(AgentRuntimeNamespaceRefError):
            strict_json_loads(payload)
    # rerun3 probe: a standard decode collapses duplicates; the collapsed
    # dictionary cannot be laundered into validation — public parsing takes
    # raw text/bytes or decoder provenance, and the programmatic wrapper is
    # no longer public decode provenance.
    collapsed = json.loads(duplicate_generation)
    assert collapsed["generation"] == 1
    with pytest.raises(AgentRuntimeNamespaceRefError):
        parse_agent_runtime_namespace_ref(collapsed)
    import sourcing_agent.agent_runtime_namespace_ref as ns_module

    assert "canonical_json_object" not in ns_module.__all__
    assert not hasattr(ns_module, "canonical_json_object")
    # plain dictionaries are never a decode boundary, even when fully valid
    with pytest.raises(AgentRuntimeNamespaceRefError):
        parse_agent_runtime_namespace_ref(dict(record))
    with pytest.raises(AgentRuntimeNamespaceRefError):
        parse_agent_runtime_namespace_ref(json.loads(canonical))
    # the text/bytes boundary and decoder provenance both round-trip
    assert parse_agent_runtime_namespace_ref(canonical) == record
    assert parse_agent_runtime_namespace_ref(canonical.encode("utf-8")) == record
    assert parse_agent_runtime_namespace_ref(record) == record
    assert strict_json_loads(canonical) == record
    assert strict_json_loads(bytearray(canonical.encode("utf-8"))) == record
    for bad_payload in (b"\xff\xfe{}", 1, None, ["x"]):
        with pytest.raises(AgentRuntimeNamespaceRefError):
            strict_json_loads(bad_payload)
    with pytest.raises(AgentRuntimeNamespaceRefError):
        strict_json_loads("{not json")
    # the decoder type cannot be constructed outside the decoder
    with pytest.raises(AgentRuntimeNamespaceRefError):
        CanonicalJsonObject({"a": 1})
    with pytest.raises(AgentRuntimeNamespaceRefError):
        CanonicalJsonObject(_token=object(), a=1)
    # canonical_json still rejects non-finite values on encode
    with pytest.raises(AgentRuntimeNamespaceRefError):
        canonical_json({"x": float("nan")})


def test_retained_v1_path_bearing_history_fence() -> None:
    manifest = _manifest()
    gates = manifest["retention_replay_migration_gates"]
    assert RETAINED_V1_CAPABILITY_LITERAL in gates["retained_literals"]
    assert set(RETAINED_PATH_BEARING_HISTORY_LITERALS) <= set(gates["retained_literals"])
    assert list(RETAINED_LOOKUP_FORBIDDEN) == gates["lookup_forbidden"]
    # The retained v1 capability stays a path-bearing record owned elsewhere;
    # presenting it for mint/parse fails closed instead of re-minting a ref.
    retained_v1_capability = {
        "schema_version": "cohort_execution_capability.v1",
        "owner": "cohort_runtime",
        "policy_revision": "policy-rev-1",
        "provider_mode": "simulate",
        "runtime_namespace": "/tmp/legacy-runtime/ns-1",
        "max_provider_calls": 4,
        "max_provider_items": 250,
        "max_output_candidates": 250,
        "role_proof_verifier_id": "",
        "role_proof_verifier_revision": "",
    }
    assert retained_v1_capability["runtime_namespace"].startswith("/")
    with pytest.raises(AgentRuntimeNamespaceRefError):
        assert_not_retained_path_bearing_history(retained_v1_capability)
    _expect_invalid(retained_v1_capability)
    assert_not_retained_path_bearing_history(_valid_ref_record())


def test_zero_repo_collision_at_base_commit() -> None:
    merge_base = subprocess.run(
        ["git", "-C", str(REPO_ROOT), "merge-base", "--is-ancestor", BASE_COMMIT, "HEAD"],
        check=False,
    )
    assert merge_base.returncode == 0, "pinned packet base must be an ancestor of HEAD"
    assert _git_grep_paths_at_base("agent_runtime_namespace_ref.v1") == []
    # Retained history remains byte-referenced at the base: the fence never erases it.
    assert _git_grep_paths_at_base(RETAINED_V1_CAPABILITY_LITERAL) != []


def test_lookup_forbidden_rules_are_exact() -> None:
    assert RETAINED_LOOKUP_FORBIDDEN == (
        "shape inference",
        "lexicographic latest",
        "mutable current alias",
        "auto-upgrade",
    )
