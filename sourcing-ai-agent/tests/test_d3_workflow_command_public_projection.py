from __future__ import annotations

import ast
import copy
import json
import re
import subprocess
from pathlib import Path
from typing import Any

from sourcing_agent.command_kernel import (
    WORKFLOW_COMMAND_OPERATION_SYNC_PUBLIC_FIELDS,
    WORKFLOW_COMMAND_PRIVATE_PUBLIC_MIRROR_FIELDS,
    WORKFLOW_COMMAND_PUBLIC_DERIVED_FIELDS,
    WORKFLOW_COMMAND_PUBLIC_DESCRIPTOR_FIELDS,
    CommandKernel,
)
from sourcing_agent.repositories.workflow_runtime import WORKFLOW_COMMANDS

REPO_ROOT = Path(__file__).resolve().parents[1]
SOURCE_ROOT = REPO_ROOT / "src" / "sourcing_agent"
COMMAND_KERNEL_PATH = SOURCE_ROOT / "command_kernel.py"
ACQUISITION_OWNER_PATH = SOURCE_ROOT / "acquisition_command_owner.py"
ORCHESTRATOR_PATH = SOURCE_ROOT / "orchestrator.py"
API_PATH = SOURCE_ROOT / "api.py"
FRONTEND_SCHEMA_PATH = REPO_ROOT / "contracts" / "frontend_api_contract.schema.json"
FRONTEND_TYPES_PATH = REPO_ROOT / "contracts" / "frontend_api_contract.ts"
FRONTEND_ADAPTER_PATH = REPO_ROOT / "contracts" / "frontend_api_adapter.ts"
FRONTEND_DEMO_API_PATH = REPO_ROOT / "frontend-demo" / "src" / "lib" / "api.ts"
ESBUILD_MODULE_PATH = REPO_ROOT / "frontend-demo" / "node_modules" / "esbuild" / "lib" / "main.js"

SAFE_DIAGNOSTIC_FIELDS = ("claim_generation", "control_epoch")
D3C2A_DORMANT_INTERNAL_FIELDS = (
    "runtime_namespace",
    "provider_mode",
    "workspace_id",
    "scope_digest",
    "coordination_plan_review_id",
    "claim_authority_spec_digest",
    "expected_predecessor_intent_id",
    "expected_predecessor_phase_generation",
    "expected_predecessor_source_control_epoch",
    "expected_predecessor_decision_source_event_id",
    "d3_business_fence_digest",
    "claim_selection_generation",
    "consumed_claim_authority_id",
    "claim_token_digest",
    "heartbeat_sequence",
    "last_heartbeat_id",
    "terminal_event_id",
    "terminal_outcome_digest",
)
D3B_PRIVATE_BOOTSTRAP_CAPABILITY_FIELDS = (
    "bootstrap_authority",
    "bootstrap_authority_id",
    "bootstrap_authority_digest",
    "bootstrap_receipt",
    "issuer_digest",
    "issuer_revision",
    "lease_identity",
    "scoped_review_session_bootstrap_authority",
    "scoped_review_session_bootstrap_receipt",
)
EXPECTED_DERIVED_FIELDS = (
    "agent_exposure_gate",
    "agent_exposure_status",
    "display_contract",
    "control_policy",
    "control_state",
    "activity_spine_policy",
    "execution_summary",
)
EXPECTED_OPERATION_SYNC_FIELDS = (
    "status",
    "reason",
    "operation_run_id",
    "operation_status",
    "control_action",
    "command_status",
    "operation_run",
    "event",
    "workflow_command",
)
EXPECTED_PUBLIC_CARRIER_ROUTES = frozenset(
    {
        "/api/workflow/commands",
        "/api/workflow/commands/{command_id}",
        "/api/workflow/commands/{command_id}/cancel",
        "/api/workflow/commands/{command_id}/retry",
        "/api/workflow/commands/{command_id}/resume",
        "/api/operations/runs",
        "/api/operations/runs/{run_id}",
        "/api/operations/runs/{run_id}/provenance",
        "/api/operations/actions",
        "/api/operations/actions/{action_id}",
        "/api/operations/actions/{action_id}/approve",
        "/api/operations/actions/{action_id}/reject",
        "/api/operations/runs/{run_id}/cancel",
        "/api/operations/runs/{run_id}/retry",
        "/api/operations/runs/{run_id}/resume",
        "/api/operations/runs/{run_id}/dispatch",
    }
)
EXPECTED_PUBLIC_CARRIER_VARIANTS = frozenset(
    {
        ("GET", "/api/workflow/commands"),
        ("GET", "/api/workflow/commands/{command_id}"),
        ("POST", "/api/workflow/commands/{command_id}/cancel"),
        ("POST", "/api/workflow/commands/{command_id}/retry"),
        ("POST", "/api/workflow/commands/{command_id}/resume"),
        ("GET", "/api/operations/runs"),
        ("GET", "/api/operations/runs/{run_id}"),
        ("GET", "/api/operations/runs/{run_id}/provenance"),
        ("GET", "/api/operations/actions"),
        ("POST", "/api/operations/actions"),
        ("GET", "/api/operations/actions/{action_id}"),
        ("POST", "/api/operations/actions/{action_id}/approve"),
        ("POST", "/api/operations/actions/{action_id}/reject"),
        ("POST", "/api/operations/runs/{run_id}/cancel"),
        ("POST", "/api/operations/runs/{run_id}/retry"),
        ("POST", "/api/operations/runs/{run_id}/resume"),
        ("POST", "/api/operations/runs/{run_id}/dispatch"),
    }
)


class SyntheticClaimAuthority:
    def __eq__(self, other: object) -> bool:
        return isinstance(other, SyntheticClaimAuthority)


def _full_command_record() -> dict[str, Any]:
    record: dict[str, Any] = {}
    list_fields = {
        "input_artifact_refs",
        "output_artifact_refs",
        "downstream_command_ids",
        "artifact_refs",
    }
    object_fields = {"produced_entity_counts", "payload", "retry_policy", "result"}
    number_fields = {"attempt", "max_attempts", "claim_generation", "control_epoch"}
    for field in WORKFLOW_COMMAND_PUBLIC_DESCRIPTOR_FIELDS:
        if field in list_fields:
            record[field] = [f"{field}-value"]
        elif field in object_fields:
            record[field] = {"business_value": field}
        elif field in number_fields:
            record[field] = 3
        else:
            record[field] = f"{field}-value"
    return record


def _normalized_key(value: str) -> str:
    raw = str(value or "").strip().replace("-", "_")
    return re.sub(r"(?<=[a-z0-9])(?=[A-Z])", "_", raw).lower()


def _assert_no_private_capability(value: Any) -> None:
    if isinstance(value, dict):
        for key, item in value.items():
            normalized = _normalized_key(str(key))
            assert normalized not in WORKFLOW_COMMAND_PRIVATE_PUBLIC_MIRROR_FIELDS
            assert normalized not in D3B_PRIVATE_BOOTSTRAP_CAPABILITY_FIELDS
            assert not normalized.startswith(
                ("bootstrap_authority_", "claim_authority_", "claim_token_", "scoped_review_session_bootstrap_")
            )
            _assert_no_private_capability(item)
    elif isinstance(value, list):
        for item in value:
            _assert_no_private_capability(item)
    else:
        assert not isinstance(value, SyntheticClaimAuthority)


def _schema_ref_count(value: Any, target: str) -> int:
    if isinstance(value, dict):
        return int(value.get("$ref") == target) + sum(_schema_ref_count(item, target) for item in value.values())
    if isinstance(value, list):
        return sum(_schema_ref_count(item, target) for item in value)
    return 0


def _typescript_segment(source: str, start: str, end: str) -> str:
    start_offset = source.index(start)
    return source[start_offset : source.index(end, start_offset + len(start))]


def _raw_nested_command_return_count() -> int:
    count = 0
    for path in (COMMAND_KERNEL_PATH, ACQUISITION_OWNER_PATH, ORCHESTRATOR_PATH):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if not isinstance(node, ast.Dict):
                continue
            for key, value in zip(node.keys, node.values, strict=True):
                if (
                    isinstance(key, ast.Constant)
                    and key.value == "workflow_command"
                    and isinstance(value, ast.Name)
                    and value.id == "command_payload"
                ):
                    count += 1
    return count


def test_checked_in_public_field_contract_matches_descriptor_and_is_exactly_42() -> None:
    descriptor_fields = tuple(column.key for column in WORKFLOW_COMMANDS.columns)
    assert len(descriptor_fields) == 33
    assert WORKFLOW_COMMAND_PUBLIC_DESCRIPTOR_FIELDS == (*descriptor_fields, *SAFE_DIAGNOSTIC_FIELDS)
    assert WORKFLOW_COMMAND_PUBLIC_DERIVED_FIELDS == EXPECTED_DERIVED_FIELDS
    assert len(set(WORKFLOW_COMMAND_PUBLIC_DESCRIPTOR_FIELDS) | set(WORKFLOW_COMMAND_PUBLIC_DERIVED_FIELDS)) == 42
    assert WORKFLOW_COMMAND_OPERATION_SYNC_PUBLIC_FIELDS == EXPECTED_OPERATION_SYNC_FIELDS


def test_backend_projector_is_sparse_closed_recursive_and_non_mutating() -> None:
    kernel = CommandKernel(store=None)
    command = _full_command_record()
    command.update(
        {
            "unknown_future_column": "must-not-project",
            **{field: f"d3c2a-{field}" for field in D3C2A_DORMANT_INTERNAL_FIELDS},
            "claim_token": "top-level-secret",
            "payload": {
                "business_value": "preserved",
                "artifact_digest": "legitimate-business-digest",
                "nested": {
                    "claimToken": "secret",
                    "claim-token-digest": "secret",
                    "CLAIM_AUTHORITY_ID": "secret",
                    "lastHeartbeatId": "secret",
                },
                "items": (
                    {"leaseToken": "secret"},
                    {"safe": True, "claimAuthorityEnvelope": "secret"},
                    SyntheticClaimAuthority(),
                ),
                "bootstrapReceipt": {"safe": "must-not-survive-private-carrier"},
                "scopedReviewSessionBootstrapAuthority": {"safe": "must-not-survive-private-carrier"},
            },
            "result": {"safe_result": 7, "claimTokenPreview": "secret"},
            "retry_policy": {
                "bootstrapAuthorityId": "secret",
                "bootstrap-authority-digest": "secret",
                "issuerDigest": "secret",
                "issuerRevision": 7,
                "leaseIdentity": "secret",
                "safe_retry": True,
            },
            "produced_entity_counts": {"finite": 1.5, "nan": float("nan"), "inf": float("inf")},
        }
    )
    original = copy.deepcopy(command)

    projected = kernel._workflow_command_api_record(command)

    assert command == original
    assert "unknown_future_column" not in projected
    assert "claim_token" not in projected
    assert set(D3C2A_DORMANT_INTERNAL_FIELDS).isdisjoint(projected)
    assert projected["claim_generation"] == 3
    assert projected["control_epoch"] == 3
    assert projected["payload"]["business_value"] == "preserved"
    assert projected["payload"]["artifact_digest"] == "legitimate-business-digest"
    assert projected["payload"]["nested"] == {}
    assert projected["payload"]["items"] == [{}, {"safe": True}]
    assert projected["result"] == {"safe_result": 7}
    assert projected["retry_policy"] == {"safe_retry": True}
    assert projected["produced_entity_counts"] == {"finite": 1.5}
    assert set(projected) == (
        set(WORKFLOW_COMMAND_PUBLIC_DESCRIPTOR_FIELDS)
        | (set(WORKFLOW_COMMAND_PUBLIC_DERIVED_FIELDS) - {"execution_summary"})
    )
    _assert_no_private_capability(projected)
    for private_field in D3B_PRIVATE_BOOTSTRAP_CAPABILITY_FIELDS:
        assert private_field in WORKFLOW_COMMAND_PRIVATE_PUBLIC_MIRROR_FIELDS

    sparse = kernel._workflow_command_api_record({"command_id": "cmd-sparse", "status": None})
    assert "status" not in sparse
    assert "payload" not in sparse
    assert "claim_generation" not in sparse


def test_d3c2a_raw_postgres_columns_remain_dormant_behind_the_closed_descriptor() -> None:
    raw_row = {column: None for column in WORKFLOW_COMMANDS.column_names()}
    raw_row.update(
        {
            **{field: f"d3c2a-{field}" for field in D3C2A_DORMANT_INTERNAL_FIELDS},
            "claim_generation": 7,
            "control_epoch": 11,
        }
    )

    mapped = WORKFLOW_COMMANDS.from_row(raw_row)
    descriptor_fields = tuple(column.key for column in WORKFLOW_COMMANDS.columns)

    assert set(mapped) == set(descriptor_fields)
    assert set(D3C2A_DORMANT_INTERNAL_FIELDS).isdisjoint(mapped)
    assert set(SAFE_DIAGNOSTIC_FIELDS).isdisjoint(mapped)
    assert len(descriptor_fields) == 33


def test_operation_sync_and_recursive_public_carriers_share_the_same_sanitizer() -> None:
    kernel = CommandKernel(store=None)
    raw_command = {
        "command_id": "cmd-operation-sync",
        "command_type": "test.command",
        "owner": "test-owner",
        "status": "claimed",
        "claim_generation": 9,
        "control_epoch": 4,
        "claim_token": "secret",
        "unknown": "drop",
        "result": {"safe": True, "claimAuthoritySeal": "secret"},
    }
    operation_sync = kernel._workflow_command_operation_sync_api_record(
        {
            "status": "running",
            "unknown_sync_field": "drop",
            "operation_run": {
                "result_ref": {"safe": True, "claim_token_digest": "secret"},
            },
            "event": {"payload": {"safe": True, "leaseToken": "secret"}},
            "workflow_command": raw_command,
        }
    )

    assert set(operation_sync) == {"status", "operation_run", "event", "workflow_command"}
    assert operation_sync["operation_run"] == {"result_ref": {"safe": True}}
    assert operation_sync["event"] == {"payload": {"safe": True}}
    assert operation_sync["workflow_command"]["claim_generation"] == 9
    assert "unknown" not in operation_sync["workflow_command"]
    _assert_no_private_capability(operation_sync)

    carrier = kernel._workflow_command_public_carrier_api_record(
        {
            "action": {"input": {"safe": True, "claimToken": "secret"}},
            "events": [{"payload": {"claim_receipt": {"token": "secret"}, "safe": True}}],
            "workflow_command": {
                **raw_command,
                "unknown_nested_descriptor": "drop",
                "execution_summary": {"latest_activity": {"claimToken": "secret", "safe": True}},
            },
            "execution_summary": {"latest_activity": {"metadata": {"claim-authority-seal": "secret", "safe": True}}},
        }
    )
    _assert_no_private_capability(carrier)
    assert carrier["events"] == [{"payload": {"safe": True}}]
    assert "unknown_nested_descriptor" not in carrier["workflow_command"]
    assert carrier["workflow_command"]["execution_summary"] == {"latest_activity": {"safe": True}}
    assert carrier["execution_summary"]["latest_activity"]["metadata"] == {"safe": True}


def test_all_raw_nested_command_returns_and_public_compact_bypass_are_closed() -> None:
    assert _raw_nested_command_return_count() == 0

    call_count = 0
    for path in (COMMAND_KERNEL_PATH, ACQUISITION_OWNER_PATH, ORCHESTRATOR_PATH):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        call_count += sum(
            1
            for node in ast.walk(tree)
            if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "_workflow_command_operation_sync_api_record"
        )
    assert call_count == 7

    orchestrator_source = ORCHESTRATOR_PATH.read_text(encoding="utf-8")
    materialization_segment = _typescript_segment(
        orchestrator_source,
        "    def get_job_materialization_items",
        "    def ",
    )
    assert "_compact_public_workflow_command_payload(self._workflow_command_api_record(dict(command)))" in (
        materialization_segment
    )
    for method_name in (
        "_workflow_command_api_record_with_execution_summary",
        "_operation_action_api_record",
        "_operation_event_api_record",
        "_operation_run_api_record",
        "_operation_run_control_response_record",
    ):
        method_start = orchestrator_source.index(f"    def {method_name}")
        method_end = orchestrator_source.index("\n    def ", method_start + 8)
        assert "_workflow_command_public_carrier_api_record" in orchestrator_source[method_start:method_end]


def test_route_inventory_expands_to_all_16_public_carriers() -> None:
    api_tree = ast.parse(API_PATH.read_text(encoding="utf-8"))
    actual_variants: set[tuple[str, str]] = set()
    for node in ast.walk(api_tree):
        if not (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id == "add"
            and len(node.args) >= 2
            and isinstance(node.args[0], (ast.List, ast.Tuple))
            and isinstance(node.args[1], ast.Constant)
            and isinstance(node.args[1].value, str)
        ):
            continue
        path = node.args[1].value
        if path not in EXPECTED_PUBLIC_CARRIER_ROUTES:
            continue
        for method_node in node.args[0].elts:
            if isinstance(method_node, ast.Constant) and isinstance(method_node.value, str):
                actual_variants.add((method_node.value, path))

    assert len(EXPECTED_PUBLIC_CARRIER_ROUTES) == 16
    assert {path for _, path in actual_variants} == EXPECTED_PUBLIC_CARRIER_ROUTES
    assert len(EXPECTED_PUBLIC_CARRIER_VARIANTS) == 17
    assert actual_variants == EXPECTED_PUBLIC_CARRIER_VARIANTS


def test_frontend_schema_and_mappers_are_closed_and_operation_sync_is_typed() -> None:
    schema = json.loads(FRONTEND_SCHEMA_PATH.read_text(encoding="utf-8"))
    workflow_command = schema["$defs"]["WorkflowCommandRecord"]
    operation_sync = schema["$defs"]["WorkflowCommandOperationSync"]
    expected_fields = set(WORKFLOW_COMMAND_PUBLIC_DESCRIPTOR_FIELDS) | set(WORKFLOW_COMMAND_PUBLIC_DERIVED_FIELDS)

    assert workflow_command["additionalProperties"] is False
    assert set(workflow_command["properties"]) == expected_fields
    assert len(workflow_command["properties"]) == 42
    assert operation_sync["additionalProperties"] is False
    assert set(operation_sync["properties"]) == set(EXPECTED_OPERATION_SYNC_FIELDS)
    assert _schema_ref_count(schema, "#/$defs/WorkflowCommandRecord") == 7

    types_source = FRONTEND_TYPES_PATH.read_text(encoding="utf-8")
    adapter_source = FRONTEND_ADAPTER_PATH.read_text(encoding="utf-8")
    demo_source = FRONTEND_DEMO_API_PATH.read_text(encoding="utf-8")
    type_segment = _typescript_segment(
        types_source,
        "export interface WorkflowCommandRecord",
        "export interface WorkflowCommandExecutionSummary",
    )
    adapter_segment = _typescript_segment(
        adapter_source,
        "export function mapWorkflowCommandRecord",
        "export function mapWorkflowCommandListResponse",
    )
    demo_segment = _typescript_segment(
        demo_source,
        "function deriveWorkflowCommandRecord",
        "function deriveWorkflowActivityRecord",
    )
    assert "extends JsonObject" not in type_segment
    assert "[key: string]" not in type_segment
    assert "...(source as JsonObject)" not in adapter_segment
    assert "raw: record" not in demo_segment
    assert "WorkflowCommandOperationSync" in types_source
    assert "mapWorkflowCommandOperationSync" in adapter_source


def test_frontend_mappers_executably_drop_unknown_and_nested_capability_fields(tmp_path: Path) -> None:
    public_adapter_bundle = tmp_path / "frontend_api_adapter.cjs"
    demo_api_bundle = tmp_path / "demo_api.cjs"
    bundle_script = """
const esbuild = require(process.argv[1]);
esbuild.buildSync({
  entryPoints: [process.argv[2]],
  outfile: process.argv[3],
  bundle: true,
  platform: "node",
  format: "cjs",
  target: "node20",
  define: { "import.meta.env": "{}" },
});
"""
    for entrypoint, output_path in (
        (FRONTEND_ADAPTER_PATH, public_adapter_bundle),
        (FRONTEND_DEMO_API_PATH, demo_api_bundle),
    ):
        build = subprocess.run(
            [
                "node",
                "-e",
                bundle_script,
                str(ESBUILD_MODULE_PATH),
                str(entrypoint),
                str(output_path),
            ],
            cwd=REPO_ROOT,
            capture_output=True,
            text=True,
            check=False,
        )
        assert build.returncode == 0, build.stderr

    assertion_script = """
const assert = require("node:assert/strict");
const publicAdapter = require(process.argv[1]);
const demoApi = require(process.argv[2]);

const input = {
  command_id: "cmd-frontend-projection",
  command_type: "test.command",
  owner: "test-owner",
  status: "claimed",
  claim_generation: 5,
  control_epoch: 8,
  unknown_future_column: "drop",
  claim_token: "secret",
  payload: {
    safe: true,
    artifact_digest: "legitimate-business-digest",
    nested: { claimTokenDigest: "secret", safe: "preserved" },
    bootstrapReceipt: { safe: "must-not-survive-private-carrier" },
    scopedReviewSessionBootstrapAuthority: { safe: "must-not-survive-private-carrier" },
  },
  result: { safe_result: 7, "claim-authority-seal": "secret" },
  produced_entity_counts: { finite: 1.5, nan: Number.NaN, inf: Number.POSITIVE_INFINITY },
  retry_policy: {
    bootstrapAuthorityId: "secret",
    "bootstrap-authority-digest": "secret",
    issuerDigest: "secret",
    issuerRevision: 7,
    leaseIdentity: "secret",
    safe_retry: true,
  },
};

function normalizedKey(value) {
  return String(value || "").trim().replaceAll("-", "_")
    .replace(/(?<=[a-z0-9])(?=[A-Z])/g, "_").toLowerCase();
}

function assertNoPrivate(value) {
  if (Array.isArray(value)) {
    for (const item of value) assertNoPrivate(item);
    return;
  }
  if (!value || typeof value !== "object") return;
  for (const [key, item] of Object.entries(value)) {
    const normalized = normalizedKey(key);
    assert.equal(normalized.startsWith("claim_token"), false);
    assert.equal(normalized.startsWith("claim_authority"), false);
    assert.equal(normalized.startsWith("bootstrap_authority"), false);
    assert.equal(normalized.startsWith("scoped_review_session_bootstrap"), false);
    assert.notEqual(normalized, "lease_token");
    assert.notEqual(normalized, "issuer_digest");
    assert.notEqual(normalized, "issuer_revision");
    assert.notEqual(normalized, "lease_identity");
    assertNoPrivate(item);
  }
}

const mapped = publicAdapter.mapWorkflowCommandRecord(input);
const demo = demoApi.deriveWorkflowCommandRecord(input);
assert.equal(mapped.unknown_future_column, undefined);
assert.equal(mapped.claim_token, undefined);
assert.equal(mapped.payload.safe, true);
assert.equal(mapped.payload.artifact_digest, "legitimate-business-digest");
assert.deepEqual(mapped.payload.nested, { safe: "preserved" });
assert.deepEqual(mapped.result, { safe_result: 7 });
assert.deepEqual(mapped.produced_entity_counts, { finite: 1.5 });
assert.deepEqual(mapped.retry_policy, { safe_retry: true });
assert.equal(demo.raw.unknown_future_column, undefined);
assert.equal(demo.raw.claim_token, undefined);
assert.equal(demo.raw.payload.artifact_digest, "legitimate-business-digest");
assert.deepEqual(demo.raw.produced_entity_counts, { finite: 1.5 });
assert.deepEqual(demo.raw.retry_policy, { safe_retry: true });
assertNoPrivate(mapped);
assertNoPrivate(demo);
"""
    assertion = subprocess.run(
        [
            "node",
            "-e",
            assertion_script,
            str(public_adapter_bundle),
            str(demo_api_bundle),
        ],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    assert assertion.returncode == 0, assertion.stderr
