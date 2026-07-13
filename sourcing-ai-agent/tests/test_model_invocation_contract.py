from __future__ import annotations

import ast
import hashlib
import json
from dataclasses import FrozenInstanceError, fields, replace
from pathlib import Path

import pytest

from sourcing_agent.model_route_registry import (
    DEFAULT_MODEL_ROUTE_SPECS,
    MODEL_ROUTE_MANIFEST_KEYS,
    MODEL_ROUTE_MANIFEST_ROUTE_KEYS,
    MODEL_ROUTE_SPEC_RECORD_KEYS,
    ModelRouteRegistryError,
    model_route_registry_manifest,
    validate_model_route_registry_manifest,
    validate_model_route_specs,
)
from sourcing_agent.model_tool_runtime import (
    D0A_EFFECT_AUTHORIZATION_AVAILABLE,
    MODEL_INVOCATION_ENVELOPE_RECORD_KEYS,
    MODEL_INVOCATION_ENVELOPE_SCHEMA_VERSION,
    ModelIdentity,
    ModelInvocationEnvelopeError,
    ModelInvocationEnvelopeV1,
    ModelInvocationMirrorError,
    ToolTurnResult,
    validate_tool_turn_result_envelope_mirror,
)
from sourcing_agent.model_usage import ModelUsage

REPO_ROOT = Path(__file__).resolve().parents[1]
SOURCE_ROOT = REPO_ROOT / "src" / "sourcing_agent"


def _digest(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _canonical_digest(value: object) -> str:
    encoded = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _route():
    return DEFAULT_MODEL_ROUTE_SPECS[0]


def _result(*, provider_mode: str = "scripted") -> ToolTurnResult:
    route = _route()
    return ToolTurnResult(
        text="Synthetic terminal response.",
        tool_calls=(),
        usage=ModelUsage(input_tokens=12, output_tokens=7, total_tokens=19, cached_input_tokens=2),
        usage_status="reported",
        model_identity=ModelIdentity(
            requested_model=route.model,
            response_model=route.model,
            effective_model=route.model,
        ),
        terminal_reason="end_turn",
        provider_call_id="synthetic_provider_call_001",
        route_id=route.route_id,
        workspace_id="synthetic_workspace",
        actor_id="synthetic_actor",
        runtime_namespace="test:model-invocation-envelope",
        provider_mode=provider_mode,  # type: ignore[arg-type]
        canonical_request_sha256=_digest("synthetic_canonical_request"),
    )


def _envelope_for_result(result: ToolTurnResult) -> ModelInvocationEnvelopeV1:
    route = _route()
    return ModelInvocationEnvelopeV1(
        schema_version=MODEL_INVOCATION_ENVELOPE_SCHEMA_VERSION,
        route_id=result.route_id,
        route_revision=route.revision,
        provider=route.provider,
        api_style=route.api_style,
        requested_model=result.model_identity.requested_model,
        response_model=result.model_identity.response_model,
        effective_model=result.model_identity.effective_model,
        model_identity_provenance=result.model_identity.provenance,
        effective_route_snapshot_ref=None,
        effective_route_snapshot_digest=_digest("synthetic_effective_route_snapshot"),
        circuit_identity=route.circuit_key,
        runtime_namespace=result.runtime_namespace,
        provider_mode=result.provider_mode,  # type: ignore[arg-type]
        workspace_id=result.workspace_id,
        actor_id=result.actor_id,
        permission_scope="agent:synthetic",
        prompt_policy_version="synthetic_prompt_policy_v1",
        permission_scope_revision="synthetic_permission_revision_v1",
        outbound_policy_revision="synthetic_outbound_revision_v1",
        model_safe_schema_revision="synthetic_model_safe_revision_v1",
        operation_run_id=None,
        turn_id=None,
        step_id=None,
        workflow_command_id=None,
        activity_run_id=None,
        activity_attempt_id=None,
        provider_call_id=result.provider_call_id,
        terminal_reason=result.terminal_reason,
        usage=result.usage,
        usage_status=result.usage_status,
        fallback_status="not_used",
        circuit_state="not_checked",
        evidence_bundle_hash=None,
        canonical_result_digest=result.canonical_outcome_sha256,
        result_artifact_ref=None,
        result_artifact_digest=None,
        cost_exposure_ref=None,
        canonical_request_digest=result.canonical_request_sha256,
    )


def _fully_bound_schema_fixture() -> ModelInvocationEnvelopeV1:
    """Synthetic schema-only value; it performs no live or provider operation."""

    return replace(
        _envelope_for_result(_result()),
        provider_mode="live",
        effective_route_snapshot_ref="synthetic://route-snapshot/001",
        operation_run_id="synthetic_operation_001",
        turn_id="synthetic_turn_001",
        step_id="synthetic_step_001",
        workflow_command_id="synthetic_command_001",
        activity_run_id="synthetic_activity_001",
        activity_attempt_id="synthetic_attempt_001",
        evidence_bundle_hash=_digest("synthetic_evidence_bundle"),
        result_artifact_ref="synthetic://result-artifact/001",
        result_artifact_digest=_digest("synthetic_result_artifact"),
        cost_exposure_ref="synthetic://cost-exposure/001",
        circuit_state="closed",
    )


def test_invocation_envelope_has_one_exact_immutable_physical_schema() -> None:
    envelope = _envelope_for_result(_result())
    record = envelope.to_record()
    dataclass_field_names = {item.name for item in fields(ModelInvocationEnvelopeV1)}

    assert set(record) == MODEL_INVOCATION_ENVELOPE_RECORD_KEYS
    assert dataclass_field_names == MODEL_INVOCATION_ENVELOPE_RECORD_KEYS - {"envelope_digest"}
    assert record["schema_version"] == MODEL_INVOCATION_ENVELOPE_SCHEMA_VERSION
    unsigned_record = dict(record)
    envelope_digest = unsigned_record.pop("envelope_digest")
    assert envelope_digest == _canonical_digest(unsigned_record)
    assert envelope_digest == envelope.envelope_digest
    assert ModelInvocationEnvelopeV1.from_record(record) == envelope
    assert record["operation_run_id"] is None
    assert record["effective_route_snapshot_ref"] is None
    assert record["evidence_bundle_hash"] is None
    assert record["result_artifact_ref"] is None
    assert record["result_artifact_digest"] is None
    assert record["cost_exposure_ref"] is None

    with pytest.raises(FrozenInstanceError):
        envelope.route_id = "changed"  # type: ignore[misc]
    with pytest.raises(AttributeError):
        object.__setattr__(envelope, "raw_payload", {})


def test_invocation_envelope_record_rejects_key_deletion_unknown_extra_and_digest_drift() -> None:
    envelope = _envelope_for_result(_result())
    record = envelope.to_record()

    for deleted_key in MODEL_INVOCATION_ENVELOPE_RECORD_KEYS:
        mutated = dict(record)
        del mutated[deleted_key]
        with pytest.raises(ModelInvocationEnvelopeError, match="record_keyset_invalid"):
            ModelInvocationEnvelopeV1.from_record(mutated)

    for forbidden_key in ("api_key", "authorization", "raw_payload", "credentials"):
        mutated = {**record, forbidden_key: "forbidden"}
        with pytest.raises(ModelInvocationEnvelopeError, match="record_keyset_invalid"):
            ModelInvocationEnvelopeV1.from_record(mutated)

    mutated = {**record, "envelope_digest": _digest("wrong_envelope")}
    with pytest.raises(ModelInvocationEnvelopeError, match="envelope_digest_mismatch"):
        ModelInvocationEnvelopeV1.from_record(mutated)

    constructor_values = {item.name: getattr(envelope, item.name) for item in fields(ModelInvocationEnvelopeV1)}
    constructor_values["api_key"] = "forbidden"
    with pytest.raises(TypeError):
        ModelInvocationEnvelopeV1(**constructor_values)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "usage_key",
    [
        "input_tokens",
        "output_tokens",
        "total_tokens",
        "cached_input_tokens",
        "reasoning_output_tokens",
    ],
)
def test_invocation_envelope_rejects_noncanonical_explicit_null_usage_aliases(usage_key: str) -> None:
    record = _envelope_for_result(_result()).to_record()
    usage = dict(record["usage"])
    assert usage_key not in usage or usage[usage_key] is not None
    usage[usage_key] = None
    mutated = {**record, "usage": usage}

    with pytest.raises(ModelInvocationEnvelopeError, match="envelope_digest_mismatch"):
        ModelInvocationEnvelopeV1.from_record(mutated)

    unsigned = dict(mutated)
    unsigned.pop("envelope_digest")
    mutated["envelope_digest"] = _canonical_digest(unsigned)
    with pytest.raises(ModelInvocationEnvelopeError, match="record_not_canonical"):
        ModelInvocationEnvelopeV1.from_record(mutated)


def test_invocation_envelope_digest_is_sensitive_to_every_dataclass_field() -> None:
    envelope = _fully_bound_schema_fixture()
    replacements: dict[str, object] = {
        "route_id": "synthetic.route.other",
        "route_revision": _digest("route_revision_other"),
        "provider": "synthetic_provider_other",
        "api_style": "synthetic_api_style_other",
        "requested_model": "synthetic-model-requested-other",
        "response_model": "synthetic-model-response-other",
        "effective_model": "synthetic-model-effective-other",
        "effective_route_snapshot_ref": "synthetic://route-snapshot/002",
        "effective_route_snapshot_digest": _digest("route_snapshot_other"),
        "circuit_identity": "synthetic_circuit_other",
        "runtime_namespace": "test:model-invocation-envelope-other",
        "workspace_id": "synthetic_workspace_other",
        "actor_id": "synthetic_actor_other",
        "permission_scope": "agent:synthetic-other",
        "prompt_policy_version": "synthetic_prompt_policy_v2",
        "permission_scope_revision": "synthetic_permission_revision_v2",
        "outbound_policy_revision": "synthetic_outbound_revision_v2",
        "model_safe_schema_revision": "synthetic_model_safe_revision_v2",
        "operation_run_id": "synthetic_operation_002",
        "turn_id": "synthetic_turn_002",
        "step_id": "synthetic_step_002",
        "workflow_command_id": "synthetic_command_002",
        "activity_run_id": "synthetic_activity_002",
        "activity_attempt_id": "synthetic_attempt_002",
        "provider_call_id": "synthetic_provider_call_002",
        "terminal_reason": "length",
        "usage": ModelUsage(input_tokens=13, output_tokens=7, total_tokens=20, cached_input_tokens=2),
        "usage_status": "invalid",
        "fallback_status": "blocked",
        "circuit_state": "open",
        "evidence_bundle_hash": _digest("evidence_bundle_other"),
        "canonical_result_digest": _digest("canonical_result_other"),
        "result_artifact_ref": "synthetic://result-artifact/002",
        "result_artifact_digest": _digest("result_artifact_other"),
        "cost_exposure_ref": "synthetic://cost-exposure/002",
        "canonical_request_digest": _digest("canonical_request_other"),
    }
    record_only_fields = {
        "schema_version": "model_invocation_envelope_v2",
        "model_identity_provenance": "caller_claimed",
    }
    assert set(replacements) | set(record_only_fields) | {"provider_mode"} == {
        item.name for item in fields(ModelInvocationEnvelopeV1)
    }

    for field_name, replacement in replacements.items():
        changed = replace(envelope, **{field_name: replacement})
        assert changed.envelope_digest != envelope.envelope_digest, field_name

    unsigned_record = envelope.to_record()
    unsigned_record.pop("envelope_digest")
    baseline_digest = _canonical_digest(unsigned_record)
    for field_name, replacement in record_only_fields.items():
        changed_record = {**unsigned_record, field_name: replacement}
        assert _canonical_digest(changed_record) != baseline_digest, field_name
        with pytest.raises(ModelInvocationEnvelopeError):
            replace(envelope, **{field_name: replacement})

    non_live = _envelope_for_result(_result())
    assert replace(non_live, provider_mode="live").envelope_digest != non_live.envelope_digest


@pytest.mark.parametrize(
    ("field_name", "invalid_value", "error"),
    [
        ("route_id", "", "invalid_route_id"),
        ("actor_id", True, "invalid_actor_id"),
        ("provider", " padded ", "invalid_provider"),
        ("route_revision", "not-sha256", "route_revision_sha256"),
        ("effective_route_snapshot_digest", "A" * 64, "snapshot_digest_sha256"),
        ("provider_mode", "replay", "provider_mode_invalid"),
        ("terminal_reason", "unknown", "terminal_reason_invalid"),
        ("usage_status", "unknown", "usage_status_invalid"),
        ("fallback_status", "silent_reroute", "fallback_status_invalid"),
        ("circuit_state", "unknown", "circuit_state_invalid"),
        ("canonical_request_digest", "short", "canonical_request_digest_sha256"),
    ],
)
def test_invocation_envelope_rejects_invalid_types_digests_and_enums(
    field_name: str,
    invalid_value: object,
    error: str,
) -> None:
    with pytest.raises(ModelInvocationEnvelopeError, match=error):
        replace(_envelope_for_result(_result()), **{field_name: invalid_value})


def test_invocation_envelope_rejects_partial_causality_artifact_and_opaque_usage() -> None:
    envelope = _envelope_for_result(_result())

    with pytest.raises(ModelInvocationEnvelopeError, match="causality_must_be_complete_or_absent"):
        replace(envelope, operation_run_id="synthetic_operation_only")
    with pytest.raises(ModelInvocationEnvelopeError, match="artifact_pair_incomplete"):
        replace(envelope, result_artifact_ref="synthetic://artifact/without-digest")
    with pytest.raises(ModelInvocationEnvelopeError, match="artifact_pair_incomplete"):
        replace(envelope, result_artifact_digest=_digest("digest_without_ref"))
    with pytest.raises(ModelInvocationEnvelopeError, match="usage_type_invalid"):
        replace(envelope, usage={})  # type: ignore[arg-type]
    with pytest.raises(ModelInvocationEnvelopeError, match="invalid_provider_call_id"):
        replace(envelope, provider_call_id="")
    with pytest.raises(ModelInvocationEnvelopeError, match="reported_usage_empty"):
        replace(envelope, usage=ModelUsage())
    with pytest.raises(ModelInvocationEnvelopeError, match="unavailable_usage_present"):
        replace(envelope, usage_status="unavailable")


@pytest.mark.parametrize("terminal_reason", ["length", "content_filter"])
def test_invocation_envelope_preserves_quarantined_non_authorizable_outcomes(
    terminal_reason: str,
) -> None:
    result = replace(
        _result(),
        terminal_reason=terminal_reason,
        provider_call_id=None,
        usage=ModelUsage(),
        usage_status="unavailable",
    )
    envelope = _envelope_for_result(result)

    assert result.eligible_for_policy_evaluation is False
    assert envelope.provider_call_id is None
    assert envelope.terminal_reason == terminal_reason
    assert envelope.usage_status == "unavailable"
    assert ModelInvocationEnvelopeV1.from_record(envelope.to_record()) == envelope
    assert validate_tool_turn_result_envelope_mirror(result, envelope) is None


def test_tool_turn_result_envelope_mirror_compares_only_shared_authority_fields() -> None:
    result = _result()
    envelope = _envelope_for_result(result)

    assert validate_tool_turn_result_envelope_mirror(result, envelope) is None
    assert D0A_EFFECT_AUTHORIZATION_AVAILABLE is False
    assert (
        validate_tool_turn_result_envelope_mirror(
            result,
            replace(envelope, prompt_policy_version="different_but_not_result_owned"),
        )
        is None
    )


@pytest.mark.parametrize(
    ("field_name", "replacement", "expected_mismatch"),
    [
        ("route_id", "synthetic.route.other", "route_id"),
        ("workspace_id", "synthetic_workspace_other", "workspace_id"),
        ("actor_id", "synthetic_actor_other", "actor_id"),
        ("runtime_namespace", "test:model-invocation-other", "runtime_namespace"),
        ("provider_mode", "live", "provider_mode"),
        ("canonical_request_digest", _digest("other_request"), "canonical_request_digest"),
        ("requested_model", "other-requested-model", "requested_model"),
        ("response_model", "other-response-model", "response_model"),
        ("effective_model", "other-effective-model", "effective_model"),
        ("terminal_reason", "length", "terminal_reason"),
        ("provider_call_id", None, "provider_call_id"),
        ("usage", ModelUsage(input_tokens=1), "usage"),
        ("usage_status", "invalid", "usage_status"),
        ("canonical_result_digest", _digest("other_result"), "canonical_result_digest"),
    ],
)
def test_tool_turn_result_envelope_mirror_fails_closed_on_mismatch_and_mode_pollution(
    field_name: str,
    replacement: object,
    expected_mismatch: str,
) -> None:
    envelope = replace(_envelope_for_result(_result()), **{field_name: replacement})

    with pytest.raises(ModelInvocationMirrorError, match=expected_mismatch):
        validate_tool_turn_result_envelope_mirror(_result(), envelope)


def test_route_registry_preflight_enforces_exact_unique_draft_manifest() -> None:
    manifest = model_route_registry_manifest()

    assert set(manifest) == MODEL_ROUTE_MANIFEST_KEYS
    routes = manifest["routes"]
    assert isinstance(routes, list)
    assert routes
    assert all(set(route) == MODEL_ROUTE_MANIFEST_ROUTE_KEYS for route in routes)
    assert all(set(spec.to_record()) == MODEL_ROUTE_SPEC_RECORD_KEYS for spec in DEFAULT_MODEL_ROUTE_SPECS)
    assert manifest["live_enabled"] is False
    assert all(route["rollout_state"] == "draft" for route in routes)
    assert len({route["route_id"] for route in routes}) == len(routes)
    assert len({route["circuit_key"] for route in routes}) == len(routes)
    assert validate_model_route_registry_manifest(manifest) is None


def test_route_registry_preflight_rejects_route_and_circuit_duplicates() -> None:
    first, second = DEFAULT_MODEL_ROUTE_SPECS
    duplicate_route = replace(second, route_id=first.route_id, circuit_key="synthetic_unique_circuit")
    with pytest.raises(ModelRouteRegistryError, match="model_route_duplicate"):
        validate_model_route_specs((first, duplicate_route), require_draft_only=True)

    duplicate_circuit = replace(second, route_id="synthetic.unique.route", circuit_key=first.circuit_key)
    with pytest.raises(ModelRouteRegistryError, match="circuit_key_duplicate"):
        validate_model_route_specs((first, duplicate_circuit), require_draft_only=True)


def test_route_registry_manifest_preflight_rejects_key_deletion_and_live_or_canary_pollution() -> None:
    manifest = model_route_registry_manifest()

    missing_top_level = dict(manifest)
    del missing_top_level["routes"]
    with pytest.raises(ModelRouteRegistryError, match="manifest_keyset_invalid"):
        validate_model_route_registry_manifest(missing_top_level)

    missing_route_key = json.loads(json.dumps(manifest))
    del missing_route_key["routes"][0]["budget_class"]
    with pytest.raises(ModelRouteRegistryError, match="route_keyset_invalid"):
        validate_model_route_registry_manifest(missing_route_key)

    live_enabled = {**manifest, "live_enabled": True}
    with pytest.raises(ModelRouteRegistryError, match="live_must_remain_disabled"):
        validate_model_route_registry_manifest(live_enabled)

    canary = json.loads(json.dumps(manifest))
    canary["routes"][0]["rollout_state"] = "canary"
    with pytest.raises(ModelRouteRegistryError, match="route_mismatch|route_not_draft"):
        validate_model_route_registry_manifest(canary)


def test_d0c_contract_modules_have_no_transport_settings_environment_or_storage_dependency() -> None:
    expected_imports = {
        SOURCE_ROOT / "model_tool_runtime.py": {
            "__future__",
            "abc",
            "codecs",
            "dataclasses",
            "hashlib",
            "json",
            "math",
            "model_route_registry",
            "model_usage",
            "re",
            "types",
            "typing",
        },
        SOURCE_ROOT / "model_route_registry.py": {
            "__future__",
            "dataclasses",
            "hashlib",
            "json",
            "types",
            "typing",
        },
    }
    forbidden_dynamic_calls: list[tuple[str, str]] = []
    for path, expected in expected_imports.items():
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        observed_imports: set[str] = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                observed_imports.update(alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module:
                observed_imports.add(node.module)
            elif (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Name)
                and node.func.id in {"__import__", "eval", "exec", "open"}
            ):
                forbidden_dynamic_calls.append((path.name, node.func.id))

        assert observed_imports == expected, path.name

    assert forbidden_dynamic_calls == []
    envelope_annotations = {item.name: str(item.type) for item in fields(ModelInvocationEnvelopeV1)}
    assert all("Any" not in annotation for annotation in envelope_annotations.values())
    assert all("Mapping" not in annotation and "dict" not in annotation for annotation in envelope_annotations.values())


def test_model_invocation_envelope_has_one_production_class_owner() -> None:
    owners = []
    for path in sorted(SOURCE_ROOT.rglob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        if any(isinstance(node, ast.ClassDef) and node.name == "ModelInvocationEnvelopeV1" for node in ast.walk(tree)):
            owners.append(path.relative_to(REPO_ROOT).as_posix())

    assert owners == ["src/sourcing_agent/model_tool_runtime.py"]
