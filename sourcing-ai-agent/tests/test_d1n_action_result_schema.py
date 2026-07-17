from __future__ import annotations

import json
from dataclasses import FrozenInstanceError, replace
from pathlib import Path
from types import MappingProxyType
from unittest.mock import patch

import pytest

from sourcing_agent.action_result_schema import (
    ACTION_RESULT_PROVENANCE_CLASSES,
    ACTION_RESULT_REGISTRY_SCHEMA_VERSION,
    ACTION_RESULT_VALIDATOR_OWNER,
    ACTION_RESULT_VALUE_ROLES,
    ACTION_RESULT_VARIANTS,
    DEFAULT_ACTION_RESULT_REGISTRY,
    ActionResultActionOwner,
    ActionResultQueryOwner,
    ActionResultRegistry,
    ActionResultSchemaError,
    ActionResultSpec,
)
from sourcing_agent.model_tool_runtime import MAX_MESSAGE_CONTENT_BYTES, ToolResultMessage, ToolSpec

_SERIALIZER_CONTRACT = {
    "schema_version": "projection_search_result_serializer_contract_v1",
    "owner_output": "closed_terminal_variant",
}


def _variant_schema(variant: str, *, with_artifact_ref: bool = False) -> dict[str, object]:
    properties: dict[str, object] = {
        "variant": {"type": "string", "const": variant},
        "status": {"type": "string", "minLength": 1, "maxLength": 40},
        "items": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "id": {"type": "string", "minLength": 1, "maxLength": 80},
                    "score": {"type": "number", "minimum": 0, "maximum": 1},
                },
                "required": ["id", "score"],
                "additionalProperties": False,
            },
            "maxItems": 4,
        },
    }
    required = ["variant", "status", "items"]
    if with_artifact_ref:
        properties["result_artifact_ref"] = {"type": "string", "minLength": 1, "maxLength": 160}
        required.append("result_artifact_ref")
    return {
        "type": "object",
        "properties": properties,
        "required": required,
        "additionalProperties": False,
    }


def _schemas(*, with_artifact_ref: bool = False, reverse: bool = False) -> dict[str, dict[str, object]]:
    variants = tuple(reversed(ACTION_RESULT_VARIANTS)) if reverse else ACTION_RESULT_VARIANTS
    return {variant: _variant_schema(variant, with_artifact_ref=with_artifact_ref) for variant in variants}


def _schema_field_paths(schema: dict[str, object], *, prefix: str = "") -> set[str]:
    paths: set[str] = set()
    if schema.get("type") == "object":
        properties = schema.get("properties")
        assert isinstance(properties, dict)
        for field_name, child in properties.items():
            assert isinstance(child, dict)
            token = field_name.replace("~", "~0").replace("/", "~1")
            field_path = f"{prefix}/{token}"
            paths.add(field_path)
            paths.update(_schema_field_paths(child, prefix=field_path))
    elif schema.get("type") == "array":
        items = schema.get("items")
        assert isinstance(items, dict)
        paths.update(_schema_field_paths(items, prefix=f"{prefix}/*"))
    return paths


def _provenance(
    schemas: dict[str, dict[str, object]],
    *,
    default: str = "owner_state",
) -> dict[str, dict[str, str]]:
    return {
        variant: {
            field_path: "server_derived" if field_path == "/variant" else default
            for field_path in sorted(_schema_field_paths(schema))
        }
        for variant, schema in schemas.items()
    }


def _schema_string_paths(schema: dict[str, object], *, prefix: str = "") -> set[str]:
    if schema.get("type") == "string":
        return {prefix}
    paths: set[str] = set()
    if schema.get("type") == "object":
        properties = schema.get("properties")
        assert isinstance(properties, dict)
        for field_name, child in properties.items():
            assert isinstance(child, dict)
            token = field_name.replace("~", "~0").replace("/", "~1")
            paths.update(_schema_string_paths(child, prefix=f"{prefix}/{token}"))
    elif schema.get("type") == "array":
        items = schema.get("items")
        assert isinstance(items, dict)
        paths.update(_schema_string_paths(items, prefix=f"{prefix}/*"))
    return paths


def _value_roles(schemas: dict[str, dict[str, object]]) -> dict[str, dict[str, str]]:
    result: dict[str, dict[str, str]] = {}
    for variant, schema in schemas.items():
        roles: dict[str, str] = {}
        for path in sorted(_schema_string_paths(schema)):
            field_name = path.removesuffix("/*").rsplit("/", 1)[-1]
            if path == "/variant":
                role = "control"
            elif field_name == "source_url":
                role = "web_url"
            elif (
                field_name in {"artifact_ref", "artifact_refs"}
                or field_name.endswith("_artifact_ref")
                or field_name.endswith("_artifact_refs")
            ):
                role = "opaque_artifact_ref"
            elif field_name == "status":
                role = "display_text"
            else:
                role = "identifier"
            roles[path] = role
        result[variant] = roles
    return result


def _spec(
    *,
    action_type: str = "search_projection",
    schemas: dict[str, dict[str, object]] | None = None,
    field_provenance: dict[str, dict[str, str]] | None = None,
    field_value_roles: dict[str, dict[str, str]] | None = None,
    max_serialized_bytes: int = 4096,
    max_items: int = 64,
    max_depth: int = 6,
    artifact_ref_schemes: tuple[str, ...] = (),
    tool_kind: str = "action",
) -> ActionResultSpec:
    effective_schemas = _schemas() if schemas is None else schemas
    owner_binding = (
        ActionResultActionOwner(action_type=action_type)
        if tool_kind == "action"
        else ActionResultQueryOwner(
            owner_id="operation_query_owner",
            owner_revision="operation_query_owner_v1",
            owner_contract_digest="1" * 64,
        )
    )
    return ActionResultSpec(
        tool_name=action_type,
        tool_kind=tool_kind,  # type: ignore[arg-type]
        owner_binding=owner_binding,
        result_schema_version="search_projection_result_v1",
        serializer_owner="projection_search_service.result_serializer_v1",
        serializer_revision="projection_search_result_serializer_v1",
        serializer_contract=_SERIALIZER_CONTRACT,
        validator_owner=ACTION_RESULT_VALIDATOR_OWNER,
        variant_schemas=effective_schemas,
        field_provenance=_provenance(effective_schemas) if field_provenance is None else field_provenance,
        field_value_roles=_value_roles(effective_schemas) if field_value_roles is None else field_value_roles,
        max_serialized_bytes=max_serialized_bytes,
        max_items=max_items,
        max_depth=max_depth,
        artifact_ref_schemes=artifact_ref_schemes,
    )


def _payload(variant: str = "success") -> dict[str, object]:
    return {
        "variant": variant,
        "status": "ready" if variant == "success" else variant,
        "items": [{"id": "person_2", "score": 0.75}, {"id": "person_1", "score": 1.0}],
    }


def test_default_registry_is_empty_and_does_not_serve_any_action() -> None:
    assert DEFAULT_ACTION_RESULT_REGISTRY.tool_names == ()
    assert DEFAULT_ACTION_RESULT_REGISTRY.to_manifest_record() == {
        "schema_version": ACTION_RESULT_REGISTRY_SCHEMA_VERSION,
        "tool_count": 0,
        "retained_spec_count": 0,
        "tools": [],
    }
    assert DEFAULT_ACTION_RESULT_REGISTRY.specs_for_tool("search_projection") == ()
    with pytest.raises(ActionResultSchemaError, match="action_result_historical_spec_missing:search_projection"):
        DEFAULT_ACTION_RESULT_REGISTRY.require_historical("search_projection", "search_projection_result_v1", "0" * 64)


def test_spec_and_registry_are_deeply_immutable() -> None:
    spec = _spec()
    registry = ActionResultRegistry((spec,))

    assert isinstance(spec.variant_schemas, MappingProxyType)
    assert isinstance(spec.variant_schemas["success"], MappingProxyType)
    assert isinstance(spec.field_provenance, MappingProxyType)
    assert isinstance(spec.field_provenance["success"], MappingProxyType)
    assert isinstance(spec.field_value_roles, MappingProxyType)
    assert isinstance(spec.field_value_roles["success"], MappingProxyType)
    assert isinstance(spec.serializer_contract, MappingProxyType)
    assert isinstance(registry.specs, tuple)
    with pytest.raises(TypeError):
        spec.variant_schemas["success"] = {}  # type: ignore[index]
    with pytest.raises(TypeError):
        spec.variant_schemas["success"]["type"] = "array"  # type: ignore[index]
    with pytest.raises(TypeError):
        spec.field_provenance["success"]["/status"] = "model_inferred"  # type: ignore[index]
    with pytest.raises(TypeError):
        spec.field_value_roles["success"]["/status"] = "identifier"  # type: ignore[index]
    with pytest.raises(TypeError):
        registry.specs[0] = spec  # type: ignore[index]
    with pytest.raises(FrozenInstanceError):
        spec.max_items = 100  # type: ignore[misc]


def test_digest_and_serialized_bytes_are_canonical_and_order_independent() -> None:
    first = _spec(schemas=_schemas())
    second_schemas = _schemas(reverse=True)
    for schema in second_schemas.values():
        schema["properties"] = dict(reversed(list(dict(schema["properties"]).items())))
    second = _spec(schemas=second_schemas)

    first_payload = _payload()
    second_payload = dict(reversed(list(first_payload.items())))
    assert first.result_schema_digest == second.result_schema_digest
    assert first.serialize_bytes(first_payload) == second.serialize_bytes(second_payload)
    assert first.serialize(first_payload) == json.dumps(
        first_payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    assert len(first.result_schema_digest) == 64
    assert first.to_manifest_record()["serializer_revision"] == "projection_search_result_serializer_v1"
    assert len(first.to_manifest_record()["serializer_contract_digest"]) == 64


def test_result_digest_binds_serializer_pins_and_field_provenance() -> None:
    baseline = _spec()
    changed_serializer = replace(
        baseline,
        serializer_revision="projection_search_result_serializer_v2",
        serializer_contract={"schema_version": "projection_search_result_serializer_contract_v2"},
    )
    schemas = _schemas()
    provenance = _provenance(schemas)
    provenance["success"]["/status"] = "model_inferred"
    changed_provenance = _spec(schemas=schemas, field_provenance=provenance)

    assert baseline.result_schema_digest != changed_serializer.result_schema_digest
    assert baseline.result_schema_digest != changed_provenance.result_schema_digest


def test_result_digest_binds_interpreter_policy_value_roles_and_canonical_serializer_contract() -> None:
    baseline = _spec()
    reordered_contract = dict(reversed(list(_SERIALIZER_CONTRACT.items())))
    reordered = replace(baseline, serializer_contract=reordered_contract)
    schemas = _schemas()
    roles = _value_roles(schemas)
    roles["success"]["/status"] = "identifier"
    changed_role = _spec(schemas=schemas, field_value_roles=roles)

    assert baseline.serializer_contract_digest == reordered.serializer_contract_digest
    assert baseline.result_schema_digest == reordered.result_schema_digest
    assert baseline.result_schema_digest != changed_role.result_schema_digest
    assert baseline.to_manifest_record()["interpretation_contract_digest"] == baseline.interpretation_contract_digest
    assert len(baseline.interpretation_contract_digest) == 64
    assert set(ACTION_RESULT_VALUE_ROLES) == {
        "control",
        "identifier",
        "display_text",
        "web_url",
        "opaque_artifact_ref",
    }


@pytest.mark.parametrize("variant", ACTION_RESULT_VARIANTS)
def test_all_declared_variants_validate_and_are_tool_result_message_safe(variant: str) -> None:
    spec = _spec()
    content = spec.serialize(_payload(variant))

    assert json.loads(content)["variant"] == variant
    assert len(content.encode("utf-8")) <= spec.max_serialized_bytes <= MAX_MESSAGE_CONTENT_BYTES
    assert ToolResultMessage(tool_call_id="call_1", content=content).content == content


def test_tool_spec_validate_input_is_the_single_schema_validator() -> None:
    spec = _spec()
    payload = _payload()
    with patch.object(ToolSpec, "validate_input", autospec=True, return_value=payload) as validator:
        assert json.loads(spec.serialize(payload)) == payload
    validator.assert_called_once()
    assert validator.call_args.args[0].name == "search_projection:success:result"
    assert validator.call_args.args[1] == payload


@pytest.mark.parametrize(
    "payload",
    [
        {**_payload(), "unknown": "leak"},
        {"variant": "success", "status": "ready"},
        {**_payload(), "variant": "stale"},
        {**_payload(), "variant": "error", "status": "ready", "items": [], "detail": "raw"},
    ],
)
def test_unknown_missing_or_undeclared_result_fields_fail_closed(payload: dict[str, object]) -> None:
    with pytest.raises(ActionResultSchemaError):
        _spec().serialize(payload)


@pytest.mark.parametrize(
    "payload",
    [
        {**_payload(), "items": [{"id": "person_1", "score": float("nan")}]},
        {**_payload(), "items": [{"id": "person_1", "score": float("inf")}]},
        {**_payload(), "items": [{"id": Path("/private/result.json"), "score": 1.0}]},
        {**_payload(), "items": ({"id": "person_1", "score": 1.0},)},
    ],
)
def test_nonfinite_and_non_json_owner_output_fail_closed(payload: dict[str, object]) -> None:
    with pytest.raises(ActionResultSchemaError, match="action_result_(?:nonfinite_number|not_strict_json)"):
        _spec().serialize(payload)


@pytest.mark.parametrize(
    "raw_path",
    [
        "/Users/operator/private/result.json",
        "../runtime/result.json",
        "~/result.json",
        "C:\\private\\result.json",
        "runtime/results/result.json",
        "relative/results.json",
    ],
)
def test_raw_local_path_values_fail_closed(raw_path: str) -> None:
    payload = _payload()
    payload["status"] = raw_path
    with pytest.raises(ActionResultSchemaError, match="action_result_raw_local_path_forbidden"):
        _spec().serialize(payload)


def test_explicit_opaque_artifact_reference_policy() -> None:
    spec = _spec(
        schemas=_schemas(with_artifact_ref=True),
        artifact_ref_schemes=("asset", "artifact"),
    )
    payload = {**_payload(), "result_artifact_ref": "artifact:projection/result-1"}
    assert json.loads(spec.serialize(payload))["result_artifact_ref"] == payload["result_artifact_ref"]
    dotted_payload = {**_payload(), "result_artifact_ref": "artifact:projection/result-1.json"}
    assert json.loads(spec.serialize(dotted_payload))["result_artifact_ref"] == dotted_payload["result_artifact_ref"]

    for invalid_ref in (
        "artifact://projection/result-1",
        "artifact:../private/result.json",
        "artifact:/private/result.json",
        "artifact:file:/private/result.json",
        "artifact:https://example.test/result.json",
        "artifact:urn:result-1",
        "artifact:projection/file:/private/result.json",
        "file:/private/result.json",
        "https://example.test/result.json",
        "unknown:result-1",
        "/private/result.json",
    ):
        with pytest.raises(ActionResultSchemaError, match="artifact_ref"):
            spec.serialize({**payload, "result_artifact_ref": invalid_ref})


def test_plural_opaque_artifact_references_use_the_same_explicit_value_role() -> None:
    schemas = _schemas()
    for schema in schemas.values():
        properties = dict(schema["properties"])
        properties["result_artifact_refs"] = {
            "type": "array",
            "items": {"type": "string", "minLength": 1, "maxLength": 160},
            "maxItems": 4,
        }
        schema["properties"] = properties
        schema["required"] = [*schema["required"], "result_artifact_refs"]
    spec = _spec(schemas=schemas, artifact_ref_schemes=("artifact",))
    refs = ["artifact:projection/a.json", "artifact:projection/b.json"]

    assert json.loads(spec.serialize({**_payload(), "result_artifact_refs": refs}))["result_artifact_refs"] == refs
    with pytest.raises(ActionResultSchemaError, match="artifact_ref"):
        spec.serialize({**_payload(), "result_artifact_refs": ["https://example.test/a.json"]})


def test_artifact_reference_field_requires_an_explicit_policy() -> None:
    with pytest.raises(ActionResultSchemaError, match="artifact_ref_policy_required"):
        _spec(schemas=_schemas(with_artifact_ref=True))


def test_canonical_artifact_reference_fields_require_exact_scalar_or_bounded_list_shape() -> None:
    for field_name, invalid_schema in (
        ("artifact_ref", {"type": "object", "properties": {}, "additionalProperties": False}),
        ("artifact_refs", {"type": "array", "items": {"type": "string"}}),
    ):
        schemas = _schemas()
        for schema in schemas.values():
            properties = dict(schema["properties"])
            properties[field_name] = invalid_schema
            schema["properties"] = properties
        with pytest.raises(ActionResultSchemaError, match="artifact_ref_schema_invalid"):
            _spec(schemas=schemas)


@pytest.mark.parametrize(
    "field_name",
    [
        "artifact_url",
        "artifact_uri",
        "artifact_file",
        "artifact_filename",
        "artifact_handle",
        "artifact_href",
        "artifact_hrefs",
        "artifact_download",
        "artifact_endpoint",
        "artifact_hyperlink",
        "artifact_link",
        "artifact_locator",
        "artifact_pointer",
        "artifact_src",
        "artifact_srcs",
        "artifact_address",
        "artifacturl",
        "resultartifacthref",
        "artifactHref",
        "artifactSrc",
        "artifacts_url",
        "Artifact_Ref",
        "artifactRef",
        "artifactURLValue",
        "resultArtifactDownloadValue",
        "result_artifact_reference",
    ],
)
def test_noncanonical_artifact_locator_aliases_are_rejected(field_name: str) -> None:
    schemas = _schemas()
    for schema in schemas.values():
        properties = dict(schema["properties"])
        properties[field_name] = {"type": "string", "maxLength": 200}
        schema["properties"] = properties
    with pytest.raises(ActionResultSchemaError, match="artifact_locator_field_noncanonical"):
        _spec(schemas=schemas)


@pytest.mark.parametrize("nested_field", ["ref", "download", "endpoint"])
def test_nested_artifact_locator_is_rejected_by_ancestor_aware_schema_path_policy(nested_field: str) -> None:
    schemas = _schemas()
    for schema in schemas.values():
        properties = dict(schema["properties"])
        properties["artifact"] = {
            "type": "object",
            "properties": {nested_field: {"type": "string", "maxLength": 200}},
            "required": [nested_field],
            "additionalProperties": False,
        }
        schema["properties"] = properties
    with pytest.raises(ActionResultSchemaError, match="artifact_locator_field_noncanonical"):
        _spec(schemas=schemas)


def test_unknown_artifact_string_role_cannot_carry_a_network_locator() -> None:
    schemas = _schemas()
    for schema in schemas.values():
        properties = dict(schema["properties"])
        properties["artifact_access"] = {"type": "string", "maxLength": 200}
        schema["properties"] = properties
    spec = _spec(schemas=schemas)
    with pytest.raises(ActionResultSchemaError, match="artifact_locator_value_noncanonical"):
        spec.serialize({**_payload(), "artifact_access": "https://example.test/private-result"})


@pytest.mark.parametrize(
    ("field_name", "value"),
    [
        ("artifact_destination", "s3:bucket/object"),
        ("artifact_target", "gs:bucket/object"),
        ("artifact_key", "urn:private-result"),
        ("artifactdestination", "s3:bucket/object"),
        ("artifact_destination", "locator s3:bucket/object"),
        ("artifact_destination", "see (s3:bucket/object)"),
        ("artifact_destination", "value=artifact:private"),
        ("artifact_destination", "prefix urn:artifact:item"),
    ],
)
def test_noncanonical_artifact_metadata_cannot_carry_opaque_locator(field_name: str, value: str) -> None:
    schemas = _schemas()
    for schema in schemas.values():
        properties = schema["properties"]
        assert isinstance(properties, dict)
        properties[field_name] = {"type": "string", "maxLength": 200}
        required = schema["required"]
        assert isinstance(required, list)
        required.append(field_name)
    spec = _spec(schemas=schemas)
    with pytest.raises(ActionResultSchemaError, match="artifact_locator_value_noncanonical"):
        spec.serialize({**_payload(), field_name: value})


@pytest.mark.parametrize("nested_field", ["destination", "target", "key"])
def test_nested_artifact_metadata_cannot_carry_opaque_locator(nested_field: str) -> None:
    schemas = _schemas()
    for schema in schemas.values():
        properties = schema["properties"]
        assert isinstance(properties, dict)
        properties["artifact"] = {
            "type": "object",
            "properties": {nested_field: {"type": "string", "maxLength": 200}},
            "required": [nested_field],
            "additionalProperties": False,
        }
        required = schema["required"]
        assert isinstance(required, list)
        required.append("artifact")
    spec = _spec(schemas=schemas)
    with pytest.raises(ActionResultSchemaError, match="artifact_locator_value_noncanonical"):
        spec.serialize({**_payload(), "artifact": {nested_field: "s3:bucket/object"}})


def test_ordinary_url_fields_and_natural_language_slashes_are_not_false_positive_paths() -> None:
    schemas = _schemas()
    for schema in schemas.values():
        properties = dict(schema["properties"])
        properties["source_url"] = {"type": "string", "minLength": 1, "maxLength": 200}
        schema["properties"] = properties
    spec = _spec(schemas=schemas)

    assert (
        json.loads(
            spec.serialize({**_payload(), "status": "research/engineering", "source_url": "https://example.test/a/b"})
        )["source_url"]
        == "https://example.test/a/b"
    )
    assert json.loads(spec.serialize({**_payload(), "source_url": "/api/results"}))["source_url"] == "/api/results"
    for invalid_url in (
        "/Users/operator/private/result.json",
        "/workspace/private/result.json",
        "/root/.ssh/id_rsa",
        "/usr/bin/python",
        "/proc/self/environ",
        "/System/Library/Keychains",
        "/boot/grub/grub.cfg",
        "/media/user/volume/file",
        "/nix/store/abc-package/bin/tool",
        "/snap/bin/tool",
        "/Network/Servers/host",
        "/cores/core.123",
        "/lost+found/file",
        "https:///Users/operator/private/result.json",
        "https://example.test/x\x7f",
        "http://.",
        "http://_bad.test/path",
    ):
        with pytest.raises(ActionResultSchemaError, match="web_url_invalid"):
            spec.serialize({**_payload(), "source_url": invalid_url})


@pytest.mark.parametrize(
    "raw_path",
    [
        "local:/private/result.json",
        "read:/Users/a/result.json",
        "urn:/tmp/x",
        "///private/result.json",
        "////tmp/result.json",
        "\\private\\result.json",
        "\\Users\\a\\result.json",
        "read runtime/results/result.json",
        "read relative/results.json",
        "read:runtime/results/result.json",
        "read:relative/results.json",
        "read:runtime\\results.json",
        "read relative/resume.docx",
        "read relative/slides.pptx",
        "read relative/script.py",
        "read relative/query.sql",
        "read relative/config.toml",
        "read relative/cache.sqlite3",
    ],
)
def test_scheme_prefixed_absolute_local_paths_fail_closed(raw_path: str) -> None:
    with pytest.raises(ActionResultSchemaError, match="raw_local_path_forbidden"):
        _spec().serialize({**_payload(), "status": raw_path})


@pytest.mark.parametrize(
    "status",
    [
        "See https://example.test/report.pdf",
        "Endpoint http://localhost:8000/api",
        "Model openai/gpt-5.6",
        "Team team/member.name",
        "Use /help for details",
        "C++/Python",
        "See [report](https://e.test/r)",
        "Success rate 3/4",
        "Use /api/results for the public view",
        "See /docs/getting-started for help",
    ],
)
def test_display_urls_and_non_path_slash_tokens_remain_model_safe(status: str) -> None:
    assert json.loads(_spec().serialize({**_payload(), "status": status}))["status"] == status


@pytest.mark.parametrize("control", ["\u0000", "\u0001", "\u001f", "\u007f"])
def test_opaque_artifact_refs_reject_json_control_characters(control: str) -> None:
    spec = _spec(schemas=_schemas(with_artifact_ref=True), artifact_ref_schemes=("artifact",))
    with pytest.raises(ActionResultSchemaError, match="artifact_ref_invalid"):
        spec.serialize({**_payload(), "result_artifact_ref": f"artifact:ok{control}bad"})


@pytest.mark.parametrize(
    "embedded_path",
    [
        "read /Users/a/result.json",
        "read /.env",
        "read /secret.json",
        "read:/secret.json",
        "read /root/.ssh/id_rsa",
        "read /bin/sh",
        "read /usr/bin/python",
        "read /proc/self/environ",
        "read /System/Library/Keychains",
        "read /Applications/Foo.app",
        "read /Library/Keychains",
        "read /boot/grub/grub.cfg",
        "read /media/user/volume/file",
        "read /nix/store/abc-package/bin/tool",
        "read /snap/bin/tool",
        "read /Network/Servers/host",
        "read /cores/core.123",
        "read /lost+found/file",
        "read C:\\Users\\a\\result.json",
        "read file:///tmp/result.json",
    ],
)
def test_embedded_absolute_or_file_paths_are_scanned_across_the_full_string(embedded_path: str) -> None:
    with pytest.raises(ActionResultSchemaError, match="raw_local_path_forbidden"):
        _spec().serialize({**_payload(), "status": embedded_path})


@pytest.mark.parametrize(
    ("field_name", "error"),
    [
        ("artifact_path", "artifact_locator_field_noncanonical"),
        ("private_file_path", "private_path_field_forbidden"),
        ("runtime_paths", "private_path_field_forbidden"),
        ("privatePath", "private_path_field_forbidden"),
        ("runtimePath", "private_path_field_forbidden"),
        ("rawPath", "private_path_field_forbidden"),
        ("localPath", "private_path_field_forbidden"),
        ("filePath", "private_path_field_forbidden"),
        ("private-file-path", "private_path_field_forbidden"),
        ("private_path_value", "private_path_field_forbidden"),
        ("runtime_path_value", "private_path_field_forbidden"),
        ("filepathvalue", "private_path_field_forbidden"),
    ],
)
def test_private_artifact_path_fields_are_rejected_at_declaration(field_name: str, error: str) -> None:
    schemas = _schemas()
    for schema in schemas.values():
        properties = dict(schema["properties"])
        properties[field_name] = {"type": "string", "maxLength": 120}
        schema["properties"] = properties
    with pytest.raises(ActionResultSchemaError, match=error):
        _spec(schemas=schemas)


def test_byte_item_and_depth_limits_fail_closed() -> None:
    with pytest.raises(ActionResultSchemaError, match="max_serialized_bytes_invalid"):
        _spec(max_serialized_bytes=MAX_MESSAGE_CONTENT_BYTES + 1)
    with pytest.raises(ActionResultSchemaError, match="schema_depth_exceeded"):
        _spec(max_depth=2)

    byte_limited = _spec(max_serialized_bytes=100)
    payload = _payload()
    payload["status"] = "x" * 40
    with pytest.raises(ActionResultSchemaError, match="serialized_bytes_exceeded"):
        byte_limited.serialize(payload)

    item_limited = _spec(max_items=8)
    with pytest.raises(ActionResultSchemaError, match="item_limit_exceeded"):
        item_limited.serialize(_payload())


def test_deep_unknown_output_fails_with_bounded_contract_error() -> None:
    value: object = "leaf"
    for _ in range(2_000):
        value = {"unknown": value}
    with pytest.raises(ActionResultSchemaError, match="depth_exceeded"):
        _spec().serialize({**_payload(), "unknown": value})


def test_deep_schema_fails_before_tool_spec_recursive_canonicalization() -> None:
    base_schemas = _schemas()
    deep_schema: dict[str, object] = {"type": "string", "maxLength": 1}
    for _ in range(2_000):
        deep_schema = {
            "type": "object",
            "properties": {"child": deep_schema},
            "required": ["child"],
            "additionalProperties": False,
        }
    schemas = _schemas()
    properties = dict(schemas["success"]["properties"])
    properties["deep"] = deep_schema
    schemas["success"]["properties"] = properties
    with pytest.raises(ActionResultSchemaError, match="schema_(?:document_)?depth_exceeded"):
        _spec(
            schemas=schemas,
            field_provenance=_provenance(base_schemas),
            field_value_roles=_value_roles(base_schemas),
        )


@pytest.mark.parametrize("mutation", ["large_description", "deep_enum", "huge_integer"])
def test_every_schema_keyword_is_bounded_before_tool_spec(mutation: str) -> None:
    schemas = _schemas()
    success_properties = dict(schemas["success"]["properties"])
    status_schema = dict(success_properties["status"])
    if mutation == "large_description":
        status_schema["description"] = "x" * 100_000
        error = "schema_document_bytes_exceeded"
    elif mutation == "deep_enum":
        enum_value: object = "ready"
        for _ in range(2_000):
            enum_value = [enum_value]
        status_schema["enum"] = [enum_value]
        error = "schema_document_depth_exceeded"
    else:
        status_schema["maxLength"] = 10**5_000
        error = "schema_document_integer_too_large"
    success_properties["status"] = status_schema
    schemas["success"]["properties"] = success_properties

    with patch("sourcing_agent.action_result_schema.ToolSpec", side_effect=AssertionError("ToolSpec reached")) as tool:
        with pytest.raises(ActionResultSchemaError, match=error):
            _spec(schemas=schemas)
    tool.assert_not_called()


def test_oversize_payload_fails_before_schema_validator_allocation() -> None:
    spec = _spec(max_serialized_bytes=100)
    payload = {**_payload(), "status": "x" * 100_000}
    with patch.object(ToolSpec, "validate_input", autospec=True) as validator:
        with pytest.raises(ActionResultSchemaError, match="serialized_bytes_exceeded"):
            spec.serialize(payload)
    validator.assert_not_called()


def test_huge_integer_payload_fails_with_bounded_contract_error_before_tool_spec() -> None:
    payload = _payload()
    payload["items"] = [{"id": "person_1", "score": 10**5_000}]
    with patch.object(ToolSpec, "validate_input", autospec=True) as validator:
        with pytest.raises(ActionResultSchemaError, match="integer_too_large"):
            _spec().serialize(payload)
    validator.assert_not_called()


def test_array_schemas_must_have_a_deterministic_item_limit() -> None:
    schemas = _schemas()
    for schema in schemas.values():
        items_schema = dict(dict(schema["properties"])["items"])
        items_schema.pop("maxItems")
        properties = dict(schema["properties"])
        properties["items"] = items_schema
        schema["properties"] = properties
    with pytest.raises(ActionResultSchemaError, match="schema_array_bound_invalid"):
        _spec(schemas=schemas)


@pytest.mark.parametrize("mutation", ["missing", "extra", "unknown_class", "variant_not_server"])
def test_field_provenance_requires_exact_variant_schema_path_coverage(mutation: str) -> None:
    schemas = _schemas()
    provenance = _provenance(schemas)
    if mutation == "missing":
        provenance["success"].pop("/items/*/score")
        error = "field_provenance_path_mismatch"
    elif mutation == "extra":
        provenance["success"]["/private"] = "owner_state"
        error = "field_provenance_path_mismatch"
    elif mutation == "unknown_class":
        provenance["success"]["/status"] = "caller_claimed"
        error = "field_provenance_class_invalid"
    else:
        provenance["success"]["/variant"] = "user_supplied"
        error = "field_provenance_variant_not_server_derived"
    with pytest.raises(ActionResultSchemaError, match=error):
        _spec(schemas=schemas, field_provenance=provenance)


def test_field_provenance_requires_all_variants_and_known_classes() -> None:
    assert set(ACTION_RESULT_PROVENANCE_CLASSES) == {
        "server_derived",
        "owner_state",
        "user_supplied",
        "provider_observed",
        "model_inferred",
    }
    schemas = _schemas()
    provenance = _provenance(schemas)
    provenance.pop("deferred")
    with pytest.raises(ActionResultSchemaError, match="field_provenance_variants_incomplete"):
        _spec(schemas=schemas, field_provenance=provenance)


@pytest.mark.parametrize("mutation", ["missing", "extra", "unknown", "artifact_wrong_role"])
def test_field_value_roles_are_exact_closed_and_artifact_role_owned(mutation: str) -> None:
    schemas = _schemas(with_artifact_ref=mutation == "artifact_wrong_role")
    roles = _value_roles(schemas)
    if mutation == "missing":
        roles["success"].pop("/status")
        error = "field_value_roles_path_mismatch"
    elif mutation == "extra":
        roles["success"]["/private"] = "identifier"
        error = "field_value_roles_path_mismatch"
    elif mutation == "unknown":
        roles["success"]["/status"] = "filesystem_path"
        error = "field_value_role_invalid"
    else:
        roles["success"]["/result_artifact_ref"] = "display_text"
        error = "artifact_ref_role_required"
    with pytest.raises(ActionResultSchemaError, match=error):
        _spec(
            schemas=schemas,
            field_value_roles=roles,
            artifact_ref_schemes=("artifact",) if mutation == "artifact_wrong_role" else (),
        )


def _payload_with_exact_utf8_bytes(target_bytes: int) -> dict[str, object]:
    payload: dict[str, object] = {"variant": "success", "status": "", "items": []}
    empty_size = len(json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8"))
    content_bytes = target_bytes - empty_size
    multibyte_count, ascii_count = divmod(content_bytes, len("界".encode("utf-8")))
    payload["status"] = ("界" * multibyte_count) + ("x" * ascii_count)
    assert (
        len(json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8"))
        == target_bytes
    )
    return payload


def test_tool_result_utf8_boundary_is_exactly_65536_bytes() -> None:
    schemas = _schemas()
    for schema in schemas.values():
        properties = dict(schema["properties"])
        status_schema = dict(properties["status"])
        status_schema["maxLength"] = MAX_MESSAGE_CONTENT_BYTES
        properties["status"] = status_schema
        schema["properties"] = properties
    spec = _spec(schemas=schemas, max_serialized_bytes=MAX_MESSAGE_CONTENT_BYTES)

    at_limit = spec.serialize(_payload_with_exact_utf8_bytes(MAX_MESSAGE_CONTENT_BYTES))
    assert len(at_limit.encode("utf-8")) == 65_536
    assert ToolResultMessage(tool_call_id="call_boundary", content=at_limit).content == at_limit
    with pytest.raises(ActionResultSchemaError, match="serialized_bytes_exceeded"):
        spec.serialize(_payload_with_exact_utf8_bytes(MAX_MESSAGE_CONTENT_BYTES + 1))


def test_unicode_surrogates_are_normalized_to_action_result_error() -> None:
    with pytest.raises(ActionResultSchemaError, match="unicode_surrogate_forbidden"):
        _spec().serialize({**_payload(), "status": "\ud800"})

    schemas = _schemas()
    success_properties = dict(schemas["success"]["properties"])
    status_schema = dict(success_properties["status"])
    status_schema["description"] = "\udfff"
    success_properties["status"] = status_schema
    schemas["success"]["properties"] = success_properties
    with pytest.raises(ActionResultSchemaError, match="schema_document_surrogate|variant_schema_invalid"):
        _spec(schemas=schemas)


def test_original_schema_provenance_and_registry_inputs_cannot_mutate_contract() -> None:
    schemas = _schemas()
    provenance = _provenance(schemas)
    spec = _spec(schemas=schemas, field_provenance=provenance)
    original_digest = spec.result_schema_digest
    source_registry: list[ActionResultSpec] = [spec]
    registry = ActionResultRegistry(source_registry)

    success_properties = dict(schemas["success"]["properties"])
    success_properties["leaked"] = {"type": "string"}
    schemas["success"]["properties"] = success_properties
    provenance["success"]["/status"] = "model_inferred"
    source_registry.clear()

    assert spec.result_schema_digest == original_digest
    assert "leaked" not in dict(spec.variant_schemas["success"]["properties"])
    assert spec.field_provenance["success"]["/status"] == "owner_state"
    assert registry.tool_names == ("search_projection",)
    assert json.loads(spec.serialize(_payload()))["variant"] == "success"


@pytest.mark.parametrize(
    ("overrides", "error"),
    [
        ({"tool_name": ""}, "tool_name_invalid"),
        ({"tool_name": "_private"}, "tool_name_invalid"),
        ({"tool_name": "search.projection"}, "tool_name_invalid"),
        ({"tool_name": "search:projection"}, "tool_name_invalid"),
        ({"tool_name": "x" * 129}, "tool_name_invalid"),
        ({"tool_kind": "other"}, "tool_kind_invalid"),
        ({"tool_kind": "query"}, "query_owner_binding_required"),
        ({"result_schema_version": ""}, "schema_version_invalid"),
        ({"serializer_owner": ""}, "serializer_owner_invalid"),
        ({"serializer_revision": "serializer"}, "serializer_revision_invalid"),
        ({"serializer_contract": {}}, "serializer_contract_invalid"),
        ({"validator_owner": "other.validator"}, "validator_owner_not_canonical"),
        ({"max_items": 0}, "max_items_invalid"),
        ({"max_depth": 0}, "max_depth_invalid"),
        ({"artifact_ref_schemes": ["artifact"]}, "artifact_ref_schemes_invalid"),
        ({"artifact_ref_schemes": ("file",)}, "artifact_ref_scheme_not_opaque"),
    ],
)
def test_any_missing_or_invalid_required_contract_condition_fails_closed(
    overrides: dict[str, object],
    error: str,
) -> None:
    values: dict[str, object] = {
        "tool_name": "search_projection",
        "tool_kind": "action",
        "owner_binding": ActionResultActionOwner(action_type="search_projection"),
        "result_schema_version": "search_projection_result_v1",
        "serializer_owner": "projection_search_service.result_serializer_v1",
        "serializer_revision": "projection_search_result_serializer_v1",
        "serializer_contract": _SERIALIZER_CONTRACT,
        "validator_owner": ACTION_RESULT_VALIDATOR_OWNER,
        "variant_schemas": (schemas := _schemas()),
        "field_provenance": _provenance(schemas),
        "field_value_roles": _value_roles(schemas),
        "max_serialized_bytes": 4096,
        "max_items": 64,
        "max_depth": 6,
        "artifact_ref_schemes": (),
    }
    values.update(overrides)
    with pytest.raises(ActionResultSchemaError, match=error):
        ActionResultSpec(**values)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "missing_field",
    [
        "tool_name",
        "tool_kind",
        "owner_binding",
        "result_schema_version",
        "serializer_owner",
        "serializer_revision",
        "serializer_contract",
        "validator_owner",
        "variant_schemas",
        "field_provenance",
        "field_value_roles",
        "max_serialized_bytes",
        "max_items",
        "max_depth",
        "artifact_ref_schemes",
    ],
)
def test_each_contract_declaration_is_constructor_required(missing_field: str) -> None:
    values: dict[str, object] = {
        "tool_name": "search_projection",
        "tool_kind": "action",
        "owner_binding": ActionResultActionOwner(action_type="search_projection"),
        "result_schema_version": "search_projection_result_v1",
        "serializer_owner": "projection_search_service.result_serializer_v1",
        "serializer_revision": "projection_search_result_serializer_v1",
        "serializer_contract": _SERIALIZER_CONTRACT,
        "validator_owner": ACTION_RESULT_VALIDATOR_OWNER,
        "variant_schemas": (schemas := _schemas()),
        "field_provenance": _provenance(schemas),
        "field_value_roles": _value_roles(schemas),
        "max_serialized_bytes": 4096,
        "max_items": 64,
        "max_depth": 6,
        "artifact_ref_schemes": (),
    }
    values.pop(missing_field)
    with pytest.raises(TypeError):
        ActionResultSpec(**values)  # type: ignore[arg-type]


def test_all_three_variant_declarations_are_required_and_exact() -> None:
    for missing in ACTION_RESULT_VARIANTS:
        schemas = _schemas()
        schemas.pop(missing)
        with pytest.raises(ActionResultSchemaError, match="variants_incomplete"):
            _spec(schemas=schemas)

    schemas = _schemas()
    schemas["stale"] = _variant_schema("stale")
    with pytest.raises(ActionResultSchemaError, match="variants_incomplete"):
        _spec(schemas=schemas)


def test_registry_is_tool_keyed_canonical_and_resolves_exact_history() -> None:
    spec = _spec()
    registry = ActionResultRegistry((spec,))

    assert registry.tool_names == ("search_projection",)
    assert registry.require_historical(spec.tool_name, spec.result_schema_version, spec.result_schema_digest) is spec
    assert registry.to_manifest_record()["tools"] == [spec.to_manifest_record()]
    assert len(registry.registry_digest) == 64
    assert ActionResultRegistry((_spec(),)).registry_digest == registry.registry_digest
    with pytest.raises(ActionResultSchemaError, match="registry_entry_invalid"):
        ActionResultRegistry({"search_projection": spec})


def test_result_identity_is_tool_neutral_for_action_and_query_tools() -> None:
    action = _spec(action_type="filter_projection")
    query = _spec(action_type="inspect_operation", tool_kind="query")
    registry = ActionResultRegistry((query, action))

    assert action.tool_kind == "action"
    assert action.action_type == "filter_projection"
    assert action.query_owner_id is None
    assert query.tool_kind == "query"
    assert query.action_type is None
    assert query.query_owner_id == "operation_query_owner"
    assert registry.tool_names == ("filter_projection", "inspect_operation")
    assert (
        registry.require_historical(query.tool_name, query.result_schema_version, query.result_schema_digest) is query
    )


def test_registry_retains_versions_and_rejects_duplicate_or_drifting_history() -> None:
    search = _spec()
    filter_spec = _spec(action_type="filter_projection")
    search_v2 = replace(
        search,
        result_schema_version="search_projection_result_v2",
        serializer_revision="projection_search_result_serializer_v2",
        serializer_contract={"schema_version": "projection_search_result_serializer_contract_v2"},
    )
    registry = ActionResultRegistry.from_specs((filter_spec, search_v2, search))

    assert registry.tool_names == ("filter_projection", "search_projection")
    assert registry.specs_for_tool("search_projection") == (search, search_v2)
    with pytest.raises(ActionResultSchemaError, match="registry_duplicate_spec:search_projection"):
        ActionResultRegistry.from_specs(spec for spec in (search, search))
    digest_drift = replace(search, serializer_contract={"schema_version": "same_version_drift_v1"})
    with pytest.raises(ActionResultSchemaError, match="registry_version_digest_drift:search_projection"):
        ActionResultRegistry((search, digest_drift))


def test_registry_rejects_action_and_query_owner_drift_for_one_tool_name() -> None:
    action = _spec(action_type="search_projection")
    action_drift = replace(
        action,
        owner_binding=ActionResultActionOwner(action_type="filter_projection"),
        result_schema_version="search_projection_result_v2",
        serializer_contract=dict(action.serializer_contract),
    )
    with pytest.raises(ActionResultSchemaError, match="registry_owner_binding_drift:search_projection"):
        ActionResultRegistry((action, action_drift))

    query = _spec(action_type="inspect_operation", tool_kind="query")
    query_drift = replace(
        query,
        owner_binding=ActionResultQueryOwner(
            owner_id="operation_query_owner",
            owner_revision="operation_query_owner_v2",
            owner_contract_digest="2" * 64,
        ),
        result_schema_version="inspect_operation_result_v2",
        serializer_contract=dict(query.serializer_contract),
    )
    with pytest.raises(ActionResultSchemaError, match="registry_owner_binding_drift:inspect_operation"):
        ActionResultRegistry((query, query_drift))


def test_registry_rejects_two_tool_names_for_one_owner_route() -> None:
    action = _spec(action_type="filter_projection")
    action_alias = replace(
        action,
        tool_name="filter_people",
        serializer_contract=dict(action.serializer_contract),
    )
    with pytest.raises(ActionResultSchemaError, match="registry_owner_route_collision:filter_projection"):
        ActionResultRegistry((action, action_alias))

    query = _spec(action_type="inspect_operation", tool_kind="query")
    query_alias = replace(
        query,
        tool_name="inspect_job",
        serializer_contract=dict(query.serializer_contract),
    )
    with pytest.raises(ActionResultSchemaError, match="registry_owner_route_collision:operation_query_owner"):
        ActionResultRegistry((query, query_alias))
