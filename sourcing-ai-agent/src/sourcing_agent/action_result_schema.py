"""Revisioned model-safe result contracts for Agent-callable actions.

This module owns the deterministic boundary between an action-owner serializer
and ``ToolResultMessage``.  It deliberately does not read command results,
discover serializers, or mark any action as served.  A caller must first invoke
the declared owner serializer and then pass the resulting mapping through an
``ActionResultSpec``.

Result schemas reuse :class:`model_tool_runtime.ToolSpec` as their sole JSON
schema validator.  Each terminal variant has its own closed schema because the
bounded ToolSpec dialect intentionally has no union/``oneOf`` support.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any, Literal, TypeAlias, cast

from .model_tool_runtime import (
    MAX_MESSAGE_CONTENT_BYTES,
    ModelToolRuntimeError,
    ModelToolSchemaError,
    ToolSpec,
)

ACTION_RESULT_REGISTRY_SCHEMA_VERSION = "action_result_registry_v1"
ACTION_RESULT_VALIDATOR_OWNER = "sourcing_agent.model_tool_runtime.ToolSpec.validate_input"
ACTION_RESULT_VARIANTS = ("success", "deferred", "error")
ACTION_RESULT_PROVENANCE_CLASSES = (
    "server_derived",
    "owner_state",
    "user_supplied",
    "provider_observed",
    "model_inferred",
)

ActionResultVariant: TypeAlias = Literal["success", "deferred", "error"]

_VERSION_PATTERN = re.compile(r"[a-z][a-z0-9_]*_v[1-9][0-9]*")
_OWNER_PATTERN = re.compile(r"[A-Za-z][A-Za-z0-9_.:-]*")
_SHA256_PATTERN = re.compile(r"[0-9a-f]{64}")
_OPAQUE_SCHEME_PATTERN = re.compile(r"[a-z][a-z0-9+.-]*")
_NESTED_SCHEME_PATTERN = re.compile(r"[A-Za-z][A-Za-z0-9+.-]*:")
_FORBIDDEN_REFERENCE_SCHEMES = frozenset({"data", "file", "ftp", "http", "https", "javascript", "sftp", "ssh"})
_FULL_LOCAL_PATH_PATTERN = re.compile(
    r"(?:"
    r"~[/\\]|"
    r"\.\.?[/\\]|"
    r"[A-Za-z]:[/\\]|"
    r"\\\\|"
    r"(?:\.cache|cache|etc|home|logs?|opt|private|runtime|tmp|Users|var)[/\\]"
    r")\S*",
    re.IGNORECASE,
)
_FILE_URI_PATTERN = re.compile(r"(?<![A-Za-z0-9+.-])file:(?://)?\S+", re.IGNORECASE)
_WINDOWS_ABSOLUTE_PATH_PATTERN = re.compile(r"(?<![A-Za-z0-9])(?:[A-Za-z]:[/\\]|\\\\[^/\\\s]+[/\\])\S*")
_POSIX_ABSOLUTE_PATH_PATTERN = re.compile(r"(?<![A-Za-z0-9/])/(?!/)[^\s]+")
_LOCAL_POSIX_ROOT_PATTERN = re.compile(
    r"(?<![A-Za-z0-9/])/(?:Users|Volumes|private|tmp|var|home|etc|opt)(?:/|\b)",
    re.IGNORECASE,
)
_MULTISLASH_ABSOLUTE_PATH_PATTERN = re.compile(r"(?<![A-Za-z0-9/])/{2,}\S+")
_WINDOWS_ROOT_RELATIVE_PATH_PATTERN = re.compile(
    r"(?<![A-Za-z0-9\\])\\(?:Users|Volumes|private|tmp|var|home|etc|opt)(?:\\|\b)\S*",
    re.IGNORECASE,
)
_GENERIC_RELATIVE_FILE_PATH_PATTERN = re.compile(
    r"(?<![A-Za-z0-9/])(?:[^/\\\s:]+[/\\])+[^/\\\s:]+\."
    r"(?:bin|csv|db|doc|docx|htm|html|jpeg|jpg|json|jsonl|log|md|parquet|pdf|png|ppt|pptx|py|sql|sqlite|sqlite3|toml|txt|webp|xls|xlsx|xml|yaml|yml)\b",
    re.IGNORECASE,
)
_NETWORK_URL_PATTERN = re.compile(r"https?://\S+", re.IGNORECASE)
_ROOT_RELATIVE_URL_PATTERN = re.compile(r"/(?!/)\S*")
_ARTIFACT_LOCATOR_TOKENS = frozenset(
    {
        "file",
        "files",
        "filename",
        "filenames",
        "handle",
        "handles",
        "href",
        "hrefs",
        "hyperlink",
        "hyperlinks",
        "link",
        "links",
        "location",
        "locations",
        "locator",
        "locators",
        "path",
        "paths",
        "pointer",
        "pointers",
        "src",
        "srcs",
        "ref",
        "refs",
        "reference",
        "references",
        "address",
        "addresses",
        "uri",
        "uris",
        "url",
        "urls",
    }
)
_PATH_FIELD_MARKERS = frozenset(
    {
        "artifact",
        "cache",
        "filesystem",
        "internal",
        "local",
        "private",
        "raw",
        "runtime",
        "storage",
        "temp",
        "tmp",
    }
)


class ActionResultSchemaError(ValueError):
    """Raised when a result declaration or serialized payload fails closed."""


def _required_identifier(field_name: str, value: object, *, pattern: re.Pattern[str]) -> str:
    if type(value) is not str or not value or value != value.strip() or pattern.fullmatch(value) is None:
        raise ActionResultSchemaError(f"action_result_{field_name}_invalid")
    return value


def _required_sha256(field_name: str, value: object) -> str:
    if type(value) is not str or _SHA256_PATTERN.fullmatch(value) is None:
        raise ActionResultSchemaError(f"action_result_{field_name}_invalid")
    return value


def _canonical_json(value: object) -> str:
    try:
        encoded = json.dumps(
            _thaw_json(value),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
        encoded.encode("utf-8")
        return encoded
    except (TypeError, ValueError, OverflowError, UnicodeError) as exc:
        raise ActionResultSchemaError("action_result_not_strict_json") from exc


def _thaw_json(value: object) -> object:
    if isinstance(value, Mapping):
        if any(type(key) is not str for key in value):
            raise ActionResultSchemaError("action_result_object_key_invalid")
        return {cast(str, key): _thaw_json(child) for key, child in value.items()}
    if isinstance(value, (list, tuple)):
        return [_thaw_json(child) for child in value]
    return value


def _sha256_json(value: object) -> str:
    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def _validate_strict_json_value(
    value: object,
    *,
    path: str = "$",
    depth: int = 1,
    max_depth: int | None = None,
) -> None:
    """Reject Python conveniences that are not values in the JSON data model."""

    if max_depth is not None and depth > max_depth:
        raise ActionResultSchemaError(f"action_result_depth_exceeded:{path}")
    if value is None or type(value) in {bool, int}:
        return
    if type(value) is str:
        if any(0xD800 <= ord(character) <= 0xDFFF for character in value):
            raise ActionResultSchemaError(f"action_result_unicode_surrogate_forbidden:{path}")
        return
    if type(value) is float:
        if not math.isfinite(value):
            raise ActionResultSchemaError(f"action_result_nonfinite_number:{path}")
        return
    if type(value) is list:
        for index, child in enumerate(value):
            _validate_strict_json_value(
                child,
                path=f"{path}[{index}]",
                depth=depth + 1,
                max_depth=max_depth,
            )
        return
    if type(value) is dict:
        for field_name, child in value.items():
            if type(field_name) is not str:
                raise ActionResultSchemaError(f"action_result_object_key_invalid:{path}")
            if any(0xD800 <= ord(character) <= 0xDFFF for character in field_name):
                raise ActionResultSchemaError(f"action_result_unicode_surrogate_forbidden:{path}")
            _validate_strict_json_value(
                child,
                path=f"{path}.{field_name}",
                depth=depth + 1,
                max_depth=max_depth,
            )
        return
    raise ActionResultSchemaError(f"action_result_not_strict_json:{path}")


def _is_artifact_ref_field(field_name: str) -> bool:
    return field_name == "artifact_ref" or field_name.endswith("_artifact_ref")


def _is_artifact_refs_field(field_name: str) -> bool:
    return field_name == "artifact_refs" or field_name.endswith("_artifact_refs")


def _field_name_tokens(field_name: str) -> frozenset[str]:
    with_camel_boundaries = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", "_", field_name)
    return frozenset(part.casefold() for part in re.findall(r"[A-Za-z0-9]+", with_camel_boundaries))


def _is_noncanonical_artifact_locator_field(field_name: str) -> bool:
    tokens = _field_name_tokens(field_name)
    compact_name = "".join(re.findall(r"[A-Za-z0-9]+", field_name)).casefold()
    compact_alias = any(
        compact_name.endswith(f"{artifact_token}{locator_token}")
        for artifact_token in ("artifact", "artifacts")
        for locator_token in _ARTIFACT_LOCATOR_TOKENS
    )
    return (
        (bool(tokens & {"artifact", "artifacts"}) and bool(tokens & _ARTIFACT_LOCATOR_TOKENS)) or compact_alias
    ) and not (_is_artifact_ref_field(field_name) or _is_artifact_refs_field(field_name))


def _is_ordinary_url_field(field_name: str | None) -> bool:
    if field_name is None:
        return False
    tokens = _field_name_tokens(field_name)
    return not (tokens & {"artifact", "artifacts"}) and bool(tokens & {"url", "urls", "uri", "uris"})


def _is_private_path_field(field_name: str) -> bool:
    tokens = _field_name_tokens(field_name)
    path_tokens = tokens & {"path", "paths"}
    if path_tokens and (len(tokens) == 1 or bool(tokens & ({"file", "files"} | _PATH_FIELD_MARKERS))):
        return True
    compact_name = "".join(re.findall(r"[A-Za-z0-9]+", field_name)).casefold()
    path_suffixes = ("path", "paths", "pathvalue", "pathsvalue", "pathvalues", "pathsvalues")
    return compact_name in {
        "path",
        "paths",
        "filepath",
        "filepaths",
        "filepathvalue",
        "filepathsvalue",
        "filepathvalues",
        "filepathsvalues",
    } or any(compact_name.startswith(marker) and compact_name.endswith(path_suffixes) for marker in _PATH_FIELD_MARKERS)


def _artifact_ref_shape(schema: Mapping[str, Any], *, plural: bool) -> bool:
    if plural:
        items = schema.get("items")
        return (
            schema.get("type") == "array"
            and isinstance(items, Mapping)
            and items.get("type") == "string"
            and isinstance(schema.get("maxItems"), int)
        )
    return schema.get("type") == "string"


def _json_pointer_token(value: str) -> str:
    return value.replace("~", "~0").replace("/", "~1")


def _schema_field_paths(schema: Mapping[str, Any], *, prefix: str = "") -> frozenset[str]:
    paths: set[str] = set()
    schema_type = schema.get("type")
    if schema_type == "object":
        properties = schema.get("properties")
        if isinstance(properties, Mapping):
            for field_name, child in properties.items():
                if type(field_name) is not str or not isinstance(child, Mapping):
                    continue
                field_path = f"{prefix}/{_json_pointer_token(field_name)}"
                paths.add(field_path)
                paths.update(_schema_field_paths(child, prefix=field_path))
    elif schema_type == "array":
        items = schema.get("items")
        if isinstance(items, Mapping):
            paths.update(_schema_field_paths(items, prefix=f"{prefix}/*"))
    return frozenset(paths)


def _contains_raw_local_path(value: str, *, field_name: str | None) -> bool:
    normalized = value.strip()
    ordinary_url_field = _is_ordinary_url_field(field_name)
    if ordinary_url_field and _NETWORK_URL_PATTERN.fullmatch(normalized) is not None:
        return False
    scan_value = _NETWORK_URL_PATTERN.sub("", value)
    scan_normalized = scan_value.strip()
    if (
        _FILE_URI_PATTERN.search(scan_value) is not None
        or _WINDOWS_ABSOLUTE_PATH_PATTERN.search(scan_value) is not None
        or _WINDOWS_ROOT_RELATIVE_PATH_PATTERN.search(scan_value) is not None
        or _MULTISLASH_ABSOLUTE_PATH_PATTERN.search(scan_value) is not None
    ):
        return True
    if _LOCAL_POSIX_ROOT_PATTERN.search(scan_value) is not None:
        return True
    if _FULL_LOCAL_PATH_PATTERN.fullmatch(scan_normalized) is not None:
        return True
    if ordinary_url_field and _ROOT_RELATIVE_URL_PATTERN.fullmatch(normalized) is not None:
        return False
    if (
        not ordinary_url_field
        and ("/" in scan_normalized or "\\" in scan_normalized)
        and _GENERIC_RELATIVE_FILE_PATH_PATTERN.search(scan_normalized) is not None
    ):
        return True
    return _POSIX_ABSOLUTE_PATH_PATTERN.search(scan_value) is not None


def _validate_schema_policy(
    schema: Mapping[str, Any],
    *,
    max_items: int,
    max_depth: int,
    artifact_ref_schemes: tuple[str, ...],
    depth: int = 1,
    path: str = "$",
) -> int:
    if depth > max_depth:
        raise ActionResultSchemaError(f"action_result_schema_depth_exceeded:{path}")
    schema_type = schema.get("type")
    item_count = 0
    if schema_type == "object":
        if schema.get("additionalProperties") is not False:
            raise ActionResultSchemaError(f"action_result_schema_object_not_closed:{path}")
        properties = schema.get("properties")
        if not isinstance(properties, Mapping):
            raise ActionResultSchemaError(f"action_result_schema_properties_invalid:{path}")
        item_count += len(properties)
        for field_name, child in properties.items():
            if type(field_name) is not str or not isinstance(child, Mapping):
                raise ActionResultSchemaError(f"action_result_schema_property_invalid:{path}")
            if field_name == "*":
                raise ActionResultSchemaError(f"action_result_schema_field_reserved:{path}.{field_name}")
            if _is_noncanonical_artifact_locator_field(field_name):
                raise ActionResultSchemaError(f"action_result_artifact_locator_field_noncanonical:{path}.{field_name}")
            if _is_private_path_field(field_name):
                raise ActionResultSchemaError(f"action_result_private_path_field_forbidden:{path}.{field_name}")
            is_ref = _is_artifact_ref_field(field_name)
            is_refs = _is_artifact_refs_field(field_name)
            if (is_ref or is_refs) and not artifact_ref_schemes:
                raise ActionResultSchemaError("action_result_artifact_ref_policy_required")
            if (is_ref or is_refs) and not _artifact_ref_shape(child, plural=is_refs):
                raise ActionResultSchemaError(f"action_result_artifact_ref_schema_invalid:{path}.{field_name}")
            item_count += _validate_schema_policy(
                child,
                max_items=max_items,
                max_depth=max_depth,
                artifact_ref_schemes=artifact_ref_schemes,
                depth=depth + 1,
                path=f"{path}.{field_name}",
            )
    elif schema_type == "array":
        maximum = schema.get("maxItems")
        if type(maximum) is not int or maximum < 0 or maximum > max_items:
            raise ActionResultSchemaError(f"action_result_schema_array_bound_invalid:{path}")
        items = schema.get("items")
        if not isinstance(items, Mapping):
            raise ActionResultSchemaError(f"action_result_schema_array_items_invalid:{path}")
        item_count += _validate_schema_policy(
            items,
            max_items=max_items,
            max_depth=max_depth,
            artifact_ref_schemes=artifact_ref_schemes,
            depth=depth + 1,
            path=f"{path}[]",
        )
    if item_count > max_items:
        raise ActionResultSchemaError(f"action_result_schema_item_limit_exceeded:{path}")
    return item_count


def _validate_opaque_artifact_ref(value: object, *, allowed_schemes: tuple[str, ...], path: str) -> None:
    if (
        type(value) is not str
        or not value
        or value != value.strip()
        or any(char.isspace() or ord(char) < 0x20 or ord(char) == 0x7F for char in value)
    ):
        raise ActionResultSchemaError(f"action_result_artifact_ref_invalid:{path}")
    scheme, separator, opaque = value.partition(":")
    if (
        not separator
        or not opaque
        or opaque.startswith("//")
        or opaque.startswith(("/", "\\", "~", "."))
        or "\\" in opaque
        or ".." in opaque.split("/")
        or _NESTED_SCHEME_PATTERN.search(opaque) is not None
        or _OPAQUE_SCHEME_PATTERN.fullmatch(scheme) is None
        or scheme not in allowed_schemes
    ):
        raise ActionResultSchemaError(f"action_result_artifact_ref_not_allowed:{path}")


def _validate_payload_policy(
    value: object,
    *,
    max_items: int,
    max_depth: int,
    artifact_ref_schemes: tuple[str, ...],
    depth: int = 1,
    path: str = "$",
    field_name: str | None = None,
    item_counter: list[int] | None = None,
) -> None:
    if item_counter is None:
        item_counter = [0]
    if depth > max_depth:
        raise ActionResultSchemaError(f"action_result_depth_exceeded:{path}")
    if isinstance(value, Mapping):
        item_counter[0] += len(value)
        if item_counter[0] > max_items:
            raise ActionResultSchemaError("action_result_item_limit_exceeded")
        for field_name, child in value.items():
            if type(field_name) is not str:
                raise ActionResultSchemaError(f"action_result_object_key_invalid:{path}")
            if _is_noncanonical_artifact_locator_field(field_name):
                raise ActionResultSchemaError(f"action_result_artifact_locator_field_noncanonical:{path}.{field_name}")
            if _is_private_path_field(field_name):
                raise ActionResultSchemaError(f"action_result_private_path_field_forbidden:{path}.{field_name}")
            if _is_artifact_ref_field(field_name):
                _validate_opaque_artifact_ref(
                    child,
                    allowed_schemes=artifact_ref_schemes,
                    path=f"{path}.{field_name}",
                )
            elif _is_artifact_refs_field(field_name):
                if not isinstance(child, list):
                    raise ActionResultSchemaError(f"action_result_artifact_ref_invalid:{path}.{field_name}")
                for index, item in enumerate(child):
                    _validate_opaque_artifact_ref(
                        item,
                        allowed_schemes=artifact_ref_schemes,
                        path=f"{path}.{field_name}[{index}]",
                    )
            _validate_payload_policy(
                child,
                max_items=max_items,
                max_depth=max_depth,
                artifact_ref_schemes=artifact_ref_schemes,
                depth=depth + 1,
                path=f"{path}.{field_name}",
                field_name=field_name,
                item_counter=item_counter,
            )
    elif isinstance(value, list):
        item_counter[0] += len(value)
        if item_counter[0] > max_items:
            raise ActionResultSchemaError("action_result_item_limit_exceeded")
        for index, child in enumerate(value):
            _validate_payload_policy(
                child,
                max_items=max_items,
                max_depth=max_depth,
                artifact_ref_schemes=artifact_ref_schemes,
                depth=depth + 1,
                path=f"{path}[{index}]",
                field_name=field_name,
                item_counter=item_counter,
            )
    elif isinstance(value, str) and _contains_raw_local_path(value, field_name=field_name):
        raise ActionResultSchemaError(f"action_result_raw_local_path_forbidden:{path}")


@dataclass(frozen=True, slots=True)
class ActionResultSpec:
    """Immutable result schema, owner, and serialization policy for one action."""

    action_type: str
    result_schema_version: str
    serializer_owner: str
    serializer_revision: str
    serializer_contract_digest: str
    validator_owner: str
    variant_schemas: Mapping[str, Mapping[str, Any]]
    field_provenance: Mapping[str, Mapping[str, str]]
    max_serialized_bytes: int
    max_items: int
    max_depth: int
    artifact_ref_schemes: tuple[str, ...]
    _variant_tools: Mapping[str, ToolSpec] = field(init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        action_type = _required_identifier("action_type", self.action_type, pattern=_OWNER_PATTERN)
        version = _required_identifier(
            "schema_version",
            self.result_schema_version,
            pattern=_VERSION_PATTERN,
        )
        serializer_owner = _required_identifier(
            "serializer_owner",
            self.serializer_owner,
            pattern=_OWNER_PATTERN,
        )
        serializer_revision = _required_identifier(
            "serializer_revision",
            self.serializer_revision,
            pattern=_VERSION_PATTERN,
        )
        serializer_contract_digest = _required_sha256(
            "serializer_contract_digest",
            self.serializer_contract_digest,
        )
        validator_owner = _required_identifier(
            "validator_owner",
            self.validator_owner,
            pattern=_OWNER_PATTERN,
        )
        if validator_owner != ACTION_RESULT_VALIDATOR_OWNER:
            raise ActionResultSchemaError("action_result_validator_owner_not_canonical")
        if (
            type(self.max_serialized_bytes) is not int
            or self.max_serialized_bytes < 2
            or self.max_serialized_bytes > MAX_MESSAGE_CONTENT_BYTES
        ):
            raise ActionResultSchemaError("action_result_max_serialized_bytes_invalid")
        if type(self.max_items) is not int or not 1 <= self.max_items <= 100_000:
            raise ActionResultSchemaError("action_result_max_items_invalid")
        if type(self.max_depth) is not int or not 1 <= self.max_depth <= 64:
            raise ActionResultSchemaError("action_result_max_depth_invalid")
        if type(self.artifact_ref_schemes) is not tuple:
            raise ActionResultSchemaError("action_result_artifact_ref_schemes_invalid")
        normalized_schemes: list[str] = []
        for scheme in self.artifact_ref_schemes:
            normalized = _required_identifier("artifact_ref_scheme", scheme, pattern=_OPAQUE_SCHEME_PATTERN)
            if normalized in _FORBIDDEN_REFERENCE_SCHEMES:
                raise ActionResultSchemaError("action_result_artifact_ref_scheme_not_opaque")
            normalized_schemes.append(normalized)
        if len(normalized_schemes) != len(set(normalized_schemes)):
            raise ActionResultSchemaError("action_result_artifact_ref_schemes_duplicate")
        canonical_schemes = tuple(sorted(normalized_schemes))

        if not isinstance(self.variant_schemas, Mapping) or set(self.variant_schemas) != set(ACTION_RESULT_VARIANTS):
            raise ActionResultSchemaError("action_result_variants_incomplete")
        frozen_schemas: dict[str, Mapping[str, Any]] = {}
        variant_tools: dict[str, ToolSpec] = {}
        for variant in ACTION_RESULT_VARIANTS:
            schema = self.variant_schemas.get(variant)
            if not isinstance(schema, Mapping):
                raise ActionResultSchemaError(f"action_result_variant_schema_invalid:{variant}")
            try:
                tool = ToolSpec(
                    name=f"{action_type}:{variant}:result",
                    description=f"Model-safe {variant} result for {action_type}.",
                    input_schema=schema,
                    schema_version=version,
                    approval_policy="result_validation_only",
                    budget_required=False,
                )
            except (ModelToolRuntimeError, ModelToolSchemaError, UnicodeError) as exc:
                raise ActionResultSchemaError(f"action_result_variant_schema_invalid:{variant}:{exc}") from exc
            copied_schema = tool.input_schema
            root_properties = copied_schema.get("properties")
            required = copied_schema.get("required")
            if not isinstance(root_properties, Mapping) or "variant" not in root_properties:
                raise ActionResultSchemaError(f"action_result_variant_field_required:{variant}")
            variant_schema = root_properties["variant"]
            if not isinstance(variant_schema, Mapping) or variant_schema.get("const") != variant:
                raise ActionResultSchemaError(f"action_result_variant_const_mismatch:{variant}")
            if not isinstance(required, tuple) or "variant" not in required:
                raise ActionResultSchemaError(f"action_result_variant_field_required:{variant}")
            _validate_schema_policy(
                copied_schema,
                max_items=self.max_items,
                max_depth=self.max_depth,
                artifact_ref_schemes=canonical_schemes,
            )
            frozen_schemas[variant] = copied_schema
            variant_tools[variant] = tool

        if not isinstance(self.field_provenance, Mapping) or set(self.field_provenance) != set(ACTION_RESULT_VARIANTS):
            raise ActionResultSchemaError("action_result_field_provenance_variants_incomplete")
        frozen_provenance: dict[str, Mapping[str, str]] = {}
        for variant in ACTION_RESULT_VARIANTS:
            provenance = self.field_provenance.get(variant)
            if not isinstance(provenance, Mapping) or any(type(path) is not str for path in provenance):
                raise ActionResultSchemaError(f"action_result_field_provenance_invalid:{variant}")
            expected_paths = _schema_field_paths(frozen_schemas[variant])
            actual_paths = set(provenance)
            missing_paths = sorted(expected_paths - actual_paths)
            extra_paths = sorted(actual_paths - expected_paths)
            if missing_paths or extra_paths:
                raise ActionResultSchemaError(
                    f"action_result_field_provenance_path_mismatch:{variant}:"
                    f"missing={','.join(missing_paths)}:extra={','.join(extra_paths)}"
                )
            normalized_provenance: dict[str, str] = {}
            for field_path in sorted(expected_paths):
                classification = provenance[field_path]
                if type(classification) is not str or classification not in ACTION_RESULT_PROVENANCE_CLASSES:
                    raise ActionResultSchemaError(
                        f"action_result_field_provenance_class_invalid:{variant}:{field_path}"
                    )
                if field_path == "/variant" and classification != "server_derived":
                    raise ActionResultSchemaError(
                        f"action_result_field_provenance_variant_not_server_derived:{variant}"
                    )
                normalized_provenance[field_path] = classification
            frozen_provenance[variant] = MappingProxyType(normalized_provenance)

        object.__setattr__(self, "action_type", action_type)
        object.__setattr__(self, "result_schema_version", version)
        object.__setattr__(self, "serializer_owner", serializer_owner)
        object.__setattr__(self, "serializer_revision", serializer_revision)
        object.__setattr__(self, "serializer_contract_digest", serializer_contract_digest)
        object.__setattr__(self, "validator_owner", validator_owner)
        object.__setattr__(self, "artifact_ref_schemes", canonical_schemes)
        object.__setattr__(self, "variant_schemas", MappingProxyType(frozen_schemas))
        object.__setattr__(self, "field_provenance", MappingProxyType(frozen_provenance))
        object.__setattr__(self, "_variant_tools", MappingProxyType(variant_tools))

    @property
    def allowed_variants(self) -> tuple[ActionResultVariant, ...]:
        return cast(tuple[ActionResultVariant, ...], ACTION_RESULT_VARIANTS)

    def to_fingerprint_record(self) -> dict[str, object]:
        return {
            "action_type": self.action_type,
            "result_schema_version": self.result_schema_version,
            "serializer_owner": self.serializer_owner,
            "serializer_revision": self.serializer_revision,
            "serializer_contract_digest": self.serializer_contract_digest,
            "validator_owner": self.validator_owner,
            "variant_schemas": _thaw_json(self.variant_schemas),
            "field_provenance": _thaw_json(self.field_provenance),
            "allowed_variants": list(self.allowed_variants),
            "max_serialized_bytes": self.max_serialized_bytes,
            "max_items": self.max_items,
            "max_depth": self.max_depth,
            "artifact_ref_schemes": list(self.artifact_ref_schemes),
        }

    @property
    def result_schema_digest(self) -> str:
        return _sha256_json(self.to_fingerprint_record())

    def to_manifest_record(self) -> dict[str, object]:
        return {
            **self.to_fingerprint_record(),
            "result_schema_digest": self.result_schema_digest,
        }

    def serialize(self, owner_output: dict[str, Any]) -> str:
        """Validate owner output and return canonical ToolResultMessage content."""

        if type(owner_output) is not dict:
            raise ActionResultSchemaError("action_result_owner_output_must_be_object")
        _validate_strict_json_value(owner_output, max_depth=self.max_depth)
        variant = owner_output.get("variant")
        if type(variant) is not str or variant not in ACTION_RESULT_VARIANTS:
            raise ActionResultSchemaError("action_result_variant_not_allowed")
        tool = self._variant_tools[variant]
        try:
            validated = tool.validate_input(owner_output)
        except (ModelToolRuntimeError, ModelToolSchemaError) as exc:
            raise ActionResultSchemaError(f"action_result_schema_validation_failed:{exc}") from exc
        _validate_payload_policy(
            validated,
            max_items=self.max_items,
            max_depth=self.max_depth,
            artifact_ref_schemes=self.artifact_ref_schemes,
        )
        encoded = _canonical_json(validated)
        if len(encoded.encode("utf-8")) > self.max_serialized_bytes:
            raise ActionResultSchemaError("action_result_serialized_bytes_exceeded")
        return encoded

    def serialize_bytes(self, owner_output: dict[str, Any]) -> bytes:
        return self.serialize(owner_output).encode("utf-8")


@dataclass(frozen=True, slots=True)
class ActionResultRegistry:
    """Immutable action-keyed registry; an empty registry serves no tools.

    Direct Mapping construction is retained for already-unique keyed
    definitions. Population builders must use :meth:`from_specs` so duplicate
    action ids are rejected before a Mapping can silently collapse them.
    """

    specs: Mapping[str, ActionResultSpec] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not isinstance(self.specs, Mapping):
            raise ActionResultSchemaError("action_result_registry_specs_invalid")
        copied: dict[str, ActionResultSpec] = {}
        for action_type, spec in self.specs.items():
            if type(action_type) is not str or not isinstance(spec, ActionResultSpec):
                raise ActionResultSchemaError("action_result_registry_entry_invalid")
            if action_type != spec.action_type:
                raise ActionResultSchemaError("action_result_registry_action_mismatch")
            copied[action_type] = spec
        object.__setattr__(self, "specs", MappingProxyType(dict(sorted(copied.items()))))

    @classmethod
    def from_specs(cls, specs: Iterable[ActionResultSpec]) -> ActionResultRegistry:
        try:
            materialized = tuple(specs)
        except TypeError as exc:
            raise ActionResultSchemaError("action_result_registry_specs_invalid") from exc
        by_action_type: dict[str, ActionResultSpec] = {}
        for spec in materialized:
            if not isinstance(spec, ActionResultSpec):
                raise ActionResultSchemaError("action_result_registry_entry_invalid")
            if spec.action_type in by_action_type:
                raise ActionResultSchemaError(f"action_result_registry_duplicate_action:{spec.action_type}")
            by_action_type[spec.action_type] = spec
        return cls(by_action_type)

    @property
    def action_types(self) -> tuple[str, ...]:
        return tuple(self.specs)

    def get(self, action_type: str) -> ActionResultSpec | None:
        return self.specs.get(action_type)

    def require(self, action_type: str) -> ActionResultSpec:
        spec = self.get(action_type)
        if spec is None:
            raise ActionResultSchemaError(f"action_result_spec_missing:{action_type}")
        return spec

    def to_manifest_record(self) -> dict[str, object]:
        records = [self.specs[action_type].to_manifest_record() for action_type in self.action_types]
        return {
            "schema_version": ACTION_RESULT_REGISTRY_SCHEMA_VERSION,
            "action_count": len(records),
            "actions": records,
        }

    @property
    def registry_digest(self) -> str:
        return _sha256_json(self.to_manifest_record())


DEFAULT_ACTION_RESULT_REGISTRY = ActionResultRegistry.from_specs(())


__all__ = [
    "ACTION_RESULT_REGISTRY_SCHEMA_VERSION",
    "ACTION_RESULT_PROVENANCE_CLASSES",
    "ACTION_RESULT_VALIDATOR_OWNER",
    "ACTION_RESULT_VARIANTS",
    "ActionResultRegistry",
    "ActionResultSchemaError",
    "ActionResultSpec",
    "ActionResultVariant",
    "DEFAULT_ACTION_RESULT_REGISTRY",
]
