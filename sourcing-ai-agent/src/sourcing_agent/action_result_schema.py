"""Revisioned model-safe result contracts for Agent-callable tools.

This module owns the deterministic boundary between a tool-owner serializer
and ``ToolResultMessage``.  It deliberately does not read command results,
discover serializers, or mark any action as served.  A caller must first invoke
the declared owner serializer and then pass the resulting mapping through an
``ActionResultSpec``.

Result schemas reuse the bounded JSON-schema implementation through the
internal-only :class:`model_tool_runtime.InternalToolValidatorSpec`.  They do
not mint provider-visible ``ToolSpec`` names.  Each terminal variant has its
own closed schema because the bounded dialect intentionally has no
union/``oneOf`` support.
"""

from __future__ import annotations

import hashlib
import ipaddress
import json
import math
import re
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any, Literal, TypeAlias, cast
from urllib.parse import urlsplit

from .agent_contract_identity import is_valid_agent_tool_name
from .model_tool_runtime import (
    MAX_MESSAGE_CONTENT_BYTES,
    InternalToolValidatorSpec,
    ModelToolRuntimeError,
    ModelToolSchemaError,
)

ACTION_RESULT_REGISTRY_SCHEMA_VERSION = "action_result_registry_v2"
ACTION_RESULT_VALIDATOR_OWNER = "sourcing_agent.model_tool_runtime.InternalToolValidatorSpec.validate_input"
ACTION_RESULT_INTERPRETATION_CONTRACT_VERSION = "action_result_interpretation_contract_v3"
ACTION_RESULT_CANONICALIZER_REVISION = "action_result_canonical_json_utf8_v3"
ACTION_RESULT_VALUE_POLICY_REVISION = "action_result_positive_value_roles_v3"
ACTION_RESULT_LIMIT_POLICY_REVISION = "action_result_prevalidation_limits_v3"
ACTION_RESULT_SERIALIZER_CONTRACT_VERSION = "action_result_serializer_contract_v1"
ACTION_RESULT_MAX_SCHEMA_BYTES = 32 * 1024
ACTION_RESULT_MAX_SCHEMA_DOCUMENT_DEPTH = 128
ACTION_RESULT_MAX_SCHEMA_DOCUMENT_ITEMS = 10_000
ACTION_RESULT_MAX_INTEGER_BITS = 13_600
ACTION_RESULT_VARIANTS = ("success", "deferred", "error")
ACTION_RESULT_PROVENANCE_CLASSES = (
    "server_derived",
    "owner_state",
    "user_supplied",
    "provider_observed",
    "model_inferred",
)

ActionResultVariant: TypeAlias = Literal["success", "deferred", "error"]
ActionResultToolKind: TypeAlias = Literal["action", "query"]
ActionResultValueRole: TypeAlias = Literal[
    "control",
    "identifier",
    "display_text",
    "web_url",
    "opaque_artifact_ref",
]

ACTION_RESULT_TOOL_KINDS = ("action", "query")
ACTION_RESULT_VALUE_ROLES = (
    "control",
    "identifier",
    "display_text",
    "web_url",
    "opaque_artifact_ref",
)

_VERSION_PATTERN = re.compile(r"[a-z][a-z0-9_]*_v[1-9][0-9]*")
_OWNER_PATTERN = re.compile(r"[A-Za-z][A-Za-z0-9_.:-]*")
_SHA256_PATTERN = re.compile(r"[0-9a-f]{64}")
_OPAQUE_SCHEME_PATTERN = re.compile(r"[a-z][a-z0-9+.-]*")
_NESTED_SCHEME_PATTERN = re.compile(r"[A-Za-z][A-Za-z0-9+.-]*:")
_URI_SCHEME_VALUE_PATTERN = re.compile(r"(?<![A-Za-z0-9+.-])[A-Za-z][A-Za-z0-9+.-]*:[^\s]")
_FORBIDDEN_REFERENCE_SCHEMES = frozenset({"data", "file", "ftp", "http", "https", "javascript", "sftp", "ssh"})
_FULL_LOCAL_PATH_PATTERN = re.compile(
    r"(?:"
    r"~[/\\]|"
    r"\.\.?[/\\]|"
    r"[A-Za-z]:[/\\]|"
    r"\\\\|"
    r"(?:Applications|bin|boot|\.cache|cache|cores|dev|etc|home|lib|lib64|Library|logs?|lost\+found|media|mnt|Network|nix|opt|private|proc|root|run|runtime|sbin|snap|srv|sys|System|tmp|usr|Users|var)[/\\]"
    r")\S*",
    re.IGNORECASE,
)
_FILE_URI_PATTERN = re.compile(r"(?<![A-Za-z0-9+.-])file:(?://)?\S+", re.IGNORECASE)
_WINDOWS_ABSOLUTE_PATH_PATTERN = re.compile(r"(?<![A-Za-z0-9])(?:[A-Za-z]:[/\\]|\\\\[^/\\\s]+[/\\])\S*")
_POSIX_ABSOLUTE_PATH_PATTERN = re.compile(r"(?<![A-Za-z0-9/])/(?!/)[^\s]+")
_LOCAL_POSIX_ROOT_PATTERN = re.compile(
    r"(?<![A-Za-z0-9/])/(?:Applications|bin|boot|cores|dev|etc|home|lib|lib64|Library|lost\+found|media|mnt|Network|nix|opt|private|proc|root|run|sbin|snap|srv|sys|System|tmp|usr|Users|var|Volumes)(?:/|\b)",
    re.IGNORECASE,
)
_MULTISLASH_ABSOLUTE_PATH_PATTERN = re.compile(r"(?<![A-Za-z0-9/])/{2,}\S+")
_WINDOWS_ROOT_RELATIVE_PATH_PATTERN = re.compile(
    r"(?<![A-Za-z0-9\\])\\(?:Applications|bin|boot|cores|dev|etc|home|lib|lib64|Library|lost\+found|media|mnt|Network|nix|opt|private|proc|root|run|sbin|snap|srv|sys|System|tmp|usr|Users|var|Volumes)(?:\\|\b)\S*",
    re.IGNORECASE,
)
_GENERIC_RELATIVE_FILE_PATH_PATTERN = re.compile(
    r"(?<![A-Za-z0-9/])(?:[^/\\\s:]+[/\\])+[^/\\\s:]+\."
    r"(?:avro|bin|csv|db|doc|docx|htm|html|jpeg|jpg|json|jsonl|log|md|parquet|pdf|pem|png|ppt|pptx|py|sql|sqlite|sqlite3|toml|txt|webp|xls|xlsx|xml|yaml|yml|zip)\b",
    re.IGNORECASE,
)
_SENSITIVE_RELATIVE_FILE_PATTERN = re.compile(
    r"(?<![A-Za-z0-9/])(?:[^/\\\s:]+[/\\])+(?:authorized_keys|id_(?:dsa|ecdsa|ed25519|rsa)|known_hosts)\b",
    re.IGNORECASE,
)
_NETWORK_URL_PATTERN = re.compile(r"https?://[^\s)\]}>;,]+", re.IGNORECASE)
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
        "target",
        "targets",
        "ref",
        "refs",
        "reference",
        "references",
        "address",
        "addresses",
        "download",
        "downloads",
        "destination",
        "destinations",
        "endpoint",
        "endpoints",
        "key",
        "keys",
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
_LOCAL_ROOT_NAMES = frozenset(
    {
        ".cache",
        "cache",
        "applications",
        "bin",
        "boot",
        "dev",
        "cores",
        "etc",
        "home",
        "lib",
        "lib64",
        "library",
        "log",
        "logs",
        "lost+found",
        "media",
        "mnt",
        "network",
        "nix",
        "opt",
        "private",
        "proc",
        "root",
        "run",
        "runtime",
        "sbin",
        "snap",
        "srv",
        "sys",
        "system",
        "tmp",
        "usr",
        "users",
        "var",
        "volumes",
        "workspace",
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


def _field_name_tokens(field_name: str) -> frozenset[str]:
    with_camel_boundaries = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", "_", field_name)
    return frozenset(part.casefold() for part in re.findall(r"[A-Za-z0-9]+", with_camel_boundaries))


def _is_canonical_artifact_ref_field(field_name: str) -> bool:
    return field_name == "artifact_ref" or field_name.endswith("_artifact_ref")


def _is_canonical_artifact_refs_field(field_name: str) -> bool:
    return field_name == "artifact_refs" or field_name.endswith("_artifact_refs")


def _canonical_artifact_ref_shape(schema: Mapping[str, Any], *, plural: bool) -> bool:
    if not plural:
        return schema.get("type") == "string"
    items = schema.get("items")
    return (
        schema.get("type") == "array"
        and isinstance(items, Mapping)
        and items.get("type") == "string"
        and type(schema.get("maxItems")) is int
    )


def _artifact_role_field(path_segments: tuple[str, ...]) -> str | None:
    fields = tuple(segment for segment in path_segments if segment != "*")
    return fields[-1] if fields else None


def _looks_like_artifact_locator_path(
    path_segments: tuple[str, ...],
    *,
    interpretation_contract_version: str,
) -> bool:
    fields = tuple(segment for segment in path_segments if segment != "*")
    tokens = frozenset(token for field_name in fields for token in _field_name_tokens(field_name))
    if tokens & {"artifact", "artifacts"} and tokens & _ARTIFACT_LOCATOR_TOKENS:
        return True
    compact_field = "".join(re.findall(r"[A-Za-z0-9]+", fields[-1])).casefold() if fields else ""
    if re.fullmatch(
        r"(?:result)?artifacts?(?:address|destination|download|endpoint|file|filename|handle|href|hyperlink|key|link|location|locator|path|pointer|ref|reference|src|target|uri|url)(?:value|values)?",
        compact_field,
    ):
        return True
    if interpretation_contract_version != "action_result_interpretation_contract_v2":
        return False
    for field_name in fields:
        compact = "".join(re.findall(r"[A-Za-z0-9]+", field_name)).casefold()
        artifact_index = compact.find("artifact")
        if artifact_index >= 0 and any(
            locator in compact[artifact_index + len("artifact") :] for locator in _ARTIFACT_LOCATOR_TOKENS
        ):
            return True
    return False


def _is_artifact_context_path(
    path_segments: tuple[str, ...],
    *,
    interpretation_contract_version: str,
) -> bool:
    """Return whether any schema-path component declares artifact context."""

    for field_name in path_segments:
        if field_name == "*":
            continue
        if _field_name_tokens(field_name) & {"artifact", "artifacts"}:
            return True
        compact = "".join(re.findall(r"[A-Za-z0-9]+", field_name)).casefold()
        if interpretation_contract_version == "action_result_interpretation_contract_v2" and "artifact" in compact:
            return True
    return False


def _schema_path_segments(schema_path: str) -> tuple[str, ...]:
    return tuple(segment.replace("~1", "/").replace("~0", "~") for segment in schema_path.split("/")[1:])


def _is_private_path_field(field_name: str) -> bool:
    """Reject declarations that explicitly advertise private filesystem data."""

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


def _schema_string_paths(schema: Mapping[str, Any], *, prefix: str = "") -> frozenset[str]:
    schema_type = schema.get("type")
    if schema_type == "string":
        return frozenset({prefix})
    paths: set[str] = set()
    if schema_type == "object":
        properties = schema.get("properties")
        if isinstance(properties, Mapping):
            for field_name, child in properties.items():
                if type(field_name) is not str or not isinstance(child, Mapping):
                    continue
                field_path = f"{prefix}/{_json_pointer_token(field_name)}"
                paths.update(_schema_string_paths(child, prefix=field_path))
    elif schema_type == "array":
        items = schema.get("items")
        if isinstance(items, Mapping):
            paths.update(_schema_string_paths(items, prefix=f"{prefix}/*"))
    return frozenset(paths)


def _valid_network_url(value: str) -> bool:
    if (
        any(character.isspace() or ord(character) < 0x20 or ord(character) == 0x7F for character in value)
        or "\\" in value
    ):
        return False
    try:
        parsed = urlsplit(value)
        _ = parsed.port
    except ValueError:
        return False
    hostname = parsed.hostname
    if parsed.scheme not in {"http", "https"} or hostname is None or parsed.username is not None:
        return False
    try:
        ipaddress.ip_address(hostname)
        return True
    except ValueError:
        try:
            ascii_hostname = hostname.encode("idna").decode("ascii")
        except UnicodeError:
            return False
        labels = ascii_hostname.split(".")
        return bool(labels) and all(
            re.fullmatch(r"[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?", label) is not None for label in labels
        )


def _validate_web_url(value: str, *, path: str) -> None:
    if not value or value != value.strip():
        raise ActionResultSchemaError(f"action_result_web_url_invalid:{path}")
    if value.startswith("/") and not value.startswith("//"):
        if "\\" in value or any(
            character.isspace() or ord(character) < 0x20 or ord(character) == 0x7F for character in value
        ):
            raise ActionResultSchemaError(f"action_result_web_url_invalid:{path}")
        root = value[1:].split("/", 1)[0].split("?", 1)[0].split("#", 1)[0].casefold()
        if not root or root in _LOCAL_ROOT_NAMES:
            raise ActionResultSchemaError(f"action_result_web_url_invalid:{path}")
        return
    if not _valid_network_url(value):
        raise ActionResultSchemaError(f"action_result_web_url_invalid:{path}")


def _strip_valid_network_urls(value: str) -> str:
    def replacement(match: re.Match[str]) -> str:
        candidate = match.group(0)
        return "" if _valid_network_url(candidate) else candidate

    return _NETWORK_URL_PATTERN.sub(replacement, value)


def _contains_raw_local_path(
    value: str,
    *,
    allow_embedded_network_urls: bool,
    allow_public_root_relative_urls: bool,
) -> bool:
    scan_value = _strip_valid_network_urls(value) if allow_embedded_network_urls else value
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
    if ("/" in scan_normalized or "\\" in scan_normalized) and _GENERIC_RELATIVE_FILE_PATH_PATTERN.search(
        scan_normalized
    ) is not None:
        return True
    if re.search(
        r"(?<![A-Za-z0-9_.-])(?:\.cache|cache|logs?|private|relative|runtime|tmp|workspace)[/\\]\S+",
        scan_value,
        re.IGNORECASE,
    ):
        return True
    if _SENSITIVE_RELATIVE_FILE_PATTERN.search(scan_value) is not None:
        return True
    for match in re.finditer(r"(?:^|[\s:(\[{=\"'])(/(?!/)[^\s)\]}>;,]+)", scan_value):
        token = match.group(1)
        components = tuple(component for component in token.split("/") if component)
        if not components:
            continue
        basename = components[-1].split("?", 1)[0].split("#", 1)[0]
        file_like = (
            basename.startswith(".")
            or re.search(
                r"\.(?:avro|bin|csv|db|docx?|html?|jpe?g|jsonl?|log|md|parquet|pdf|png|pptx?|py|sql|sqlite3?|toml|txt|webp|xlsx?|xml|ya?ml)$",
                basename,
                re.IGNORECASE,
            )
            is not None
        )
        if components[0].casefold() in _LOCAL_ROOT_NAMES or file_like or not allow_public_root_relative_urls:
            return True
    return False


def _validate_schema_policy(
    schema: Mapping[str, Any],
    *,
    max_items: int,
    max_depth: int,
    artifact_ref_schemes: tuple[str, ...],
    interpretation_contract_version: str,
    depth: int = 1,
    path: str = "$",
    path_segments: tuple[str, ...] = (),
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
            child_segments = (*path_segments, field_name)
            if _looks_like_artifact_locator_path(
                child_segments,
                interpretation_contract_version=interpretation_contract_version,
            ) and not (_is_canonical_artifact_ref_field(field_name) or _is_canonical_artifact_refs_field(field_name)):
                raise ActionResultSchemaError(f"action_result_artifact_locator_field_noncanonical:{path}.{field_name}")
            is_artifact_ref = _is_canonical_artifact_ref_field(field_name)
            is_artifact_refs = _is_canonical_artifact_refs_field(field_name)
            if (is_artifact_ref or is_artifact_refs) and not _canonical_artifact_ref_shape(
                child, plural=is_artifact_refs
            ):
                raise ActionResultSchemaError(f"action_result_artifact_ref_schema_invalid:{path}.{field_name}")
            if _is_private_path_field(field_name):
                raise ActionResultSchemaError(f"action_result_private_path_field_forbidden:{path}.{field_name}")
            item_count += _validate_schema_policy(
                child,
                max_items=max_items,
                max_depth=max_depth,
                artifact_ref_schemes=artifact_ref_schemes,
                interpretation_contract_version=interpretation_contract_version,
                depth=depth + 1,
                path=f"{path}.{field_name}",
                path_segments=child_segments,
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
            interpretation_contract_version=interpretation_contract_version,
            depth=depth + 1,
            path=f"{path}[]",
            path_segments=(*path_segments, "*"),
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


def _json_string_utf8_size(value: str, *, byte_limit: int | None = None) -> int:
    size = 2
    for character in value:
        codepoint = ord(character)
        if character in {'"', "\\"} or character in {"\b", "\f", "\n", "\r", "\t"}:
            size += 2
        elif codepoint < 0x20:
            size += 6
        else:
            size += len(character.encode("utf-8"))
        if byte_limit is not None and size > byte_limit:
            return byte_limit + 1
    return size


def _validate_schema_document(value: object) -> None:
    """Bound every JSON schema keyword/value before ``ToolSpec`` allocates."""

    item_count = [0]
    byte_count = [0]

    def add_bytes(count: int) -> None:
        byte_count[0] += count
        if byte_count[0] > ACTION_RESULT_MAX_SCHEMA_BYTES:
            raise ActionResultSchemaError("action_result_schema_document_bytes_exceeded")

    def walk(child: object, *, depth: int, path: str) -> None:
        if depth > ACTION_RESULT_MAX_SCHEMA_DOCUMENT_DEPTH:
            raise ActionResultSchemaError(f"action_result_schema_document_depth_exceeded:{path}")
        if isinstance(child, Mapping):
            item_count[0] += len(child)
            if item_count[0] > ACTION_RESULT_MAX_SCHEMA_DOCUMENT_ITEMS:
                raise ActionResultSchemaError("action_result_schema_document_items_exceeded")
            add_bytes(2 + max(0, len(child) - 1))
            for key, nested in child.items():
                if type(key) is not str or any(0xD800 <= ord(character) <= 0xDFFF for character in key):
                    raise ActionResultSchemaError(f"action_result_schema_document_key_invalid:{path}")
                remaining = max(0, ACTION_RESULT_MAX_SCHEMA_BYTES - byte_count[0] - 1)
                add_bytes(_json_string_utf8_size(key, byte_limit=remaining) + 1)
                walk(nested, depth=depth + 1, path=f"{path}.{key}")
            return
        if isinstance(child, (list, tuple)):
            item_count[0] += len(child)
            if item_count[0] > ACTION_RESULT_MAX_SCHEMA_DOCUMENT_ITEMS:
                raise ActionResultSchemaError("action_result_schema_document_items_exceeded")
            add_bytes(2 + max(0, len(child) - 1))
            for index, nested in enumerate(child):
                walk(nested, depth=depth + 1, path=f"{path}[{index}]")
            return
        if child is None:
            add_bytes(4)
            return
        if type(child) is bool:
            add_bytes(4 if child else 5)
            return
        if type(child) is int:
            if abs(child).bit_length() > ACTION_RESULT_MAX_INTEGER_BITS:
                raise ActionResultSchemaError(f"action_result_schema_document_integer_too_large:{path}")
            add_bytes(len(str(child)))
            return
        if type(child) is float:
            if not math.isfinite(child):
                raise ActionResultSchemaError(f"action_result_schema_document_nonfinite:{path}")
            add_bytes(len(json.dumps(child, allow_nan=False)))
            return
        if type(child) is str:
            if any(0xD800 <= ord(character) <= 0xDFFF for character in child):
                raise ActionResultSchemaError(f"action_result_schema_document_surrogate:{path}")
            remaining = max(0, ACTION_RESULT_MAX_SCHEMA_BYTES - byte_count[0])
            add_bytes(_json_string_utf8_size(child, byte_limit=remaining))
            return
        raise ActionResultSchemaError(f"action_result_schema_document_not_json:{path}")

    walk(value, depth=1, path="$")


def _validate_positive_display_text(value: str, *, path: str) -> None:
    """Accept human-readable text while rejecting high-confidence private locators.

    Slashes are ordinary display characters in role labels, ratios, commands,
    Markdown links, and public root-relative routes.  Network URLs are also
    display data here; fields that own a URL contract still use ``web_url``.
    Strip valid network URLs before looking for other URI schemes, then apply
    the filesystem parser instead of treating every separator as a path.
    """

    if (
        not value
        or value != value.strip()
        or any(ord(character) < 0x20 and character not in {"\t", "\n", "\r"} for character in value)
        or any(0x7F <= ord(character) <= 0x9F for character in value)
        or _URI_SCHEME_VALUE_PATTERN.search(_strip_valid_network_urls(value)) is not None
        or _contains_raw_local_path(
            value,
            allow_embedded_network_urls=True,
            allow_public_root_relative_urls=True,
        )
    ):
        raise ActionResultSchemaError(f"action_result_raw_local_path_forbidden:{path}")


def _validate_positive_identifier(value: str, *, path: str) -> None:
    """Keep identifiers transport-neutral and separator-free."""

    if (
        not value
        or value != value.strip()
        or any(character.isspace() for character in value)
        or "/" in value
        or "\\" in value
        or _URI_SCHEME_VALUE_PATTERN.search(value) is not None
        or any(ord(character) < 0x20 or 0x7F <= ord(character) <= 0x9F for character in value)
    ):
        raise ActionResultSchemaError(f"action_result_identifier_noncanonical:{path}")


def _validate_payload_policy(
    value: object,
    *,
    max_items: int,
    max_depth: int,
    max_serialized_bytes: int,
    artifact_ref_schemes: tuple[str, ...],
    field_value_roles: Mapping[str, str],
    interpretation_contract_version: str,
    depth: int = 1,
    path: str = "$",
    schema_path: str = "",
    item_counter: list[int] | None = None,
    byte_counter: list[int] | None = None,
) -> None:
    if item_counter is None:
        item_counter = [0]
    if byte_counter is None:
        byte_counter = [0]

    def add_bytes(count: int) -> None:
        byte_counter[0] += count
        if byte_counter[0] > max_serialized_bytes:
            raise ActionResultSchemaError("action_result_serialized_bytes_exceeded")

    if depth > max_depth:
        raise ActionResultSchemaError(f"action_result_depth_exceeded:{path}")
    if type(value) is dict:
        item_counter[0] += len(value)
        if item_counter[0] > max_items:
            raise ActionResultSchemaError("action_result_item_limit_exceeded")
        add_bytes(2 + max(0, len(value) - 1))
        for field_name, child in value.items():
            if type(field_name) is not str:
                raise ActionResultSchemaError(f"action_result_object_key_invalid:{path}")
            if any(0xD800 <= ord(character) <= 0xDFFF for character in field_name):
                raise ActionResultSchemaError(f"action_result_unicode_surrogate_forbidden:{path}")
            remaining = max(0, max_serialized_bytes - byte_counter[0] - 1)
            add_bytes(_json_string_utf8_size(field_name, byte_limit=remaining) + 1)
            child_schema_path = f"{schema_path}/{_json_pointer_token(field_name)}"
            _validate_payload_policy(
                child,
                max_items=max_items,
                max_depth=max_depth,
                max_serialized_bytes=max_serialized_bytes,
                artifact_ref_schemes=artifact_ref_schemes,
                field_value_roles=field_value_roles,
                interpretation_contract_version=interpretation_contract_version,
                depth=depth + 1,
                path=f"{path}.{field_name}",
                schema_path=child_schema_path,
                item_counter=item_counter,
                byte_counter=byte_counter,
            )
        return
    if type(value) is list:
        item_counter[0] += len(value)
        if item_counter[0] > max_items:
            raise ActionResultSchemaError("action_result_item_limit_exceeded")
        add_bytes(2 + max(0, len(value) - 1))
        for index, child in enumerate(value):
            _validate_payload_policy(
                child,
                max_items=max_items,
                max_depth=max_depth,
                max_serialized_bytes=max_serialized_bytes,
                artifact_ref_schemes=artifact_ref_schemes,
                field_value_roles=field_value_roles,
                interpretation_contract_version=interpretation_contract_version,
                depth=depth + 1,
                path=f"{path}[{index}]",
                schema_path=f"{schema_path}/*",
                item_counter=item_counter,
                byte_counter=byte_counter,
            )
        return
    if value is None:
        add_bytes(4)
        return
    if type(value) is bool:
        add_bytes(4 if value else 5)
        return
    if type(value) is int:
        if abs(value).bit_length() > ACTION_RESULT_MAX_INTEGER_BITS:
            raise ActionResultSchemaError(f"action_result_integer_too_large:{path}")
        add_bytes(len(str(value)))
        return
    if type(value) is float:
        if not math.isfinite(value):
            raise ActionResultSchemaError(f"action_result_nonfinite_number:{path}")
        add_bytes(len(json.dumps(value, allow_nan=False)))
        return
    if type(value) is str:
        if any(0xD800 <= ord(character) <= 0xDFFF for character in value):
            raise ActionResultSchemaError(f"action_result_unicode_surrogate_forbidden:{path}")
        role = field_value_roles.get(schema_path)
        if role == "web_url":
            _validate_web_url(value, path=path)
        elif role == "opaque_artifact_ref":
            _validate_opaque_artifact_ref(value, allowed_schemes=artifact_ref_schemes, path=path)
        elif _is_artifact_context_path(
            _schema_path_segments(schema_path),
            interpretation_contract_version=interpretation_contract_version,
        ) and _URI_SCHEME_VALUE_PATTERN.search(value):
            raise ActionResultSchemaError(f"action_result_artifact_locator_value_noncanonical:{path}")
        elif role == "display_text" and interpretation_contract_version != "action_result_interpretation_contract_v2":
            _validate_positive_display_text(value, path=path)
        elif role == "identifier" and interpretation_contract_version != "action_result_interpretation_contract_v2":
            _validate_positive_identifier(value, path=path)
        elif role == "display_text" and _contains_raw_local_path(
            value,
            allow_embedded_network_urls=True,
            allow_public_root_relative_urls=True,
        ):
            raise ActionResultSchemaError(f"action_result_raw_local_path_forbidden:{path}")
        elif role != "display_text" and (
            _NETWORK_URL_PATTERN.search(value) is not None
            or _contains_raw_local_path(
                value,
                allow_embedded_network_urls=False,
                allow_public_root_relative_urls=False,
            )
        ):
            raise ActionResultSchemaError(f"action_result_raw_local_path_forbidden:{path}")
        remaining = max(0, max_serialized_bytes - byte_counter[0])
        add_bytes(_json_string_utf8_size(value, byte_limit=remaining))
        return
    raise ActionResultSchemaError(f"action_result_not_strict_json:{path}")


def _freeze_json(value: object) -> object:
    if type(value) is dict:
        return MappingProxyType({cast(str, key): _freeze_json(child) for key, child in value.items()})
    if type(value) is list:
        return tuple(_freeze_json(child) for child in value)
    return value


def _json_pointer_segments(path: str) -> tuple[str, ...]:
    if not path.startswith("/"):
        return ()
    return tuple(token.replace("~1", "/").replace("~0", "~") for token in path[1:].split("/"))


_ACTION_RESULT_INTERPRETATION_CONTRACTS: Mapping[str, Mapping[str, object]] = MappingProxyType(
    {
        "action_result_interpretation_contract_v2": cast(
            Mapping[str, object],
            _freeze_json(
                {
                    "schema_version": "action_result_interpretation_contract_v2",
                    "validator_owner": "sourcing_agent.model_tool_runtime.ToolSpec.validate_input",
                    "canonicalizer_revision": "action_result_canonical_json_utf8_v2",
                    "value_policy_revision": "action_result_explicit_value_roles_v2",
                    "limit_policy_revision": "action_result_prevalidation_limits_v2",
                    "schema_document_limits": {"max_bytes": 32768, "max_depth": 128, "max_items": 10000},
                    "max_integer_bits": 13600,
                    "value_roles": ["control", "identifier", "display_text", "web_url", "opaque_artifact_ref"],
                    "canonical_json": {
                        "ensure_ascii": False,
                        "sort_keys": True,
                        "separators": [",", ":"],
                        "allow_nan": False,
                        "utf8_byte_limit": "prevalidated_exact",
                    },
                }
            ),
        ),
        ACTION_RESULT_INTERPRETATION_CONTRACT_VERSION: cast(
            Mapping[str, object],
            _freeze_json(
                {
                    "schema_version": ACTION_RESULT_INTERPRETATION_CONTRACT_VERSION,
                    "validator_owner": ACTION_RESULT_VALIDATOR_OWNER,
                    "canonicalizer_revision": ACTION_RESULT_CANONICALIZER_REVISION,
                    "value_policy_revision": ACTION_RESULT_VALUE_POLICY_REVISION,
                    "limit_policy_revision": ACTION_RESULT_LIMIT_POLICY_REVISION,
                    "schema_document_limits": {"max_bytes": 32768, "max_depth": 128, "max_items": 10000},
                    "max_integer_bits": 13600,
                    "value_roles": ["control", "identifier", "display_text", "web_url", "opaque_artifact_ref"],
                    "canonical_json": {
                        "ensure_ascii": False,
                        "sort_keys": True,
                        "separators": [",", ":"],
                        "allow_nan": False,
                        "utf8_byte_limit": "prevalidated_exact",
                    },
                }
            ),
        ),
    }
)


def _interpretation_contract_record(version: object) -> dict[str, object]:
    """Return one retained interpreter record by exact historical version."""

    normalized_version = _required_identifier(
        "interpretation_contract_version",
        version,
        pattern=_VERSION_PATTERN,
    )
    record = _ACTION_RESULT_INTERPRETATION_CONTRACTS.get(normalized_version)
    if record is None:
        raise ActionResultSchemaError(f"action_result_interpretation_contract_unknown:{normalized_version}")
    return cast(dict[str, object], _thaw_json(record))


ACTION_RESULT_INTERPRETATION_CONTRACT_DIGEST = _sha256_json(
    _interpretation_contract_record(ACTION_RESULT_INTERPRETATION_CONTRACT_VERSION)
)


@dataclass(frozen=True, slots=True)
class ActionResultActionOwner:
    """Closed owner binding for an action-backed tool result."""

    action_type: str

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "action_type", _required_identifier("action_type", self.action_type, pattern=_OWNER_PATTERN)
        )

    def to_fingerprint_record(self) -> dict[str, object]:
        return {"tool_kind": "action", "action_type": self.action_type, "query_owner": None}


@dataclass(frozen=True, slots=True)
class ActionResultQueryOwner:
    """Closed owner binding for a read-only query tool result."""

    owner_id: str
    owner_revision: str
    owner_contract_digest: str

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "owner_id", _required_identifier("query_owner_id", self.owner_id, pattern=_OWNER_PATTERN)
        )
        object.__setattr__(
            self,
            "owner_revision",
            _required_identifier("query_owner_revision", self.owner_revision, pattern=_VERSION_PATTERN),
        )
        object.__setattr__(
            self,
            "owner_contract_digest",
            _required_sha256("query_owner_contract_digest", self.owner_contract_digest),
        )

    def to_fingerprint_record(self) -> dict[str, object]:
        return {
            "tool_kind": "query",
            "action_type": None,
            "query_owner": {
                "owner_id": self.owner_id,
                "owner_revision": self.owner_revision,
                "owner_contract_digest": self.owner_contract_digest,
            },
        }


ActionResultOwnerBinding: TypeAlias = ActionResultActionOwner | ActionResultQueryOwner


def _schema_at_result_path(schema: Mapping[str, Any], field_path: str) -> Mapping[str, Any] | None:
    current: Mapping[str, Any] = schema
    for segment in _json_pointer_segments(field_path):
        if segment == "*":
            child = current.get("items")
        else:
            properties = current.get("properties")
            child = properties.get(segment) if isinstance(properties, Mapping) else None
        if not isinstance(child, Mapping):
            return None
        current = child
    return current


def _schema_has_closed_control_values(schema: Mapping[str, Any]) -> bool:
    if type(schema.get("const")) is str:
        return True
    values = schema.get("enum")
    return isinstance(values, (list, tuple)) and bool(values) and all(type(value) is str for value in values)


def _schema_has_bounded_identifier_values(schema: Mapping[str, Any]) -> bool:
    if _schema_has_closed_control_values(schema):
        return True
    return (
        schema.get("type") == "string"
        and type(schema.get("maxLength")) is int
        and int(schema["maxLength"]) > 0
        and type(schema.get("pattern")) is str
        and bool(schema.get("pattern"))
    )


def _provenance_for_value_path(provenance: Mapping[str, str], field_path: str) -> str:
    direct = provenance.get(field_path)
    if direct is not None:
        return direct
    ancestor = field_path
    while "/" in ancestor:
        ancestor = ancestor.rsplit("/", 1)[0]
        inherited = provenance.get(ancestor)
        if inherited is not None:
            return inherited
    raise ActionResultSchemaError(f"action_result_field_provenance_value_path_missing:{field_path}")


@dataclass(frozen=True, slots=True)
class ActionResultSpec:
    """Immutable result schema, owner, and serialization policy for one tool."""

    tool_name: str
    tool_kind: ActionResultToolKind
    owner_binding: ActionResultOwnerBinding
    result_schema_version: str
    serializer_owner: str
    serializer_revision: str
    serializer_contract: Mapping[str, Any]
    validator_owner: str
    variant_schemas: Mapping[str, Mapping[str, Any]]
    field_provenance: Mapping[str, Mapping[str, str]]
    field_value_roles: Mapping[str, Mapping[str, ActionResultValueRole]]
    max_serialized_bytes: int
    max_items: int
    max_depth: int
    artifact_ref_schemes: tuple[str, ...]
    interpretation_contract_version: str = ACTION_RESULT_INTERPRETATION_CONTRACT_VERSION
    externally_controlled_identifier_paths: tuple[str, ...] = ()
    _interpretation_contract: Mapping[str, object] = field(init=False, repr=False, compare=False)
    _variant_tools: Mapping[str, InternalToolValidatorSpec] = field(init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        if not is_valid_agent_tool_name(self.tool_name):
            raise ActionResultSchemaError("action_result_tool_name_invalid")
        tool_name = self.tool_name
        if self.tool_kind not in ACTION_RESULT_TOOL_KINDS:
            raise ActionResultSchemaError("action_result_tool_kind_invalid")
        if self.tool_kind == "action" and not isinstance(self.owner_binding, ActionResultActionOwner):
            raise ActionResultSchemaError("action_result_action_owner_binding_required")
        if self.tool_kind == "query" and not isinstance(self.owner_binding, ActionResultQueryOwner):
            raise ActionResultSchemaError("action_result_query_owner_binding_required")
        interpretation_contract_version = _required_identifier(
            "interpretation_contract_version",
            self.interpretation_contract_version,
            pattern=_VERSION_PATTERN,
        )
        interpretation_contract = _interpretation_contract_record(interpretation_contract_version)
        if type(self.externally_controlled_identifier_paths) is not tuple:
            raise ActionResultSchemaError("action_result_external_identifier_paths_invalid")
        external_identifier_paths = tuple(sorted(self.externally_controlled_identifier_paths))
        if len(external_identifier_paths) != len(set(external_identifier_paths)) or any(
            type(path) is not str or not path.startswith("/") for path in external_identifier_paths
        ):
            raise ActionResultSchemaError("action_result_external_identifier_paths_invalid")
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
        if type(self.serializer_contract) is not dict or not self.serializer_contract:
            raise ActionResultSchemaError("action_result_serializer_contract_invalid")
        _validate_payload_policy(
            self.serializer_contract,
            max_items=256,
            max_depth=8,
            max_serialized_bytes=8192,
            artifact_ref_schemes=(),
            field_value_roles={},
            interpretation_contract_version=interpretation_contract_version,
        )
        serialized_contract = json.loads(_canonical_json(self.serializer_contract))
        assert isinstance(serialized_contract, dict)
        frozen_serializer_contract = cast(Mapping[str, Any], _freeze_json(serialized_contract))
        validator_owner = _required_identifier(
            "validator_owner",
            self.validator_owner,
            pattern=_OWNER_PATTERN,
        )
        expected_validator_owner = interpretation_contract.get("validator_owner")
        if type(expected_validator_owner) is not str or validator_owner != expected_validator_owner:
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
        variant_tools: dict[str, InternalToolValidatorSpec] = {}
        for variant in ACTION_RESULT_VARIANTS:
            schema = self.variant_schemas.get(variant)
            if not isinstance(schema, Mapping):
                raise ActionResultSchemaError(f"action_result_variant_schema_invalid:{variant}")
            try:
                _validate_schema_document(schema)
                _validate_schema_policy(
                    schema,
                    max_items=self.max_items,
                    max_depth=self.max_depth,
                    artifact_ref_schemes=canonical_schemes,
                    interpretation_contract_version=interpretation_contract_version,
                )
                tool = InternalToolValidatorSpec(
                    name=f"{tool_name}:{variant}:result",
                    description=f"Model-safe {variant} result for {tool_name}.",
                    input_schema=schema,
                    schema_version=version,
                )
            except (ModelToolRuntimeError, ModelToolSchemaError, RecursionError, UnicodeError) as exc:
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

        if not isinstance(self.field_value_roles, Mapping) or set(self.field_value_roles) != set(
            ACTION_RESULT_VARIANTS
        ):
            raise ActionResultSchemaError("action_result_field_value_roles_variants_incomplete")
        frozen_value_roles: dict[str, Mapping[str, ActionResultValueRole]] = {}
        artifact_role_found = False
        used_external_identifier_paths: set[str] = set()
        for variant in ACTION_RESULT_VARIANTS:
            roles = self.field_value_roles.get(variant)
            if not isinstance(roles, Mapping) or any(type(path) is not str for path in roles):
                raise ActionResultSchemaError(f"action_result_field_value_roles_invalid:{variant}")
            expected_paths = _schema_string_paths(frozen_schemas[variant])
            actual_paths = set(roles)
            missing_paths = sorted(expected_paths - actual_paths)
            extra_paths = sorted(actual_paths - expected_paths)
            if missing_paths or extra_paths:
                raise ActionResultSchemaError(
                    f"action_result_field_value_roles_path_mismatch:{variant}:"
                    f"missing={','.join(missing_paths)}:extra={','.join(extra_paths)}"
                )
            normalized_roles: dict[str, ActionResultValueRole] = {}
            for field_path in sorted(expected_paths):
                role = roles[field_path]
                if type(role) is not str or role not in ACTION_RESULT_VALUE_ROLES:
                    raise ActionResultSchemaError(f"action_result_field_value_role_invalid:{variant}:{field_path}")
                segments = _json_pointer_segments(field_path)
                field_name = _artifact_role_field(segments)
                artifact_context = _is_artifact_context_path(
                    segments,
                    interpretation_contract_version=interpretation_contract_version,
                )
                canonical_artifact_path = field_name is not None and (
                    _is_canonical_artifact_ref_field(field_name) or _is_canonical_artifact_refs_field(field_name)
                )
                provenance_class = _provenance_for_value_path(frozen_provenance[variant], field_path)
                externally_controlled = provenance_class in {"user_supplied", "provider_observed", "model_inferred"}
                field_schema = _schema_at_result_path(frozen_schemas[variant], field_path)
                assert field_schema is not None
                if role == "control" and (externally_controlled or not _schema_has_closed_control_values(field_schema)):
                    raise ActionResultSchemaError(
                        f"action_result_control_provenance_or_schema_invalid:{variant}:{field_path}"
                    )
                if role == "identifier" and externally_controlled:
                    if field_path not in external_identifier_paths or not _schema_has_bounded_identifier_values(
                        field_schema
                    ):
                        raise ActionResultSchemaError(
                            f"action_result_external_identifier_exception_required:{variant}:{field_path}"
                        )
                    used_external_identifier_paths.add(field_path)
                if role == "opaque_artifact_ref" and externally_controlled:
                    raise ActionResultSchemaError(
                        f"action_result_external_artifact_ref_forbidden:{variant}:{field_path}"
                    )
                if canonical_artifact_path and role != "opaque_artifact_ref":
                    raise ActionResultSchemaError(f"action_result_artifact_ref_role_required:{variant}:{field_path}")
                if role == "opaque_artifact_ref" and not canonical_artifact_path:
                    raise ActionResultSchemaError(
                        f"action_result_artifact_ref_path_noncanonical:{variant}:{field_path}"
                    )
                if artifact_context and not canonical_artifact_path and role not in {"control", "identifier"}:
                    raise ActionResultSchemaError(
                        f"action_result_artifact_value_role_noncanonical:{variant}:{field_path}"
                    )
                if role == "opaque_artifact_ref":
                    artifact_role_found = True
                normalized_roles[field_path] = cast(ActionResultValueRole, role)
            frozen_value_roles[variant] = MappingProxyType(normalized_roles)
        unused_external_identifier_paths = sorted(set(external_identifier_paths) - used_external_identifier_paths)
        if unused_external_identifier_paths:
            raise ActionResultSchemaError(
                "action_result_external_identifier_exception_unused:" + ",".join(unused_external_identifier_paths)
            )
        if artifact_role_found and not canonical_schemes:
            raise ActionResultSchemaError("action_result_artifact_ref_policy_required")
        if canonical_schemes and not artifact_role_found:
            raise ActionResultSchemaError("action_result_artifact_ref_policy_unused")

        object.__setattr__(self, "tool_name", tool_name)
        object.__setattr__(self, "result_schema_version", version)
        object.__setattr__(self, "serializer_owner", serializer_owner)
        object.__setattr__(self, "serializer_revision", serializer_revision)
        object.__setattr__(self, "serializer_contract", frozen_serializer_contract)
        object.__setattr__(self, "validator_owner", validator_owner)
        object.__setattr__(self, "artifact_ref_schemes", canonical_schemes)
        object.__setattr__(self, "interpretation_contract_version", interpretation_contract_version)
        object.__setattr__(self, "externally_controlled_identifier_paths", external_identifier_paths)
        object.__setattr__(self, "_interpretation_contract", _freeze_json(interpretation_contract))
        object.__setattr__(self, "variant_schemas", MappingProxyType(frozen_schemas))
        object.__setattr__(self, "field_provenance", MappingProxyType(frozen_provenance))
        object.__setattr__(self, "field_value_roles", MappingProxyType(frozen_value_roles))
        object.__setattr__(self, "_variant_tools", MappingProxyType(variant_tools))

    @property
    def action_type(self) -> str | None:
        return self.owner_binding.action_type if isinstance(self.owner_binding, ActionResultActionOwner) else None

    @property
    def query_owner_id(self) -> str | None:
        return self.owner_binding.owner_id if isinstance(self.owner_binding, ActionResultQueryOwner) else None

    @property
    def route_identity(self) -> tuple[str, str]:
        identity = self.action_type if self.tool_kind == "action" else self.query_owner_id
        assert identity is not None
        return self.tool_kind, identity

    @property
    def serializer_contract_digest(self) -> str:
        return _sha256_json(
            {
                "schema_version": ACTION_RESULT_SERIALIZER_CONTRACT_VERSION,
                "serializer_owner": self.serializer_owner,
                "serializer_revision": self.serializer_revision,
                "contract": _thaw_json(self.serializer_contract),
            }
        )

    @property
    def interpretation_contract_digest(self) -> str:
        return _sha256_json(self._interpretation_contract)

    @property
    def allowed_variants(self) -> tuple[ActionResultVariant, ...]:
        return cast(tuple[ActionResultVariant, ...], ACTION_RESULT_VARIANTS)

    def to_fingerprint_record(self) -> dict[str, object]:
        return {
            "tool_name": self.tool_name,
            "tool_kind": self.tool_kind,
            "owner_binding": self.owner_binding.to_fingerprint_record(),
            "result_schema_version": self.result_schema_version,
            "serializer_owner": self.serializer_owner,
            "serializer_revision": self.serializer_revision,
            "serializer_contract": _thaw_json(self.serializer_contract),
            "serializer_contract_digest": self.serializer_contract_digest,
            "validator_owner": self.validator_owner,
            "interpretation_contract": _thaw_json(self._interpretation_contract),
            "interpretation_contract_digest": self.interpretation_contract_digest,
            "externally_controlled_identifier_paths": list(self.externally_controlled_identifier_paths),
            "variant_schemas": _thaw_json(self.variant_schemas),
            "field_provenance": _thaw_json(self.field_provenance),
            "field_value_roles": _thaw_json(self.field_value_roles),
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
        variant = owner_output.get("variant")
        if type(variant) is not str or variant not in ACTION_RESULT_VARIANTS:
            raise ActionResultSchemaError("action_result_variant_not_allowed")
        _validate_payload_policy(
            owner_output,
            max_items=self.max_items,
            max_depth=self.max_depth,
            max_serialized_bytes=self.max_serialized_bytes,
            artifact_ref_schemes=self.artifact_ref_schemes,
            field_value_roles=self.field_value_roles[variant],
            interpretation_contract_version=self.interpretation_contract_version,
        )
        tool = self._variant_tools[variant]
        try:
            validated = tool.validate_input(owner_output)
        except (ModelToolRuntimeError, ModelToolSchemaError, RecursionError) as exc:
            raise ActionResultSchemaError(f"action_result_schema_validation_failed:{exc}") from exc
        encoded = _canonical_json(validated)
        return encoded

    def serialize_bytes(self, owner_output: dict[str, Any]) -> bytes:
        return self.serialize(owner_output).encode("utf-8")


@dataclass(frozen=True, slots=True, init=False)
class ActionResultRegistry:
    """Immutable retained registry; it declares history and never infers current."""

    specs: tuple[ActionResultSpec, ...]
    _by_tool: Mapping[str, tuple[ActionResultSpec, ...]] = field(init=False, repr=False, compare=False)
    _historical: Mapping[tuple[str, str, str], ActionResultSpec] = field(init=False, repr=False, compare=False)

    def __init__(self, specs: Iterable[ActionResultSpec] = ()) -> None:
        try:
            materialized = tuple(specs)
        except TypeError as exc:
            raise ActionResultSchemaError("action_result_registry_specs_invalid") from exc
        by_tool: dict[str, list[ActionResultSpec]] = {}
        historical: dict[tuple[str, str, str], ActionResultSpec] = {}
        version_digests: dict[tuple[str, str], str] = {}
        tool_routes: dict[str, tuple[str, str]] = {}
        route_to_tool: dict[tuple[str, str], str] = {}
        for spec in materialized:
            if not isinstance(spec, ActionResultSpec):
                raise ActionResultSchemaError("action_result_registry_entry_invalid")
            historical_key = (spec.tool_name, spec.result_schema_version, spec.result_schema_digest)
            if historical_key in historical:
                raise ActionResultSchemaError(f"action_result_registry_duplicate_spec:{spec.tool_name}")
            version_key = (spec.tool_name, spec.result_schema_version)
            prior_digest = version_digests.get(version_key)
            if prior_digest is not None and prior_digest != spec.result_schema_digest:
                raise ActionResultSchemaError(f"action_result_registry_version_digest_drift:{spec.tool_name}")
            prior_route = tool_routes.get(spec.tool_name)
            if prior_route is not None and prior_route != spec.route_identity:
                raise ActionResultSchemaError(f"action_result_registry_owner_binding_drift:{spec.tool_name}")
            prior_tool_name = route_to_tool.get(spec.route_identity)
            if prior_tool_name is not None and prior_tool_name != spec.tool_name:
                raise ActionResultSchemaError(f"action_result_registry_owner_route_collision:{spec.route_identity[1]}")
            version_digests[version_key] = spec.result_schema_digest
            tool_routes[spec.tool_name] = spec.route_identity
            route_to_tool[spec.route_identity] = spec.tool_name
            historical[historical_key] = spec
            by_tool.setdefault(spec.tool_name, []).append(spec)
        ordered_specs = tuple(
            sorted(
                materialized, key=lambda item: (item.tool_name, item.result_schema_version, item.result_schema_digest)
            )
        )
        frozen_by_tool = {
            tool_name: tuple(sorted(items, key=lambda item: (item.result_schema_version, item.result_schema_digest)))
            for tool_name, items in sorted(by_tool.items())
        }
        object.__setattr__(self, "specs", ordered_specs)
        object.__setattr__(self, "_by_tool", MappingProxyType(frozen_by_tool))
        object.__setattr__(self, "_historical", MappingProxyType(historical))

    @classmethod
    def from_specs(cls, specs: Iterable[ActionResultSpec]) -> ActionResultRegistry:
        return cls(specs)

    @property
    def tool_names(self) -> tuple[str, ...]:
        return tuple(self._by_tool)

    def specs_for_tool(self, tool_name: str) -> tuple[ActionResultSpec, ...]:
        return self._by_tool.get(tool_name, ())

    def get_historical(
        self,
        tool_name: str,
        result_schema_version: str,
        result_schema_digest: str,
    ) -> ActionResultSpec | None:
        return self._historical.get((tool_name, result_schema_version, result_schema_digest))

    def require_historical(
        self,
        tool_name: str,
        result_schema_version: str,
        result_schema_digest: str,
    ) -> ActionResultSpec:
        spec = self.get_historical(tool_name, result_schema_version, result_schema_digest)
        if spec is None:
            raise ActionResultSchemaError(
                f"action_result_historical_spec_missing:{tool_name}:{result_schema_version}:{result_schema_digest}"
            )
        return spec

    def to_manifest_record(self) -> dict[str, object]:
        records = [spec.to_manifest_record() for spec in self.specs]
        return {
            "schema_version": ACTION_RESULT_REGISTRY_SCHEMA_VERSION,
            "tool_count": len(self.tool_names),
            "retained_spec_count": len(records),
            "tools": records,
        }

    @property
    def registry_digest(self) -> str:
        return _sha256_json(self.to_manifest_record())


DEFAULT_ACTION_RESULT_REGISTRY = ActionResultRegistry.from_specs(())


__all__ = [
    "ACTION_RESULT_CANONICALIZER_REVISION",
    "ACTION_RESULT_INTERPRETATION_CONTRACT_DIGEST",
    "ACTION_RESULT_INTERPRETATION_CONTRACT_VERSION",
    "ACTION_RESULT_LIMIT_POLICY_REVISION",
    "ACTION_RESULT_REGISTRY_SCHEMA_VERSION",
    "ACTION_RESULT_PROVENANCE_CLASSES",
    "ACTION_RESULT_SERIALIZER_CONTRACT_VERSION",
    "ACTION_RESULT_TOOL_KINDS",
    "ACTION_RESULT_VALIDATOR_OWNER",
    "ACTION_RESULT_VALUE_POLICY_REVISION",
    "ACTION_RESULT_VALUE_ROLES",
    "ACTION_RESULT_VARIANTS",
    "ActionResultActionOwner",
    "ActionResultOwnerBinding",
    "ActionResultQueryOwner",
    "ActionResultRegistry",
    "ActionResultSchemaError",
    "ActionResultSpec",
    "ActionResultToolKind",
    "ActionResultValueRole",
    "ActionResultVariant",
    "DEFAULT_ACTION_RESULT_REGISTRY",
]
