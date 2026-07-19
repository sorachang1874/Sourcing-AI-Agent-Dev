"""Opaque agent runtime namespace ref contract (``agent_runtime_namespace_ref.v1``).

This module is a pure S1f0c FF-SCHEMA contract foundation.  Every literal,
owner, ordered field set, and descriptor below is consumed verbatim from the
canonical decision manifest
``docs/modules/serving-product/contracts/filter_projection_lineage_fixed_forward_decision_v1.json``;
nothing here retypes, renames, or reinterprets the manifest.

Contract facts pinned by the manifest:

- owner is ``agent_runtime_namespace_registry`` and only the registry resolves
  a ref; the PG row additionally stores the private canonical
  ``runtime_namespace`` path and lifecycle state, which never leave the server
  boundary;
- ``ref_digest`` covers the first seven fields;
- ``generation`` is a positive exact integer;
- ``provider_mode`` is ``simulate`` or ``scripted``; live and replay are not
  eligible at this boundary;
- the public V3 surface emits only ``schema_version``, ``namespace_ref_id``,
  and ``ref_digest``.

Retained-v1 path-bearing history fence: the retained
``cohort_execution_capability.v1`` record carries a raw path-bearing
``runtime_namespace`` string.  It remains exact history, looked up by exact
version plus contract digest, and is never re-minted, mutated, or
auto-upgraded into a namespace ref.  This module mints and parses only the
opaque v1 ref and fails closed on any retained path-bearing shape.

Decode boundary (manifest canonical-JSON rule "duplicate keys rejected at
decode"): this module owns the one shared strict JSON decoder for all three
FF-SCHEMA contract families.  ``strict_json_loads`` rejects duplicate object
keys and non-JSON constants (``NaN``/``Infinity``) at any depth and produces
the decoder-typed ``CanonicalJsonObject``.  Public parsers accept raw
text/bytes and invoke that decoder internally; a plain dictionary is never a
decode boundary.  ``CanonicalJsonObject`` is provenance: it is produced only
by the strict decoder or by trusted programmatic construction inside the
three contract modules (mint and parse canonicalization), so external input
always passes duplicate rejection at the text boundary.
"""

from __future__ import annotations

import hashlib
import json
import re
from typing import Any

AGENT_RUNTIME_NAMESPACE_REF_SCHEMA_VERSION = "agent_runtime_namespace_ref.v1"
AGENT_RUNTIME_NAMESPACE_REF_OWNER = "agent_runtime_namespace_registry"
AGENT_RUNTIME_NAMESPACE_REF_PROVIDER_MODES = ("simulate", "scripted")
AGENT_RUNTIME_NAMESPACE_REF_PUBLIC_FIELDS = ("schema_version", "namespace_ref_id", "ref_digest")

# Retained history, never re-minted: the v1 capability stays an exact
# path-bearing record owned by cohort_provider_compiler; lookup is exact
# version + contract digest with no shape inference, lexicographic-latest
# alias, mutable current alias, or auto-upgrade.
RETAINED_V1_CAPABILITY_LITERAL = "cohort_execution_capability.v1"
RETAINED_PATH_BEARING_HISTORY_LITERALS = (RETAINED_V1_CAPABILITY_LITERAL,)
RETAINED_LOOKUP_FORBIDDEN = (
    "shape inference",
    "lexicographic latest",
    "mutable current alias",
    "auto-upgrade",
)

_SHA256_HEX_RE = re.compile(r"[0-9a-f]{64}")


class AgentRuntimeNamespaceRefError(ValueError):
    """Raised when an agent runtime namespace ref value fails closed."""


def canonical_json(value: Any) -> str:
    """Manifest canonical JSON: sorted keys, compact separators, Unicode preserved, ``allow_nan=False``."""

    try:
        return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)
    except (TypeError, ValueError, OverflowError, UnicodeError) as exc:
        raise AgentRuntimeNamespaceRefError("agent_runtime_namespace_ref JSON invalid") from exc


def contract_digest(schema: dict[str, Any]) -> str:
    """Manifest-pinned digest equation: ``SHA256(UTF8(canonical_json(schema)))``."""

    return hashlib.sha256(canonical_json(schema).encode("utf-8")).hexdigest()


_DECODE_TOKEN = object()


class CanonicalJsonObject(dict):
    """Decoder-typed JSON object: duplicate-free and non-finite-free by construction.

    Instances are produced solely by ``strict_json_loads`` (every external
    text/bytes boundary) or by trusted programmatic construction inside the
    three FF-SCHEMA contract modules (mint and parse canonicalization); the
    constructor itself is token-guarded.  Contract parsers accept raw
    text/bytes (strictly decoded) or this provenance type — never a plain
    dictionary, which could be an already-collapsed standard-library decode.
    """

    __slots__ = ()

    def __init__(self, *args: Any, _token: object | None = None, **kwargs: Any) -> None:
        if _token is not _DECODE_TOKEN:
            raise AgentRuntimeNamespaceRefError(
                "canonical JSON objects are produced only by strict_json_loads or trusted contract-module construction"
            )
        super().__init__(*args, **kwargs)


def strict_json_loads(payload: str | bytes | bytearray) -> Any:
    """The one shared strict JSON decoder for the FF-SCHEMA contract families.

    Manifest canonical-JSON rules made executable at the decode boundary:
    duplicate object keys are rejected at every depth, and non-JSON constants
    (``NaN``, ``Infinity``, ``-Infinity``) are rejected.  Every JSON object in
    the decoded value is a ``CanonicalJsonObject``.
    """

    if isinstance(payload, (bytes, bytearray)):
        try:
            text = bytes(payload).decode("utf-8")
        except UnicodeDecodeError as exc:
            raise AgentRuntimeNamespaceRefError("contract JSON payload is not UTF-8") from exc
    elif type(payload) is str:
        text = payload
    else:
        raise AgentRuntimeNamespaceRefError("contract JSON payload must be str or UTF-8 bytes")

    def _no_duplicate_keys(pairs: list[tuple[str, Any]]) -> CanonicalJsonObject:
        result = CanonicalJsonObject(_token=_DECODE_TOKEN)
        for key, value in pairs:
            if key in result:
                raise AgentRuntimeNamespaceRefError(f"contract JSON duplicate key {key!r} at decode")
            result[key] = value
        return result

    def _no_non_json_constant(token: str) -> Any:
        raise AgentRuntimeNamespaceRefError(f"contract JSON non-finite constant {token!r} at decode")

    try:
        return json.loads(text, object_pairs_hook=_no_duplicate_keys, parse_constant=_no_non_json_constant)
    except json.JSONDecodeError as exc:
        raise AgentRuntimeNamespaceRefError("contract JSON payload is not valid JSON") from exc


def _canonical_json_object(value: Any) -> CanonicalJsonObject:
    """Trusted programmatic construction: decoder-typed object for one JSON object value.

    Private to the three FF-SCHEMA contract modules (mint and parse
    canonicalization).  The value is re-encoded with ``allow_nan=False`` and
    strict-decoded, so the result is duplicate-free and non-finite-free at
    every depth.  This is never a public decode boundary: external input must
    go through ``strict_json_loads`` or the text/bytes parse path.
    """

    try:
        encoded = json.dumps(value, ensure_ascii=False, allow_nan=False)
    except (TypeError, ValueError, OverflowError, UnicodeError) as exc:
        raise AgentRuntimeNamespaceRefError("contract JSON value invalid") from exc
    result = strict_json_loads(encoded)
    if not isinstance(result, CanonicalJsonObject):
        raise AgentRuntimeNamespaceRefError("contract JSON value must be an object")
    return result


def _fail(label: str, reason: str) -> None:
    raise AgentRuntimeNamespaceRefError(f"agent_runtime_namespace_ref {label} invalid: {reason}")


def _type_strict_equal(actual: object, expected: object) -> bool:
    if type(actual) is not type(expected):
        return False
    if isinstance(expected, dict):
        if set(actual) != set(expected):
            return False
        return all(_type_strict_equal(actual[key], expected[key]) for key in expected)
    if isinstance(expected, list):
        if len(actual) != len(expected):
            return False
        return all(_type_strict_equal(left, right) for left, right in zip(actual, expected, strict=True))
    return actual == expected


def _validate_value(value: Any, descriptor: dict[str, Any], label: str) -> None:
    declared = descriptor["type"]
    if declared == "string":
        if type(value) is not str:
            _fail(label, "expected string")
        if descriptor.get("nonempty") and not value:
            _fail(label, "empty string")
        if "max_length" in descriptor and len(value) > descriptor["max_length"]:
            _fail(label, "max_length exceeded")
        if "enum" in descriptor and value not in descriptor["enum"]:
            _fail(label, "enum violation")
        if descriptor.get("format") == "sha256_hex" and not _SHA256_HEX_RE.fullmatch(value):
            _fail(label, "sha256_hex format violation")
        if descriptor.get("format") == "https_url" and not (
            value.startswith("https://") and not any(character.isspace() for character in value)
        ):
            _fail(label, "https_url format violation")
    elif declared == "integer":
        if type(value) is not int:  # rejects bool and float aliases
            _fail(label, "expected integer")
        if "minimum" in descriptor and value < descriptor["minimum"]:
            _fail(label, "minimum violated")
        if "maximum" in descriptor and value > descriptor["maximum"]:
            _fail(label, "maximum violated")
    elif declared == "boolean":
        if type(value) is not bool:
            _fail(label, "expected boolean")
    elif declared == "null":
        if value is not None:
            _fail(label, "expected null")
    elif declared == "object":
        if not isinstance(value, dict):
            _fail(label, "expected object")
        fields = descriptor.get("fields")
        if fields is None:
            _fail(label, "object descriptor without closed fields")
        _validate_object(value, fields, label)
    elif declared == "array":
        if type(value) is not list:
            _fail(label, "expected array")
        if "min_items" in descriptor and len(value) < descriptor["min_items"]:
            _fail(label, "min_items violated")
        if "max_items" in descriptor and len(value) > descriptor["max_items"]:
            _fail(label, "max_items violated")
        items = descriptor.get("items")
        if not isinstance(items, dict):
            _fail(label, "array descriptor without closed items")
        for index, item in enumerate(value):
            _validate_value(item, items, f"{label}[{index}]")
    else:
        _fail(label, f"unknown descriptor type {declared!r}")
    if "constant" in descriptor and not _type_strict_equal(value, descriptor["constant"]):
        _fail(label, "constant mismatch")


def _validate_object(record: Any, fields: list[dict[str, Any]], label: str) -> None:
    if not isinstance(record, dict):
        _fail(label, "expected object")
    declared = {field["name"]: field for field in fields}
    required = {name for name, field in declared.items() if field.get("required") is True}
    missing = required - set(record)
    if missing:
        _fail(label, f"missing required fields {sorted(missing)}")
    unknown = set(record) - set(declared)
    if unknown:
        _fail(label, f"unknown fields {sorted(unknown)}")
    for name, field in declared.items():
        if name in record:
            _validate_value(record[name], field, f"{label}.{name}")


AGENT_RUNTIME_NAMESPACE_REF_ORDERED_FIELDS = ('schema_version',
 'owner',
 'namespace_ref_id',
 'workspace_id',
 'provider_mode',
 'policy_revision',
 'generation',
 'ref_digest')


AGENT_RUNTIME_NAMESPACE_REF_SCHEMA = {'schema_version': 'agent_runtime_namespace_ref.v1',
 'owner': 'agent_runtime_namespace_registry',
 'fields': [{'name': 'schema_version',
             'type': 'string',
             'required': True,
             'constant': 'agent_runtime_namespace_ref.v1'},
            {'name': 'owner', 'type': 'string', 'required': True, 'constant': 'agent_runtime_namespace_registry'},
            {'name': 'namespace_ref_id', 'type': 'string', 'required': True, 'nonempty': True},
            {'name': 'workspace_id', 'type': 'string', 'required': True, 'nonempty': True},
            {'name': 'provider_mode',
             'type': 'string',
             'required': True,
             'enum': ['simulate', 'scripted'],
             'derivation': 'live and replay are not eligible at this boundary'},
            {'name': 'policy_revision', 'type': 'string', 'required': True, 'nonempty': True},
            {'name': 'generation',
             'type': 'integer',
             'required': True,
             'minimum': 1,
             'derivation': 'positive exact integer'},
            {'name': 'ref_digest',
             'type': 'string',
             'required': True,
             'format': 'sha256_hex',
             'derivation': 'covers the first seven fields'}]}

AGENT_RUNTIME_NAMESPACE_REF_CONTRACT_DIGEST = contract_digest(AGENT_RUNTIME_NAMESPACE_REF_SCHEMA)

# ref_digest covers the first seven fields: every field except ref_digest itself.
_REF_DIGEST_INPUT_FIELDS = AGENT_RUNTIME_NAMESPACE_REF_ORDERED_FIELDS[:-1]


def compute_agent_runtime_namespace_ref_digest(record: dict[str, Any]) -> str:
    """Recompute ``ref_digest`` over the canonical first seven fields of one ref record."""

    if not isinstance(record, dict):
        _fail("ref_digest input", "expected object")
    try:
        covered = {field: record[field] for field in _REF_DIGEST_INPUT_FIELDS}
    except KeyError as exc:
        _fail("ref_digest input", f"missing field {exc}")
    return hashlib.sha256(canonical_json(covered).encode("utf-8")).hexdigest()


def assert_not_retained_path_bearing_history(value: Any) -> None:
    """History fence: retained v1 path-bearing records are history and are never re-minted here."""

    if isinstance(value, dict) and value.get("schema_version") in RETAINED_PATH_BEARING_HISTORY_LITERALS:
        _fail("schema_version", "retained v1 path-bearing history is never re-minted as a namespace ref")


def _coerce_boundary(value: Any, label: str) -> CanonicalJsonObject:
    if isinstance(value, (str, bytes, bytearray)):
        value = strict_json_loads(value)
    if not isinstance(value, CanonicalJsonObject):
        _fail(
            label,
            "record must be JSON text/bytes (strictly decoded) or a decoder-produced canonical JSON object;"
            " plain dictionaries are not a decode boundary",
        )
    return value


def validate_agent_runtime_namespace_ref(value: Any) -> None:
    """Fail closed unless ``value`` is one exact ``agent_runtime_namespace_ref.v1`` record."""

    parse_agent_runtime_namespace_ref(value)


def parse_agent_runtime_namespace_ref(value: Any) -> CanonicalJsonObject:
    """Validate one ref record and return it canonicalized in manifest field order.

    Parse is closed: ``value`` must be raw JSON text/bytes (strictly decoded
    with duplicate-key and non-finite rejection) or a decoder-produced
    ``CanonicalJsonObject`` — never a plain dictionary.  Unknown or missing
    fields, type/enum/format/bound violations, retained path-bearing history
    shapes, and any ``ref_digest`` that does not recompute from the first
    seven fields all fail closed (a copied digest is never proof without
    rebuilding its source object).
    """

    record = _coerce_boundary(value, "agent_runtime_namespace_ref")
    assert_not_retained_path_bearing_history(record)
    _validate_object(record, AGENT_RUNTIME_NAMESPACE_REF_SCHEMA["fields"], "agent_runtime_namespace_ref")
    ordered = {field: record[field] for field in AGENT_RUNTIME_NAMESPACE_REF_ORDERED_FIELDS}
    if ordered["ref_digest"] != compute_agent_runtime_namespace_ref_digest(ordered):
        _fail("ref_digest", "does not recompute from the first seven fields")
    return _canonical_json_object(ordered)


def mint_agent_runtime_namespace_ref(
    *,
    namespace_ref_id: Any,
    workspace_id: Any,
    provider_mode: Any,
    policy_revision: Any,
    generation: Any,
) -> CanonicalJsonObject:
    """Mint one opaque ``agent_runtime_namespace_ref.v1`` record.

    Only ``agent_runtime_namespace_registry`` may mint; this pure contract
    module derives ``ref_digest`` and validates the closed record.  There is
    no ``runtime_namespace`` path parameter by design: the private canonical
    path and lifecycle state stay inside the registry and never enter the ref.
    """

    candidate = {
        "schema_version": AGENT_RUNTIME_NAMESPACE_REF_SCHEMA_VERSION,
        "owner": AGENT_RUNTIME_NAMESPACE_REF_OWNER,
        "namespace_ref_id": namespace_ref_id,
        "workspace_id": workspace_id,
        "provider_mode": provider_mode,
        "policy_revision": policy_revision,
        "generation": generation,
    }
    candidate["ref_digest"] = compute_agent_runtime_namespace_ref_digest(candidate)
    return parse_agent_runtime_namespace_ref(_canonical_json_object(candidate))


def agent_runtime_namespace_ref_public_record(value: Any) -> CanonicalJsonObject:
    """Project one validated ref to its exact public V3 subset; workspace/path/lifecycle never leave the server."""

    record = parse_agent_runtime_namespace_ref(value)
    return _canonical_json_object({field: record[field] for field in AGENT_RUNTIME_NAMESPACE_REF_PUBLIC_FIELDS})


__all__ = [
    "AGENT_RUNTIME_NAMESPACE_REF_CONTRACT_DIGEST",
    "AGENT_RUNTIME_NAMESPACE_REF_ORDERED_FIELDS",
    "AGENT_RUNTIME_NAMESPACE_REF_OWNER",
    "AGENT_RUNTIME_NAMESPACE_REF_PROVIDER_MODES",
    "AGENT_RUNTIME_NAMESPACE_REF_PUBLIC_FIELDS",
    "AGENT_RUNTIME_NAMESPACE_REF_SCHEMA",
    "AGENT_RUNTIME_NAMESPACE_REF_SCHEMA_VERSION",
    "RETAINED_LOOKUP_FORBIDDEN",
    "RETAINED_PATH_BEARING_HISTORY_LITERALS",
    "RETAINED_V1_CAPABILITY_LITERAL",
    "AgentRuntimeNamespaceRefError",
    "CanonicalJsonObject",
    "agent_runtime_namespace_ref_public_record",
    "assert_not_retained_path_bearing_history",
    "canonical_json",
    "compute_agent_runtime_namespace_ref_digest",
    "contract_digest",
    "mint_agent_runtime_namespace_ref",
    "parse_agent_runtime_namespace_ref",
    "strict_json_loads",
    "validate_agent_runtime_namespace_ref",
]
