"""Filter-projection product terminal contract family (S1f0c FF-SCHEMA pure contract foundations).

This module consumes the canonical decision manifest
``docs/modules/serving-product/contracts/filter_projection_lineage_fixed_forward_decision_v1.json``
verbatim.  No literal, owner, ordered field set, descriptor, or digest here is
retyped, renamed, or reinterpreted.

The family pins the product terminal and its nested immutable inputs, the four
internal owner refs, and the model-safe V3 result contract:

- ``filter_projection_product_terminal.v1`` (owner ``serving_projection_owner``):
  the single product terminal, replacing the rejected
  ``filter_projection_publication_terminal.v1``.  Construction order is
  terminal core -> freshness/readiness refs -> terminal envelope:
  ``terminal_core_digest`` covers canonical fields 1-23 through
  ``terminal_generation`` (the acyclic core, excluding the nested refs and
  ``terminal_digest``), both nested refs carry that exact core digest, and
  ``terminal_digest`` is the final envelope digest over every preceding field
  including the nested refs.  Terminal rows are append-only: a later
  publication creates a new terminal with an exact predecessor digest and
  never rewrites historical freshness/readiness bytes;
- ``filter_projection_freshness_ref.v1`` and ``filter_projection_readiness_ref.v1``
  (owner ``serving_projection_owner``): immutable fresh/ready inputs nested in
  the terminal; a reader can never mint or repair them.  The readiness
  ``prerequisite_set_digest`` binds the exact eight-item prerequisite set;
  profile/card/search-index/facet diagnostics, watermarks, timestamps, and any
  persisted open readiness JSON are excluded diagnostics whose mutation does
  not alter the terminal/readiness ref;
- the four owner refs (owner ``projection_search_service``):
  ``filter_projection_success_owner_ref.v1``, ``filter_projection_stale_owner_ref.v1``,
  ``filter_projection_not_ready_owner_ref.v1``, and
  ``filter_projection_masked_absence_owner_ref.v1``; ``owner_ref_digest`` is
  the SHA-256 over the canonical complete owner ref except itself.  The masked
  absence ref carries no target id/workspace/existence/foreign owner bytes:
  the occurrence digest already binds the request without disclosing it anew;
- ``filter_projection_result_v3`` (owner ``filter_projection``): the model-safe
  result successor with exact union discrimination over the closed
  success/deferred/error roots; v2 is retained byte-for-byte.

``filter_projection_product_ref.v1`` is exposed as a REFERENCED contract: it is
not adopted or retyped here, but the V3 success root binds it as an immutable
exact-version-and-digest reference, so its schema and digest are pinned beside
the family to keep success-root validation closed.

Retained exactly as history and never retyped by this module:
``filter_projection_result_v2`` and ``filter_projection_tool_v2``.  V3
registration creates retained history only; the current tool alias remains V2
and the public/default served population remains zero.  Lookup is exact
version plus contract/result digest; shape inference, lexicographic latest,
mutable current alias, and auto-upgrade are forbidden.

Beyond closed shape and checksum validation, the terminal parser exact-compares
every field shared between the enclosing terminal and its nested
freshness/readiness refs (terminal id, route identity, membership revision,
candidate-set digest, counts, and the acyclic core digest), binds the complete
nested runtime namespace ref to the terminal's workspace and provider mode,
and requires ``row_ready_count == visible_member_count == candidate_count``
for ready records (enforced inside the readiness parser itself, so the
invariant is canonical for every consumer).  The V3 result parser enforces
the model-visible equations: the canonical
``cohort_selection_digest(cohort_selection)`` of the sibling closed selection
object (computed by the owning ``cohort_selection`` module, never retyped),
the exact registry pin, the manifest ``v3_page_equations``
(``returned_count == len(candidates) == min(limit, max(0, total_count -
offset))`` and ``truncated == (offset + returned_count < total_count)``) with
unique ``candidate_ref`` values within the returned page (the page is a
window of the unique member set M), freshness/readiness parity with the root
candidate-set digest and shared terminal core, the status-dependent
requested-target shape (stale-only terminal fields present exactly when
``status == "stale"``), the exact zero-member deferred tuple
(``projection_candidate_set_empty`` is exactly ``not_ready``,
``retryable=false``, ``reselection_required=true``), the exact lane-coverage
derivation table (``requested == completed + missing``; ``complete <=>
missing == 0``; ``partial <=> completed > 0 and missing > 0``;
``unavailable <=> completed == 0 and missing == requested``) with coverage
counts bound to unique per-lane summary evidence (one summary per requested
lane, counts derived from summary states), the decision-locked
``action_result_positive_value_roles_v3`` policy per pinned value role
(``identifier`` values are transport-neutral and separator-free,
``display_text`` carries non-whitespace content, ``web_url`` is an HTTPS
URL), and the canonical 64-KiB model-safety limit over exact canonical
UTF-8 bytes, so the schema preflight and the authoritative serializer accept
exactly the same values.

Decode boundary: public parsers accept raw JSON text/bytes (strictly decoded
by the shared decoder in ``agent_runtime_namespace_ref`` with duplicate-key
and non-finite rejection) or the decoder-produced ``CanonicalJsonObject`` —
never a plain dictionary.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Callable
from typing import Any

from .agent_runtime_namespace_ref import (
    AGENT_RUNTIME_NAMESPACE_REF_CONTRACT_DIGEST,
    AGENT_RUNTIME_NAMESPACE_REF_SCHEMA_VERSION,
    AgentRuntimeNamespaceRefError,
    CanonicalJsonObject,
    _canonical_json_object,
    parse_agent_runtime_namespace_ref,
    strict_json_loads,
)
from .cohort_selection import cohort_selection_digest, cohort_selection_registry_digest

RETAINED_FILTER_PROJECTION_HISTORY_LITERALS = (
    "filter_projection_result_v2",
    "filter_projection_tool_v2",
)
REJECTED_FILTER_PROJECTION_LITERALS = ("filter_projection_publication_terminal.v1",)
RETAINED_LOOKUP_FORBIDDEN = (
    "shape inference",
    "lexicographic latest",
    "mutable current alias",
    "auto-upgrade",
)

FILTER_PROJECTION_RESULT_V3_VARIANTS = ("success", "deferred", "error")
FILTER_PROJECTION_RESULT_V3_SUCCESS_ROOT_FIELDS = (
    "variant",
    "status",
    "projection_ref",
    "cohort_selection",
    "cohort_selection_registry_version",
    "cohort_selection_registry_digest",
    "cohort_selection_digest",
    "execution_commit_digest",
    "candidate_set_digest",
    "freshness",
    "readiness",
    "provider_mode",
    "runtime_namespace_ref",
    "requested_lane_coverage",
    "lane_summaries",
    "offset",
    "limit",
    "total_count",
    "returned_count",
    "truncated",
    "candidates",
)
FILTER_PROJECTION_RESULT_V3_DEFERRED_ROOT_FIELDS = (
    "variant",
    "status",
    "reason",
    "retryable",
    "reselection_required",
    "requested_target_ref",
    "decision_ref",
)
FILTER_PROJECTION_RESULT_V3_MASKED_ROOT_FIELDS = (
    "variant",
    "status",
    "reason",
    "retryable",
    "decision_ref",
)
FILTER_PROJECTION_RESULT_V3_MASKED_ROOT_CONSTANTS = {
    "variant": "error",
    "status": "failed",
    "reason": "projection_not_found",
    "retryable": False,
}
FILTER_PROJECTION_RESULT_V3_FIELD_VALUE_ROLES = {'deferred': {'/decision_ref': 'identifier',
              '/reason': 'control',
              '/requested_target_ref/membership_revision': 'identifier',
              '/requested_target_ref/projection_id': 'identifier',
              '/requested_target_ref/requested_terminal_digest': 'identifier',
              '/requested_target_ref/requested_terminal_id': 'identifier',
              '/requested_target_ref/route_revision_token': 'identifier',
              '/status': 'control',
              '/variant': 'control'},
 'error': {'/decision_ref': 'identifier', '/reason': 'control', '/status': 'control', '/variant': 'control'},
 'success': {'/candidate_set_digest': 'identifier',
             '/candidates/*/candidate_ref': 'identifier',
             '/candidates/*/display_name': 'display_text',
             '/candidates/*/employment_statuses/*': 'control',
             '/candidates/*/headline': 'display_text',
             '/candidates/*/public_profile_url': 'web_url',
             '/candidates/*/role_bucket_ids/*': 'control',
             '/cohort_selection/employment_statuses/*': 'identifier',
             '/cohort_selection/role_bucket_ids/*': 'identifier',
             '/cohort_selection/role_match': 'identifier',
             '/cohort_selection/schema_version': 'identifier',
             '/cohort_selection/source': 'identifier',
             '/cohort_selection_digest': 'identifier',
             '/cohort_selection_registry_digest': 'identifier',
             '/cohort_selection_registry_version': 'control',
             '/execution_commit_digest': 'identifier',
             '/freshness/candidate_set_digest': 'identifier',
             '/freshness/freshness_ref_digest': 'identifier',
             '/freshness/membership_revision': 'identifier',
             '/freshness/owner': 'control',
             '/freshness/product_terminal_id': 'identifier',
             '/freshness/route_key': 'identifier',
             '/freshness/route_kind': 'control',
             '/freshness/route_revision_token': 'identifier',
             '/freshness/schema_version': 'control',
             '/freshness/status': 'control',
             '/freshness/terminal_core_digest': 'identifier',
             '/lane_summaries/*/coverage_status': 'control',
             '/lane_summaries/*/employment_status': 'control',
             '/lane_summaries/*/lane_id': 'identifier',
             '/lane_summaries/*/role_bucket_id': 'control',
             '/projection_ref/membership_revision': 'identifier',
             '/projection_ref/projection_id': 'identifier',
             '/projection_ref/schema_version': 'control',
             '/projection_ref/terminal_digest': 'identifier',
             '/provider_mode': 'control',
             '/readiness/candidate_set_digest': 'identifier',
             '/readiness/membership_revision': 'identifier',
             '/readiness/owner': 'control',
             '/readiness/prerequisite_set_digest': 'identifier',
             '/readiness/product_terminal_id': 'identifier',
             '/readiness/projection_state': 'control',
             '/readiness/readiness_ref_digest': 'identifier',
             '/readiness/reason': 'control',
             '/readiness/schema_version': 'control',
             '/readiness/status': 'control',
             '/readiness/terminal_core_digest': 'identifier',
             '/requested_lane_coverage/status': 'control',
             '/runtime_namespace_ref/namespace_ref_id': 'identifier',
             '/runtime_namespace_ref/ref_digest': 'identifier',
             '/runtime_namespace_ref/schema_version': 'control',
             '/status': 'control',
             '/variant': 'control'}}


FILTER_PROJECTION_DEFERRED_REASON_PRECEDENCE = (
    "projection_candidate_set_empty",
    "projection_publication_pending",
    "projection_state_not_serving",
    "projection_membership_pending",
    "projection_members_not_ready",
)
FILTER_PROJECTION_READINESS_PREREQUISITE_SET = (
    "execution_lanes_complete",
    "candidate_set_nonempty",
    "candidate_set_exact",
    "projection_state_serving",
    "route_active",
    "membership_exact",
    "all_members_visible",
    "all_rows_ready",
)

# Referenced result-serializer pins bound by every owner ref; the serializer
# contract itself is owned by a later lane and is never retyped here.
FILTER_PROJECTION_RESULT_SERIALIZER_V3_OWNER_NAME = "projection_search_service.filter_projection_result_serializer_v3"
FILTER_PROJECTION_RESULT_SERIALIZER_V3_REVISION = "filter_projection_result_serializer_v3"
FILTER_PROJECTION_RESULT_SERIALIZER_V3_CONTRACT_DIGEST = "c0ab7e6620d05f8bf68d1c8601052e8f66b88d83d3f4263ff3b8b7b7f791c64c"

_SHA256_HEX_RE = re.compile(r"[0-9a-f]{64}")
_HTTPS_URL_RE = re.compile(r"^https://[^\s]+$")
# decision-locked action_result_positive_value_roles_v3 patterns
_URI_SCHEME_VALUE_RE = re.compile(r"(?<![A-Za-z0-9+.-])[A-Za-z][A-Za-z0-9+.-]*:[^\s]")
_DISPLAY_TEXT_RE = re.compile(r"(?s:.*\S.*)")

FILTER_PROJECTION_RESULT_V3_MAX_SERIALIZED_BYTES = 65536

# Ref resolution is lazy: populated with literal -> parse function after the
# parse functions below are defined.  An unresolvable ref fails closed.
_REF_PARSERS: dict[str, Callable[[Any], dict[str, Any]]] = {}


class FilterProjectionTerminalContractError(ValueError):
    """Raised when a filter-projection terminal contract value fails closed."""


def canonical_json(value: Any) -> str:
    """Manifest canonical JSON: sorted keys, compact separators, Unicode preserved, ``allow_nan=False``."""

    try:
        return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)
    except (TypeError, ValueError, OverflowError, UnicodeError) as exc:
        raise FilterProjectionTerminalContractError("filter projection terminal contract JSON invalid") from exc


def contract_digest(schema: dict[str, Any]) -> str:
    """Manifest-pinned digest equation: ``SHA256(UTF8(canonical_json(schema)))``."""

    return hashlib.sha256(canonical_json(schema).encode("utf-8")).hexdigest()


def _fail(label: str, reason: str) -> None:
    raise FilterProjectionTerminalContractError(f"filter projection terminal contract {label} invalid: {reason}")


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
        if descriptor.get("format") == "https_url" and not _HTTPS_URL_RE.fullmatch(value):
            _fail(label, "https_url format violation")
        # decision-locked action_result_positive_value_roles_v3 policy: the
        # schema preflight never approves values the authoritative serializer
        # rejects for the pinned role.
        value_role = descriptor.get("value_role")
        if value_role == "identifier":
            if (
                not value
                or value != value.strip()
                or any(character.isspace() for character in value)
                or "/" in value
                or "\\" in value
                or _URI_SCHEME_VALUE_RE.search(value) is not None
                or any(ord(character) < 0x20 or 0x7F <= ord(character) <= 0x9F for character in value)
            ):
                _fail(label, "identifier value-role violation")
        elif value_role == "display_text":
            if not _DISPLAY_TEXT_RE.search(value):
                _fail(label, "display_text must contain non-whitespace content")
        elif value_role == "web_url":
            if not _HTTPS_URL_RE.fullmatch(value):
                _fail(label, "web_url value-role violation")
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
        ref = descriptor.get("ref")
        if ref is not None:
            parser = _REF_PARSERS.get(ref)
            if parser is None:
                _fail(label, f"unresolvable ref {ref!r}")
            try:
                parser(value)
            except FilterProjectionTerminalContractError:
                raise
            except ValueError as exc:
                raise FilterProjectionTerminalContractError(
                    f"filter projection terminal contract {label} invalid: ref {ref!r} rejected the value"
                ) from exc
        else:
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


def _record_digest(record: Any, digest_field: str) -> str:
    """SHA-256 over the canonical record bytes with exactly ``digest_field`` excluded."""

    if not isinstance(record, dict):
        _fail(digest_field, "digest input must be an object")
    covered = {key: value for key, value in record.items() if key != digest_field}
    return hashlib.sha256(canonical_json(covered).encode("utf-8")).hexdigest()


FILTER_PROJECTION_PRODUCT_TERMINAL_V1_SCHEMA_VERSION = "filter_projection_product_terminal.v1"
FILTER_PROJECTION_PRODUCT_TERMINAL_V1_OWNER = "serving_projection_owner"


FILTER_PROJECTION_PRODUCT_TERMINAL_V1_ORDERED_FIELDS = ('schema_version',
 'owner',
 'product_terminal_id',
 'workspace_id',
 'acquisition_run_id',
 'operation_run_id',
 'workflow_run_id',
 'execution_commit_id',
 'execution_commit_digest',
 'projection_id',
 'projection_version',
 'membership_revision',
 'route_kind',
 'route_key',
 'route_revision_token',
 'provider_mode',
 'runtime_namespace_ref',
 'candidate_set_digest',
 'candidate_count',
 'visible_member_count',
 'excluded_member_count',
 'predecessor_terminal_digest',
 'terminal_generation',
 'terminal_core_digest',
 'freshness_ref',
 'readiness_ref',
 'terminal_digest')


FILTER_PROJECTION_PRODUCT_TERMINAL_V1_SCHEMA = {'schema_version': 'filter_projection_product_terminal.v1',
 'owner': 'serving_projection_owner',
 'fields': [{'name': 'schema_version',
             'type': 'string',
             'required': True,
             'constant': 'filter_projection_product_terminal.v1'},
            {'name': 'owner', 'type': 'string', 'required': True, 'constant': 'serving_projection_owner'},
            {'name': 'product_terminal_id', 'type': 'string', 'required': True, 'nonempty': True},
            {'name': 'workspace_id', 'type': 'string', 'required': True, 'nonempty': True},
            {'name': 'acquisition_run_id', 'type': 'string', 'required': True, 'nonempty': True},
            {'name': 'operation_run_id', 'type': 'string', 'required': True, 'nonempty': True},
            {'name': 'workflow_run_id', 'type': 'string', 'required': True, 'nonempty': True},
            {'name': 'execution_commit_id', 'type': 'string', 'required': True, 'nonempty': True},
            {'name': 'execution_commit_digest',
             'type': 'string',
             'required': True,
             'format': 'sha256_hex',
             'derivation': 'binds cohort_execution_commit.v1'},
            {'name': 'projection_id', 'type': 'string', 'required': True, 'nonempty': True},
            {'name': 'projection_version', 'type': 'integer', 'required': True, 'minimum': 1},
            {'name': 'membership_revision',
             'type': 'string',
             'required': True,
             'nonempty': True,
             'derivation': 'non-empty opaque equality token per docs/CANONICAL_SERVING_PROJECTION_CONTRACT.md; '
                           'equality/inequality only and never numerically, lexically, or chronologically ordered; no '
                           'positive/numeric bound applies'},
            {'name': 'route_kind',
             'type': 'string',
             'required': True,
             'enum': ['run_scope'],
             'derivation': 'collection-authoritative remains excluded in S1f0c'},
            {'name': 'route_key', 'type': 'string', 'required': True, 'nonempty': True},
            {'name': 'route_revision_token', 'type': 'string', 'required': True, 'nonempty': True},
            {'name': 'provider_mode',
             'type': 'string',
             'required': True,
             'enum': ['simulate', 'scripted'],
             'derivation': 'at this release boundary; live remains unauthorized'},
            {'name': 'runtime_namespace_ref',
             'type': 'object',
             'required': True,
             'ref': 'agent_runtime_namespace_ref.v1',
             'derivation': 'complete ref',
             'ref_digest': 'c5c52a1d0e411365d8a4cb3b3f2b9bcfcafed3763cad0ea87c51599c19f0dada'},
            {'name': 'candidate_set_digest',
             'type': 'string',
             'required': True,
             'format': 'sha256_hex',
             'derivation': 'binds cohort_candidate_set.v1'},
            {'name': 'candidate_count', 'type': 'integer', 'required': True, 'minimum': 1, 'maximum': 1000},
            {'name': 'visible_member_count',
             'type': 'integer',
             'required': True,
             'minimum': 1,
             'maximum': 1000,
             'derivation': '== candidate_count'},
            {'name': 'excluded_member_count', 'type': 'integer', 'required': True, 'constant': 0},
            {'name': 'predecessor_terminal_digest',
             'type': 'string',
             'required': False,
             'format': 'sha256_hex',
             'derivation': 'absent for the first terminal of a projection; otherwise the exact predecessor '
                           'terminal_digest; empty-string alias forbidden'},
            {'name': 'terminal_generation',
             'type': 'integer',
             'required': True,
             'minimum': 1,
             'derivation': 'positive exact integer'},
            {'name': 'terminal_core_digest',
             'type': 'string',
             'required': True,
             'format': 'sha256_hex',
             'derivation': 'SHA256 over canonical fields 1-23 through terminal_generation; excludes the nested '
                           'freshness/readiness refs and terminal_digest; normative acyclic core per '
                           'digest_dependency_dag'},
            {'name': 'freshness_ref',
             'type': 'object',
             'required': True,
             'ref': 'filter_projection_freshness_ref.v1',
             'ref_digest': 'baecfa7d80bde58a38d33f9bc701c639a6910b3c9a4d20d1b142d2c412fa40a5'},
            {'name': 'readiness_ref',
             'type': 'object',
             'required': True,
             'ref': 'filter_projection_readiness_ref.v1',
             'ref_digest': '1b555d9aea2c1f151a3488bdad12f7acad3d8daf70c6353214e1b23f6c98e961'},
            {'name': 'terminal_digest',
             'type': 'string',
             'required': True,
             'format': 'sha256_hex',
             'derivation': 'final envelope digest over every preceding field including the nested freshness/readiness '
                           'refs'}]}


FILTER_PROJECTION_PRODUCT_TERMINAL_V1_CONTRACT_DIGEST = contract_digest(FILTER_PROJECTION_PRODUCT_TERMINAL_V1_SCHEMA)


FILTER_PROJECTION_FRESHNESS_REF_V1_SCHEMA_VERSION = "filter_projection_freshness_ref.v1"
FILTER_PROJECTION_FRESHNESS_REF_V1_OWNER = "serving_projection_owner"


FILTER_PROJECTION_FRESHNESS_REF_V1_ORDERED_FIELDS = ('schema_version',
 'owner',
 'status',
 'product_terminal_id',
 'terminal_core_digest',
 'route_kind',
 'route_key',
 'route_revision_token',
 'membership_revision',
 'candidate_set_digest',
 'freshness_ref_digest')


FILTER_PROJECTION_FRESHNESS_REF_V1_SCHEMA = {'schema_version': 'filter_projection_freshness_ref.v1',
 'owner': 'serving_projection_owner',
 'fields': [{'name': 'schema_version',
             'type': 'string',
             'required': True,
             'constant': 'filter_projection_freshness_ref.v1'},
            {'name': 'owner', 'type': 'string', 'required': True, 'constant': 'serving_projection_owner'},
            {'name': 'status', 'type': 'string', 'required': True, 'constant': 'fresh'},
            {'name': 'product_terminal_id', 'type': 'string', 'required': True, 'nonempty': True},
            {'name': 'terminal_core_digest',
             'type': 'string',
             'required': True,
             'format': 'sha256_hex',
             'derivation': 'exact copy of the enclosing terminal core digest; the ref derives from the core, never '
                           'from the envelope terminal_digest; normative anti-cycle per digest_dependency_dag'},
            {'name': 'route_kind',
             'type': 'string',
             'required': True,
             'enum': ['run_scope'],
             'derivation': 'collection-authoritative remains excluded in S1f0c'},
            {'name': 'route_key', 'type': 'string', 'required': True, 'nonempty': True},
            {'name': 'route_revision_token', 'type': 'string', 'required': True, 'nonempty': True},
            {'name': 'membership_revision',
             'type': 'string',
             'required': True,
             'nonempty': True,
             'derivation': 'non-empty opaque equality token per docs/CANONICAL_SERVING_PROJECTION_CONTRACT.md; '
                           'equality/inequality only and never numerically, lexically, or chronologically ordered; no '
                           'positive/numeric bound applies'},
            {'name': 'candidate_set_digest',
             'type': 'string',
             'required': True,
             'format': 'sha256_hex',
             'derivation': 'binds cohort_candidate_set.v1'},
            {'name': 'freshness_ref_digest',
             'type': 'string',
             'required': True,
             'format': 'sha256_hex',
             'derivation': 'covers all preceding fields'}]}


FILTER_PROJECTION_FRESHNESS_REF_V1_CONTRACT_DIGEST = contract_digest(FILTER_PROJECTION_FRESHNESS_REF_V1_SCHEMA)


FILTER_PROJECTION_READINESS_REF_V1_SCHEMA_VERSION = "filter_projection_readiness_ref.v1"
FILTER_PROJECTION_READINESS_REF_V1_OWNER = "serving_projection_owner"


FILTER_PROJECTION_READINESS_REF_V1_ORDERED_FIELDS = ('schema_version',
 'owner',
 'status',
 'reason',
 'product_terminal_id',
 'terminal_core_digest',
 'projection_state',
 'membership_revision',
 'candidate_set_digest',
 'candidate_count',
 'visible_member_count',
 'row_ready_count',
 'prerequisite_set_digest',
 'readiness_ref_digest')


FILTER_PROJECTION_READINESS_REF_V1_SCHEMA = {'schema_version': 'filter_projection_readiness_ref.v1',
 'owner': 'serving_projection_owner',
 'fields': [{'name': 'schema_version',
             'type': 'string',
             'required': True,
             'constant': 'filter_projection_readiness_ref.v1'},
            {'name': 'owner', 'type': 'string', 'required': True, 'constant': 'serving_projection_owner'},
            {'name': 'status', 'type': 'string', 'required': True, 'constant': 'ready'},
            {'name': 'reason', 'type': 'string', 'required': True, 'constant': ''},
            {'name': 'product_terminal_id', 'type': 'string', 'required': True, 'nonempty': True},
            {'name': 'terminal_core_digest',
             'type': 'string',
             'required': True,
             'format': 'sha256_hex',
             'derivation': 'exact copy of the enclosing terminal core digest; the ref derives from the core, never '
                           'from the envelope terminal_digest; normative anti-cycle per digest_dependency_dag'},
            {'name': 'projection_state', 'type': 'string', 'required': True, 'constant': 'serving'},
            {'name': 'membership_revision',
             'type': 'string',
             'required': True,
             'nonempty': True,
             'derivation': 'non-empty opaque equality token per docs/CANONICAL_SERVING_PROJECTION_CONTRACT.md; '
                           'equality/inequality only and never numerically, lexically, or chronologically ordered; no '
                           'positive/numeric bound applies'},
            {'name': 'candidate_set_digest',
             'type': 'string',
             'required': True,
             'format': 'sha256_hex',
             'derivation': 'binds cohort_candidate_set.v1'},
            {'name': 'candidate_count', 'type': 'integer', 'required': True, 'minimum': 1, 'maximum': 1000},
            {'name': 'visible_member_count', 'type': 'integer', 'required': True, 'minimum': 1, 'maximum': 1000},
            {'name': 'row_ready_count',
             'type': 'integer',
             'required': True,
             'minimum': 0,
             'derivation': 'count of ready member rows; equals visible_member_count for status=ready'},
            {'name': 'prerequisite_set_digest',
             'type': 'string',
             'required': True,
             'format': 'sha256_hex',
             'derivation': 'SHA256 over canonical_json of the exact eight-item prerequisite set '
                           '[execution_lanes_complete, candidate_set_nonempty, candidate_set_exact, '
                           'projection_state_serving, route_active, membership_exact, all_members_visible, '
                           'all_rows_ready] pinned at product_terminal_records.readiness_ref.prerequisite_set'},
            {'name': 'readiness_ref_digest',
             'type': 'string',
             'required': True,
             'format': 'sha256_hex',
             'derivation': 'covers all preceding fields'}]}


FILTER_PROJECTION_READINESS_REF_V1_CONTRACT_DIGEST = contract_digest(FILTER_PROJECTION_READINESS_REF_V1_SCHEMA)


FILTER_PROJECTION_SUCCESS_OWNER_REF_V1_SCHEMA_VERSION = "filter_projection_success_owner_ref.v1"
FILTER_PROJECTION_SUCCESS_OWNER_REF_V1_OWNER = "projection_search_service"


FILTER_PROJECTION_SUCCESS_OWNER_REF_V1_ORDERED_FIELDS = ('schema_version',
 'logical_occurrence_digest',
 'result_slot_id',
 'slot_generation',
 'product_terminal_id',
 'terminal_digest',
 'terminal_generation',
 'projection_id',
 'membership_revision',
 'execution_commit_digest',
 'candidate_set_digest',
 'freshness_ref_digest',
 'readiness_ref_digest',
 'result_schema_version',
 'result_contract_digest',
 'serializer_owner_name',
 'serializer_revision',
 'serializer_contract_digest',
 'owner_ref_digest')


FILTER_PROJECTION_SUCCESS_OWNER_REF_V1_SCHEMA = {'schema_version': 'filter_projection_success_owner_ref.v1',
 'owner': 'projection_search_service',
 'fields': [{'name': 'schema_version',
             'type': 'string',
             'required': True,
             'constant': 'filter_projection_success_owner_ref.v1'},
            {'name': 'logical_occurrence_digest',
             'type': 'string',
             'required': True,
             'format': 'sha256_hex',
             'derivation': 'occurrence-bound request identity'},
            {'name': 'result_slot_id', 'type': 'string', 'required': True, 'nonempty': True},
            {'name': 'slot_generation', 'type': 'integer', 'required': True, 'minimum': 1},
            {'name': 'product_terminal_id', 'type': 'string', 'required': True, 'nonempty': True},
            {'name': 'terminal_digest',
             'type': 'string',
             'required': True,
             'format': 'sha256_hex',
             'derivation': 'terminal envelope digest'},
            {'name': 'terminal_generation', 'type': 'integer', 'required': True, 'minimum': 1},
            {'name': 'projection_id', 'type': 'string', 'required': True, 'nonempty': True},
            {'name': 'membership_revision',
             'type': 'string',
             'required': True,
             'nonempty': True,
             'derivation': 'non-empty opaque equality token per docs/CANONICAL_SERVING_PROJECTION_CONTRACT.md; '
                           'equality/inequality only and never numerically, lexically, or chronologically ordered; no '
                           'positive/numeric bound applies'},
            {'name': 'execution_commit_digest',
             'type': 'string',
             'required': True,
             'format': 'sha256_hex',
             'derivation': 'binds cohort_execution_commit.v1'},
            {'name': 'candidate_set_digest',
             'type': 'string',
             'required': True,
             'format': 'sha256_hex',
             'derivation': 'binds cohort_candidate_set.v1'},
            {'name': 'freshness_ref_digest',
             'type': 'string',
             'required': True,
             'format': 'sha256_hex',
             'derivation': 'binds filter_projection_freshness_ref.v1'},
            {'name': 'readiness_ref_digest',
             'type': 'string',
             'required': True,
             'format': 'sha256_hex',
             'derivation': 'binds filter_projection_readiness_ref.v1'},
            {'name': 'result_schema_version',
             'type': 'string',
             'required': True,
             'constant': 'filter_projection_result_v3'},
            {'name': 'result_contract_digest',
             'type': 'string',
             'required': True,
             'format': 'sha256_hex',
             'derivation': 'equals the filter_projection_result_v3 contract_digest'},
            {'name': 'serializer_owner_name',
             'type': 'string',
             'required': True,
             'constant': 'projection_search_service.filter_projection_result_serializer_v3'},
            {'name': 'serializer_revision',
             'type': 'string',
             'required': True,
             'constant': 'filter_projection_result_serializer_v3'},
            {'name': 'serializer_contract_digest',
             'type': 'string',
             'required': True,
             'format': 'sha256_hex',
             'derivation': 'equals the filter_projection_result_serializer_v3 contract_digest'},
            {'name': 'owner_ref_digest',
             'type': 'string',
             'required': True,
             'format': 'sha256_hex',
             'derivation': 'SHA256 canonical complete owner ref except owner_ref_digest'}]}


FILTER_PROJECTION_SUCCESS_OWNER_REF_V1_CONTRACT_DIGEST = contract_digest(FILTER_PROJECTION_SUCCESS_OWNER_REF_V1_SCHEMA)


FILTER_PROJECTION_STALE_OWNER_REF_V1_SCHEMA_VERSION = "filter_projection_stale_owner_ref.v1"
FILTER_PROJECTION_STALE_OWNER_REF_V1_OWNER = "projection_search_service"


FILTER_PROJECTION_STALE_OWNER_REF_V1_ORDERED_FIELDS = ('schema_version',
 'logical_occurrence_digest',
 'result_slot_id',
 'slot_generation',
 'requested_target_digest',
 'requested_terminal_id',
 'requested_terminal_digest',
 'successor_terminal_id',
 'successor_terminal_digest',
 'route_revision_token',
 'result_schema_version',
 'result_contract_digest',
 'serializer_owner_name',
 'serializer_revision',
 'serializer_contract_digest',
 'owner_ref_digest')


FILTER_PROJECTION_STALE_OWNER_REF_V1_SCHEMA = {'schema_version': 'filter_projection_stale_owner_ref.v1',
 'owner': 'projection_search_service',
 'fields': [{'name': 'schema_version',
             'type': 'string',
             'required': True,
             'constant': 'filter_projection_stale_owner_ref.v1'},
            {'name': 'logical_occurrence_digest',
             'type': 'string',
             'required': True,
             'format': 'sha256_hex',
             'derivation': 'occurrence-bound request identity'},
            {'name': 'result_slot_id', 'type': 'string', 'required': True, 'nonempty': True},
            {'name': 'slot_generation', 'type': 'integer', 'required': True, 'minimum': 1},
            {'name': 'requested_target_digest',
             'type': 'string',
             'required': True,
             'format': 'sha256_hex',
             'derivation': 'complete requested-target digest'},
            {'name': 'requested_terminal_id', 'type': 'string', 'required': True, 'nonempty': True},
            {'name': 'requested_terminal_digest',
             'type': 'string',
             'required': True,
             'format': 'sha256_hex',
             'derivation': 'requested retained terminal envelope digest'},
            {'name': 'successor_terminal_id', 'type': 'string', 'required': True, 'nonempty': True},
            {'name': 'successor_terminal_digest',
             'type': 'string',
             'required': True,
             'format': 'sha256_hex',
             'derivation': 'exact successor terminal envelope digest'},
            {'name': 'route_revision_token', 'type': 'string', 'required': True, 'nonempty': True},
            {'name': 'result_schema_version',
             'type': 'string',
             'required': True,
             'constant': 'filter_projection_result_v3'},
            {'name': 'result_contract_digest',
             'type': 'string',
             'required': True,
             'format': 'sha256_hex',
             'derivation': 'equals the filter_projection_result_v3 contract_digest'},
            {'name': 'serializer_owner_name',
             'type': 'string',
             'required': True,
             'constant': 'projection_search_service.filter_projection_result_serializer_v3'},
            {'name': 'serializer_revision',
             'type': 'string',
             'required': True,
             'constant': 'filter_projection_result_serializer_v3'},
            {'name': 'serializer_contract_digest',
             'type': 'string',
             'required': True,
             'format': 'sha256_hex',
             'derivation': 'equals the filter_projection_result_serializer_v3 contract_digest'},
            {'name': 'owner_ref_digest',
             'type': 'string',
             'required': True,
             'format': 'sha256_hex',
             'derivation': 'SHA256 canonical complete owner ref except owner_ref_digest'}]}


FILTER_PROJECTION_STALE_OWNER_REF_V1_CONTRACT_DIGEST = contract_digest(FILTER_PROJECTION_STALE_OWNER_REF_V1_SCHEMA)


FILTER_PROJECTION_NOT_READY_OWNER_REF_V1_SCHEMA_VERSION = "filter_projection_not_ready_owner_ref.v1"
FILTER_PROJECTION_NOT_READY_OWNER_REF_V1_OWNER = "projection_search_service"


FILTER_PROJECTION_NOT_READY_OWNER_REF_V1_ORDERED_FIELDS = ('schema_version',
 'logical_occurrence_digest',
 'result_slot_id',
 'slot_generation',
 'requested_target_digest',
 'projection_id',
 'projection_version',
 'projection_state',
 'membership_revision',
 'route_digest',
 'reason',
 'retryable',
 'reselection_required',
 'result_schema_version',
 'result_contract_digest',
 'serializer_owner_name',
 'serializer_revision',
 'serializer_contract_digest',
 'owner_ref_digest')


FILTER_PROJECTION_NOT_READY_OWNER_REF_V1_SCHEMA = {'schema_version': 'filter_projection_not_ready_owner_ref.v1',
 'owner': 'projection_search_service',
 'fields': [{'name': 'schema_version',
             'type': 'string',
             'required': True,
             'constant': 'filter_projection_not_ready_owner_ref.v1'},
            {'name': 'logical_occurrence_digest',
             'type': 'string',
             'required': True,
             'format': 'sha256_hex',
             'derivation': 'occurrence-bound request identity'},
            {'name': 'result_slot_id', 'type': 'string', 'required': True, 'nonempty': True},
            {'name': 'slot_generation', 'type': 'integer', 'required': True, 'minimum': 1},
            {'name': 'requested_target_digest',
             'type': 'string',
             'required': True,
             'format': 'sha256_hex',
             'derivation': 'complete requested-target digest'},
            {'name': 'projection_id', 'type': 'string', 'required': True, 'nonempty': True},
            {'name': 'projection_version', 'type': 'integer', 'required': True, 'minimum': 1},
            {'name': 'projection_state',
             'type': 'string',
             'required': True,
             'derivation': 'locked exact projection state'},
            {'name': 'membership_revision',
             'type': 'string',
             'required': True,
             'nonempty': True,
             'derivation': 'non-empty opaque equality token per docs/CANONICAL_SERVING_PROJECTION_CONTRACT.md; '
                           'equality/inequality only and never numerically, lexically, or chronologically ordered; no '
                           'positive/numeric bound applies'},
            {'name': 'route_digest',
             'type': 'string',
             'required': True,
             'format': 'sha256_hex',
             'derivation': 'locked route digest'},
            {'name': 'reason',
             'type': 'string',
             'required': True,
             'enum': ['projection_candidate_set_empty',
                      'projection_publication_pending',
                      'projection_state_not_serving',
                      'projection_membership_pending',
                      'projection_members_not_ready'],
             'derivation': 'exact closed deferred reason; missing and foreign never enter this precedence'},
            {'name': 'retryable', 'type': 'boolean', 'required': True},
            {'name': 'reselection_required', 'type': 'boolean', 'required': True},
            {'name': 'result_schema_version',
             'type': 'string',
             'required': True,
             'constant': 'filter_projection_result_v3'},
            {'name': 'result_contract_digest',
             'type': 'string',
             'required': True,
             'format': 'sha256_hex',
             'derivation': 'equals the filter_projection_result_v3 contract_digest'},
            {'name': 'serializer_owner_name',
             'type': 'string',
             'required': True,
             'constant': 'projection_search_service.filter_projection_result_serializer_v3'},
            {'name': 'serializer_revision',
             'type': 'string',
             'required': True,
             'constant': 'filter_projection_result_serializer_v3'},
            {'name': 'serializer_contract_digest',
             'type': 'string',
             'required': True,
             'format': 'sha256_hex',
             'derivation': 'equals the filter_projection_result_serializer_v3 contract_digest'},
            {'name': 'owner_ref_digest',
             'type': 'string',
             'required': True,
             'format': 'sha256_hex',
             'derivation': 'SHA256 canonical complete owner ref except owner_ref_digest'}]}


FILTER_PROJECTION_NOT_READY_OWNER_REF_V1_CONTRACT_DIGEST = contract_digest(FILTER_PROJECTION_NOT_READY_OWNER_REF_V1_SCHEMA)


FILTER_PROJECTION_MASKED_ABSENCE_OWNER_REF_V1_SCHEMA_VERSION = "filter_projection_masked_absence_owner_ref.v1"
FILTER_PROJECTION_MASKED_ABSENCE_OWNER_REF_V1_OWNER = "projection_search_service"


FILTER_PROJECTION_MASKED_ABSENCE_OWNER_REF_V1_ORDERED_FIELDS = ('schema_version',
 'logical_occurrence_digest',
 'result_slot_id',
 'slot_generation',
 'reason',
 'result_schema_version',
 'result_contract_digest',
 'serializer_owner_name',
 'serializer_revision',
 'serializer_contract_digest',
 'owner_ref_digest')


FILTER_PROJECTION_MASKED_ABSENCE_OWNER_REF_V1_SCHEMA = {'schema_version': 'filter_projection_masked_absence_owner_ref.v1',
 'owner': 'projection_search_service',
 'fields': [{'name': 'schema_version',
             'type': 'string',
             'required': True,
             'constant': 'filter_projection_masked_absence_owner_ref.v1'},
            {'name': 'logical_occurrence_digest',
             'type': 'string',
             'required': True,
             'format': 'sha256_hex',
             'derivation': 'occurrence-bound request identity'},
            {'name': 'result_slot_id', 'type': 'string', 'required': True, 'nonempty': True},
            {'name': 'slot_generation', 'type': 'integer', 'required': True, 'minimum': 1},
            {'name': 'reason',
             'type': 'string',
             'required': True,
             'constant': 'projection_not_found',
             'derivation': 'public reason; no target id/workspace/existence/foreign owner bytes'},
            {'name': 'result_schema_version',
             'type': 'string',
             'required': True,
             'constant': 'filter_projection_result_v3'},
            {'name': 'result_contract_digest',
             'type': 'string',
             'required': True,
             'format': 'sha256_hex',
             'derivation': 'equals the filter_projection_result_v3 contract_digest'},
            {'name': 'serializer_owner_name',
             'type': 'string',
             'required': True,
             'constant': 'projection_search_service.filter_projection_result_serializer_v3'},
            {'name': 'serializer_revision',
             'type': 'string',
             'required': True,
             'constant': 'filter_projection_result_serializer_v3'},
            {'name': 'serializer_contract_digest',
             'type': 'string',
             'required': True,
             'format': 'sha256_hex',
             'derivation': 'equals the filter_projection_result_serializer_v3 contract_digest'},
            {'name': 'owner_ref_digest',
             'type': 'string',
             'required': True,
             'format': 'sha256_hex',
             'derivation': 'SHA256 canonical complete owner ref except owner_ref_digest'}]}


FILTER_PROJECTION_MASKED_ABSENCE_OWNER_REF_V1_CONTRACT_DIGEST = contract_digest(FILTER_PROJECTION_MASKED_ABSENCE_OWNER_REF_V1_SCHEMA)


FILTER_PROJECTION_RESULT_V3_SCHEMA_VERSION = "filter_projection_result_v3"
FILTER_PROJECTION_RESULT_V3_OWNER = "filter_projection"


FILTER_PROJECTION_RESULT_V3_ORDERED_FIELDS = ('variant',
 'status',
 'projection_ref',
 'cohort_selection',
 'cohort_selection_registry_version',
 'cohort_selection_registry_digest',
 'cohort_selection_digest',
 'execution_commit_digest',
 'candidate_set_digest',
 'freshness',
 'readiness',
 'provider_mode',
 'runtime_namespace_ref',
 'requested_lane_coverage',
 'lane_summaries',
 'offset',
 'limit',
 'total_count',
 'returned_count',
 'truncated',
 'candidates',
 'reason',
 'retryable',
 'reselection_required',
 'requested_target_ref',
 'decision_ref')


FILTER_PROJECTION_RESULT_V3_SCHEMA = {'schema_version': 'filter_projection_result_v3',
 'owner': 'filter_projection',
 'fields': [{'name': 'variant',
             'type': 'string',
             'required': True,
             'constant': 'success',
             'variants': ['success'],
             'derivation': 'exact success root discriminator; union discrimination is exact per variant; at least as '
                           'strict as v2',
             'provenance': 'server_derived',
             'value_role': 'control'},
            {'name': 'variant',
             'type': 'string',
             'required': True,
             'constant': 'deferred',
             'variants': ['deferred'],
             'provenance': 'server_derived',
             'value_role': 'control'},
            {'name': 'variant',
             'type': 'string',
             'required': True,
             'constant': 'error',
             'variants': ['error'],
             'provenance': 'server_derived',
             'value_role': 'control'},
            {'name': 'status',
             'type': 'string',
             'required': True,
             'constant': 'ready',
             'variants': ['success'],
             'derivation': 'exact v2 success status constant retained',
             'provenance': 'server_derived',
             'value_role': 'control'},
            {'name': 'status',
             'type': 'string',
             'required': True,
             'enum': ['stale', 'not_ready'],
             'variants': ['deferred'],
             'derivation': 'exact v2 deferred status enum retained',
             'provenance': 'server_derived',
             'value_role': 'control'},
            {'name': 'status',
             'type': 'string',
             'required': True,
             'constant': 'failed',
             'variants': ['error'],
             'provenance': 'server_derived',
             'value_role': 'control'},
            {'name': 'projection_ref',
             'type': 'object',
             'required': True,
             'ref': 'filter_projection_product_ref.v1',
             'variants': ['success'],
             'provenance': 'owner_state',
             'ref_digest': '58a18ad0cc558ae7ff65a0638de79603b8179462617dee246d96cea93c905f8b'},
            {'name': 'cohort_selection',
             'type': 'object',
             'required': True,
             'fields': [{'name': 'schema_version',
                         'type': 'string',
                         'required': True,
                         'constant': 'cohort_selection.v1',
                         'provenance': 'user_supplied',
                         'value_role': 'control'},
                        {'name': 'role_bucket_ids',
                         'type': 'array',
                         'required': True,
                         'items': {'type': 'string',
                                   'enum': ['research',
                                            'engineering',
                                            'product_management',
                                            'infra_systems',
                                            'founding'],
                                   'provenance': 'user_supplied',
                                   'value_role': 'control'},
                         'max_items': 5,
                         'provenance': 'user_supplied'},
                        {'name': 'employment_statuses',
                         'type': 'array',
                         'required': True,
                         'items': {'type': 'string',
                                   'enum': ['current', 'former'],
                                   'provenance': 'user_supplied',
                                   'value_role': 'control'},
                         'min_items': 1,
                         'max_items': 2,
                         'provenance': 'user_supplied'},
                        {'name': 'role_match',
                         'type': 'string',
                         'required': True,
                         'enum': ['any', 'all'],
                         'provenance': 'user_supplied',
                         'value_role': 'control'},
                        {'name': 'source',
                         'type': 'string',
                         'required': True,
                         'constant': 'user_explicit',
                         'provenance': 'user_supplied',
                         'value_role': 'control'}],
             'variants': ['success'],
             'derivation': 'exact closed Cohort selection object retained from '
                           'agent_projection_query._cohort_selection_schema (schema_version, role_bucket_ids, '
                           'employment_statuses, role_match, source; additionalProperties closed); the sibling '
                           'cohort_selection_digest equals cohort_selection_digest(cohort_selection), the SHA256 over '
                           'canonical_json of the registry-pinned selection record, so the model-visible selection and '
                           'the execution commitment can never diverge',
             'provenance': 'user_supplied'},
            {'name': 'cohort_selection_registry_version',
             'type': 'string',
             'required': True,
             'constant': 'cohort_selection.registry.v1',
             'variants': ['success'],
             'derivation': 'exact COHORT_SELECTION_REGISTRY_VERSION constant; at least as strict as v2',
             'provenance': 'owner_state',
             'value_role': 'control'},
            {'name': 'cohort_selection_registry_digest',
             'type': 'string',
             'required': True,
             'format': 'sha256_hex',
             'derivation': 'exact pinned cohort selection registry',
             'variants': ['success'],
             'provenance': 'owner_state',
             'value_role': 'identifier'},
            {'name': 'cohort_selection_digest',
             'type': 'string',
             'required': True,
             'format': 'sha256_hex',
             'derivation': 'exact pinned cohort selection: equals cohort_selection_digest(cohort_selection) computed '
                           'over the sibling closed cohort_selection object; never an unbound string',
             'variants': ['success'],
             'provenance': 'owner_state',
             'value_role': 'identifier'},
            {'name': 'execution_commit_digest',
             'type': 'string',
             'required': True,
             'format': 'sha256_hex',
             'derivation': 'binds cohort_execution_commit.v1',
             'variants': ['success'],
             'provenance': 'owner_state',
             'value_role': 'identifier'},
            {'name': 'candidate_set_digest',
             'type': 'string',
             'required': True,
             'format': 'sha256_hex',
             'derivation': 'binds cohort_candidate_set.v1',
             'variants': ['success'],
             'provenance': 'owner_state',
             'value_role': 'identifier'},
            {'name': 'freshness',
             'type': 'object',
             'required': True,
             'ref': 'filter_projection_freshness_ref.v1',
             'variants': ['success'],
             'provenance': 'owner_state',
             'ref_digest': 'baecfa7d80bde58a38d33f9bc701c639a6910b3c9a4d20d1b142d2c412fa40a5'},
            {'name': 'readiness',
             'type': 'object',
             'required': True,
             'ref': 'filter_projection_readiness_ref.v1',
             'variants': ['success'],
             'provenance': 'owner_state',
             'ref_digest': '1b555d9aea2c1f151a3488bdad12f7acad3d8daf70c6353214e1b23f6c98e961'},
            {'name': 'provider_mode',
             'type': 'string',
             'required': True,
             'enum': ['simulate', 'scripted'],
             'variants': ['success'],
             'derivation': 'at this release boundary; live remains unauthorized',
             'provenance': 'owner_state',
             'value_role': 'control'},
            {'name': 'runtime_namespace_ref',
             'type': 'object',
             'required': True,
             'fields': [{'name': 'schema_version',
                         'type': 'string',
                         'required': True,
                         'constant': 'agent_runtime_namespace_ref.v1',
                         'provenance': 'owner_state',
                         'value_role': 'control'},
                        {'name': 'namespace_ref_id',
                         'type': 'string',
                         'required': True,
                         'nonempty': True,
                         'provenance': 'owner_state',
                         'value_role': 'identifier'},
                        {'name': 'ref_digest',
                         'type': 'string',
                         'required': True,
                         'format': 'sha256_hex',
                         'derivation': 'binds agent_runtime_namespace_ref.v1',
                         'provenance': 'owner_state',
                         'value_role': 'identifier'}],
             'variants': ['success'],
             'derivation': 'public subset only; no path/workspace/private identity',
             'provenance': 'owner_state'},
            {'name': 'requested_lane_coverage',
             'type': 'object',
             'required': True,
             'fields': [{'name': 'status',
                         'type': 'string',
                         'required': True,
                         'enum': ['complete', 'partial', 'unavailable'],
                         'provenance': 'owner_state',
                         'value_role': 'control'},
                        {'name': 'requested_lane_count',
                         'type': 'integer',
                         'required': True,
                         'minimum': 1,
                         'maximum': 64,
                         'provenance': 'owner_state'},
                        {'name': 'completed_lane_count',
                         'type': 'integer',
                         'required': True,
                         'minimum': 0,
                         'maximum': 64,
                         'provenance': 'owner_state'},
                        {'name': 'missing_lane_count',
                         'type': 'integer',
                         'required': True,
                         'minimum': 0,
                         'maximum': 64,
                         'provenance': 'owner_state'}],
             'variants': ['success'],
             'derivation': 'exact v2 closed coverage summary; at least as strict as v2',
             'provenance': 'owner_state'},
            {'name': 'lane_summaries',
             'type': 'array',
             'required': True,
             'items': {'type': 'object',
                       'required': True,
                       'fields': [{'name': 'lane_id',
                                   'type': 'string',
                                   'required': True,
                                   'nonempty': True,
                                   'max_length': 200,
                                   'derivation': 'exact v2 lane identifier; at least as strict as v2',
                                   'provenance': 'owner_state',
                                   'value_role': 'identifier'},
                                  {'name': 'employment_status',
                                   'type': 'string',
                                   'required': True,
                                   'enum': ['current', 'former'],
                                   'provenance': 'owner_state',
                                   'value_role': 'control'},
                                  {'name': 'role_bucket_id',
                                   'type': 'string',
                                   'required': True,
                                   'enum': ['all_roles',
                                            'research',
                                            'engineering',
                                            'product_management',
                                            'infra_systems',
                                            'founding'],
                                   'provenance': 'owner_state',
                                   'value_role': 'control'},
                                  {'name': 'coverage_status',
                                   'type': 'string',
                                   'required': True,
                                   'enum': ['complete', 'partial', 'missing'],
                                   'provenance': 'owner_state',
                                   'value_role': 'control'},
                                  {'name': 'result_count',
                                   'type': 'integer',
                                   'required': True,
                                   'minimum': 0,
                                   'maximum': 1000000,
                                   'provenance': 'owner_state'}],
                       'provenance': 'owner_state'},
             'max_items': 64,
             'variants': ['success'],
             'derivation': 'per-lane exact summaries; closed v2 lane-summary item schema; max_items=64 equals '
                           'FILTER_PROJECTION_MAX_LANE_SUMMARIES; at least as strict as v2',
             'provenance': 'owner_state'},
            {'name': 'offset',
             'type': 'integer',
             'required': True,
             'minimum': 0,
             'maximum': 100000,
             'variants': ['success'],
             'derivation': 'paging bound equals FILTER_PROJECTION_MAX_OFFSET; at least as strict as v2',
             'provenance': 'owner_state'},
            {'name': 'limit',
             'type': 'integer',
             'required': True,
             'minimum': 1,
             'maximum': 250,
             'variants': ['success'],
             'derivation': 'paging bound equals FILTER_PROJECTION_MAX_LIMIT; at least as strict as v2',
             'provenance': 'owner_state'},
            {'name': 'total_count',
             'type': 'integer',
             'required': True,
             'minimum': 0,
             'maximum': 1000,
             'variants': ['success'],
             'derivation': 'count bound equals FILTER_PROJECTION_MAX_CANDIDATES; at least as strict as v2',
             'provenance': 'owner_state'},
            {'name': 'returned_count',
             'type': 'integer',
             'required': True,
             'minimum': 0,
             'maximum': 250,
             'variants': ['success'],
             'derivation': 'paging bound equals FILTER_PROJECTION_MAX_LIMIT; at least as strict as v2',
             'provenance': 'owner_state'},
            {'name': 'truncated',
             'type': 'boolean',
             'required': True,
             'variants': ['success'],
             'provenance': 'owner_state'},
            {'name': 'candidates',
             'type': 'array',
             'required': True,
             'items': {'type': 'object',
                       'required': True,
                       'fields': [{'name': 'candidate_ref',
                                   'type': 'string',
                                   'required': True,
                                   'format': 'sha256_hex',
                                   'derivation': 'revision-bound bounded SHA-256 candidate ref',
                                   'provenance': 'owner_state',
                                   'value_role': 'identifier'},
                                  {'name': 'display_name',
                                   'type': 'string',
                                   'required': True,
                                   'nonempty': True,
                                   'max_length': 500,
                                   'derivation': 'v2 display policy retained exactly',
                                   'provenance': 'owner_state',
                                   'value_role': 'display_text'},
                                  {'name': 'headline',
                                   'type': 'string',
                                   'required': True,
                                   'nonempty': True,
                                   'max_length': 1000,
                                   'derivation': 'v2 display policy retained exactly',
                                   'provenance': 'owner_state',
                                   'value_role': 'display_text'},
                                  {'name': 'public_profile_url',
                                   'type': 'string',
                                   'required': False,
                                   'format': 'https_url',
                                   'max_length': 2048,
                                   'derivation': 'either absent from the canonical source object or an HTTPS URL; '
                                                 'empty-string alias forbidden',
                                   'provenance': 'owner_state',
                                   'value_role': 'web_url'},
                                  {'name': 'employment_statuses',
                                   'type': 'array',
                                   'required': True,
                                   'items': {'type': 'string',
                                             'enum': ['current', 'former'],
                                             'provenance': 'owner_state',
                                             'value_role': 'control'},
                                   'min_items': 1,
                                   'max_items': 2,
                                   'provenance': 'owner_state'},
                                  {'name': 'role_bucket_ids',
                                   'type': 'array',
                                   'required': True,
                                   'items': {'type': 'string',
                                             'enum': ['research',
                                                      'engineering',
                                                      'product_management',
                                                      'infra_systems',
                                                      'founding'],
                                             'provenance': 'owner_state',
                                             'value_role': 'control'},
                                   'max_items': 5,
                                   'provenance': 'owner_state'}],
                       'provenance': 'owner_state'},
             'max_items': 250,
             'variants': ['success'],
             'derivation': 'closed v2 public candidate item schema; display/URL policies and the 64-KiB result limit '
                           'remain at least as strict as v2; max_items=250 equals FILTER_PROJECTION_MAX_LIMIT',
             'provenance': 'owner_state'},
            {'name': 'reason',
             'type': 'string',
             'required': True,
             'enum': ['projection_candidate_set_empty',
                      'projection_publication_pending',
                      'projection_state_not_serving',
                      'projection_membership_pending',
                      'projection_members_not_ready'],
             'variants': ['deferred'],
             'derivation': 'exact closed deferred reason',
             'provenance': 'owner_state',
             'value_role': 'control'},
            {'name': 'reason',
             'type': 'string',
             'required': True,
             'constant': 'projection_not_found',
             'variants': ['error'],
             'provenance': 'owner_state',
             'value_role': 'control'},
            {'name': 'retryable',
             'type': 'boolean',
             'required': True,
             'variants': ['deferred'],
             'provenance': 'owner_state'},
            {'name': 'retryable',
             'type': 'boolean',
             'required': True,
             'constant': False,
             'variants': ['error'],
             'provenance': 'owner_state'},
            {'name': 'reselection_required',
             'type': 'boolean',
             'required': True,
             'variants': ['deferred'],
             'provenance': 'owner_state'},
            {'name': 'requested_target_ref',
             'type': 'object',
             'required': True,
             'fields': [{'name': 'projection_id',
                         'type': 'string',
                         'required': True,
                         'nonempty': True,
                         'provenance': 'owner_state',
                         'value_role': 'identifier'},
                        {'name': 'membership_revision',
                         'type': 'string',
                         'required': True,
                         'nonempty': True,
                         'derivation': 'non-empty opaque equality token per '
                                       'docs/CANONICAL_SERVING_PROJECTION_CONTRACT.md; equality/inequality only and '
                                       'never numerically, lexically, or chronologically ordered; no positive/numeric '
                                       'bound applies',
                         'provenance': 'owner_state',
                         'value_role': 'identifier'},
                        {'name': 'requested_terminal_id',
                         'type': 'string',
                         'required': False,
                         'nonempty': True,
                         'provenance': 'owner_state',
                         'value_role': 'identifier'},
                        {'name': 'requested_terminal_digest',
                         'type': 'string',
                         'required': False,
                         'format': 'sha256_hex',
                         'provenance': 'owner_state',
                         'value_role': 'identifier'},
                        {'name': 'route_revision_token',
                         'type': 'string',
                         'required': False,
                         'nonempty': True,
                         'provenance': 'owner_state',
                         'value_role': 'identifier'}],
             'variants': ['deferred'],
             'derivation': 'occurrence-bound requested target ref: projection_id and membership_revision always '
                           'present; requested_terminal_id/requested_terminal_digest/route_revision_token present '
                           'exactly when status=stale and absent for not_ready',
             'provenance': 'owner_state'},
            {'name': 'decision_ref',
             'type': 'string',
             'required': True,
             'format': 'sha256_hex',
             'variants': ['deferred', 'error'],
             'derivation': 'opaque SHA-256 over the occurrence-bound internal owner ref; discloses no foreign target; '
                           'missing and foreign serialize byte-identical public shapes for the same occurrence pins',
             'provenance': 'owner_state',
             'value_role': 'identifier'}]}


FILTER_PROJECTION_RESULT_V3_CONTRACT_DIGEST = contract_digest(FILTER_PROJECTION_RESULT_V3_SCHEMA)


# REFERENCED contract, not adopted or retyped by this module: the V3 success
# root binds filter_projection_product_ref.v1 as an immutable
# exact-version-and-digest reference, so its schema and contract digest are
# pinned beside the family to keep success-root validation closed.
FILTER_PROJECTION_PRODUCT_REF_V1_SCHEMA_VERSION = "filter_projection_product_ref.v1"
FILTER_PROJECTION_PRODUCT_REF_V1_OWNER = "serving_projection_owner"


FILTER_PROJECTION_PRODUCT_REF_V1_ORDERED_FIELDS = ('schema_version', 'projection_id', 'membership_revision', 'terminal_digest')


FILTER_PROJECTION_PRODUCT_REF_V1_SCHEMA = {'schema_version': 'filter_projection_product_ref.v1',
 'owner': 'serving_projection_owner',
 'fields': [{'name': 'schema_version',
             'type': 'string',
             'required': True,
             'constant': 'filter_projection_product_ref.v1'},
            {'name': 'projection_id', 'type': 'string', 'required': True, 'nonempty': True},
            {'name': 'membership_revision',
             'type': 'string',
             'required': True,
             'nonempty': True,
             'derivation': 'non-empty opaque equality token per docs/CANONICAL_SERVING_PROJECTION_CONTRACT.md; '
                           'equality/inequality only and never numerically, lexically, or chronologically ordered; no '
                           'positive/numeric bound applies'},
            {'name': 'terminal_digest',
             'type': 'string',
             'required': True,
             'format': 'sha256_hex',
             'derivation': 'terminal envelope digest'}]}


FILTER_PROJECTION_PRODUCT_REF_V1_CONTRACT_DIGEST = contract_digest(FILTER_PROJECTION_PRODUCT_REF_V1_SCHEMA)


# Adopted contract schemas by literal; implementations consume this map instead
# of rebuilding descriptor tables from the manifest.  The referenced
# filter_projection_product_ref.v1 schema is included so V3 success-root ref
# resolution stays closed.
CONTRACT_SCHEMAS = {
    FILTER_PROJECTION_PRODUCT_TERMINAL_V1_SCHEMA_VERSION: FILTER_PROJECTION_PRODUCT_TERMINAL_V1_SCHEMA,
    FILTER_PROJECTION_FRESHNESS_REF_V1_SCHEMA_VERSION: FILTER_PROJECTION_FRESHNESS_REF_V1_SCHEMA,
    FILTER_PROJECTION_READINESS_REF_V1_SCHEMA_VERSION: FILTER_PROJECTION_READINESS_REF_V1_SCHEMA,
    FILTER_PROJECTION_SUCCESS_OWNER_REF_V1_SCHEMA_VERSION: FILTER_PROJECTION_SUCCESS_OWNER_REF_V1_SCHEMA,
    FILTER_PROJECTION_STALE_OWNER_REF_V1_SCHEMA_VERSION: FILTER_PROJECTION_STALE_OWNER_REF_V1_SCHEMA,
    FILTER_PROJECTION_NOT_READY_OWNER_REF_V1_SCHEMA_VERSION: FILTER_PROJECTION_NOT_READY_OWNER_REF_V1_SCHEMA,
    FILTER_PROJECTION_MASKED_ABSENCE_OWNER_REF_V1_SCHEMA_VERSION: FILTER_PROJECTION_MASKED_ABSENCE_OWNER_REF_V1_SCHEMA,
    FILTER_PROJECTION_RESULT_V3_SCHEMA_VERSION: FILTER_PROJECTION_RESULT_V3_SCHEMA,
    FILTER_PROJECTION_PRODUCT_REF_V1_SCHEMA_VERSION: FILTER_PROJECTION_PRODUCT_REF_V1_SCHEMA,
}

# terminal_core_digest covers canonical fields 1-23 through terminal_generation.
_TERMINAL_CORE_FIELDS = FILTER_PROJECTION_PRODUCT_TERMINAL_V1_ORDERED_FIELDS[:23]


def compute_filter_projection_product_terminal_core_digest(record: dict[str, Any]) -> str:
    """Recompute ``terminal_core_digest`` over canonical fields 1-23 (the acyclic terminal core)."""

    if not isinstance(record, dict):
        _fail("terminal_core_digest", "digest input must be an object")
    covered = {field: record[field] for field in _TERMINAL_CORE_FIELDS if field in record}
    return hashlib.sha256(canonical_json(covered).encode("utf-8")).hexdigest()


def compute_filter_projection_product_terminal_digest(record: dict[str, Any]) -> str:
    """Recompute the envelope ``terminal_digest`` over every preceding field including the nested refs."""

    return _record_digest(record, "terminal_digest")


def compute_filter_projection_freshness_ref_digest(record: dict[str, Any]) -> str:
    """Recompute ``freshness_ref_digest`` over all preceding fields."""

    return _record_digest(record, "freshness_ref_digest")


def compute_filter_projection_readiness_ref_digest(record: dict[str, Any]) -> str:
    """Recompute ``readiness_ref_digest`` over all preceding fields."""

    return _record_digest(record, "readiness_ref_digest")


def compute_filter_projection_owner_ref_digest(record: dict[str, Any]) -> str:
    """Recompute ``owner_ref_digest`` over the canonical complete owner ref except itself."""

    return _record_digest(record, "owner_ref_digest")


def compute_filter_projection_readiness_prerequisite_set_digest() -> str:
    """SHA-256 over the canonical JSON of the exact eight-item readiness prerequisite set."""

    return hashlib.sha256(canonical_json(list(FILTER_PROJECTION_READINESS_PREREQUISITE_SET)).encode("utf-8")).hexdigest()


def _coerce_boundary(value: Any, label: str) -> CanonicalJsonObject:
    if isinstance(value, (str, bytes, bytearray)):
        try:
            value = strict_json_loads(value)
        except AgentRuntimeNamespaceRefError as exc:
            raise FilterProjectionTerminalContractError(
                f"filter projection terminal contract {label} invalid: {exc}"
            ) from exc
    if not isinstance(value, CanonicalJsonObject):
        _fail(
            label,
            "record must be JSON text/bytes (strictly decoded) or a decoder-produced canonical JSON object;"
            " plain dictionaries are not a decode boundary",
        )
    return value


def parse_filter_projection_freshness_ref(value: Any) -> CanonicalJsonObject:
    """Validate one ``filter_projection_freshness_ref.v1`` record and return it in manifest field order."""

    record = _coerce_boundary(value, "filter_projection_freshness_ref")
    _validate_object(record, FILTER_PROJECTION_FRESHNESS_REF_V1_SCHEMA["fields"], "filter_projection_freshness_ref")
    ordered = {field: record[field] for field in FILTER_PROJECTION_FRESHNESS_REF_V1_ORDERED_FIELDS}
    if ordered["freshness_ref_digest"] != compute_filter_projection_freshness_ref_digest(ordered):
        _fail("freshness_ref_digest", "does not recompute over all preceding fields")
    return _canonical_json_object(ordered)


def validate_filter_projection_freshness_ref(value: Any) -> None:
    """Fail closed unless ``value`` is one exact ``filter_projection_freshness_ref.v1`` record."""

    parse_filter_projection_freshness_ref(value)


def parse_filter_projection_readiness_ref(value: Any) -> CanonicalJsonObject:
    """Validate one ``filter_projection_readiness_ref.v1`` record and return it in manifest field order.

    ``prerequisite_set_digest`` must bind the exact eight-item prerequisite
    set; ``readiness_ref_digest`` must recompute over all preceding fields;
    and for the constant ``status=ready`` the counts are canonical for every
    consumer: ``row_ready_count == visible_member_count == candidate_count``.
    """

    record = _coerce_boundary(value, "filter_projection_readiness_ref")
    _validate_object(record, FILTER_PROJECTION_READINESS_REF_V1_SCHEMA["fields"], "filter_projection_readiness_ref")
    ordered = {field: record[field] for field in FILTER_PROJECTION_READINESS_REF_V1_ORDERED_FIELDS}
    if ordered["prerequisite_set_digest"] != compute_filter_projection_readiness_prerequisite_set_digest():
        _fail("prerequisite_set_digest", "must bind the exact eight-item prerequisite set")
    if ordered["visible_member_count"] != ordered["candidate_count"]:
        _fail("visible_member_count", "must equal candidate_count for status=ready")
    if ordered["row_ready_count"] != ordered["visible_member_count"]:
        _fail("row_ready_count", "must equal visible_member_count for status=ready")
    if ordered["readiness_ref_digest"] != compute_filter_projection_readiness_ref_digest(ordered):
        _fail("readiness_ref_digest", "does not recompute over all preceding fields")
    return _canonical_json_object(ordered)


def validate_filter_projection_readiness_ref(value: Any) -> None:
    """Fail closed unless ``value`` is one exact ``filter_projection_readiness_ref.v1`` record."""

    parse_filter_projection_readiness_ref(value)


def parse_filter_projection_product_terminal(value: Any) -> CanonicalJsonObject:
    """Validate one ``filter_projection_product_terminal.v1`` record and return it in manifest field order.

    Beyond closed structural validation this proves the terminal digest
    equalities and the nested ref field binding: ``terminal_core_digest``
    recomputes over canonical fields 1-23, every field shared with the nested
    freshness/readiness refs exact-compares against the terminal core
    (terminal id, route identity, membership revision, candidate-set digest,
    counts), ``row_ready_count == visible_member_count == candidate_count``
    holds for ready records, and the envelope ``terminal_digest`` recomputes
    over every preceding field.
    """

    record = _coerce_boundary(value, "filter_projection_product_terminal")
    _validate_object(record, FILTER_PROJECTION_PRODUCT_TERMINAL_V1_SCHEMA["fields"], "filter_projection_product_terminal")
    ordered = {
        field: record[field] for field in FILTER_PROJECTION_PRODUCT_TERMINAL_V1_ORDERED_FIELDS if field in record
    }
    # The complete nested namespace ref is tenant/mode-bound to the terminal:
    # a terminal can never carry a ref for another workspace or provider mode.
    namespace_ref = parse_agent_runtime_namespace_ref(ordered["runtime_namespace_ref"])
    if ordered["workspace_id"] != namespace_ref["workspace_id"]:
        _fail("workspace_id", "must equal the nested runtime namespace ref workspace_id")
    if ordered["provider_mode"] != namespace_ref["provider_mode"]:
        _fail("provider_mode", "must equal the nested runtime namespace ref provider_mode")
    core_digest = compute_filter_projection_product_terminal_core_digest(ordered)
    if ordered["terminal_core_digest"] != core_digest:
        _fail("terminal_core_digest", "does not recompute over canonical fields 1-23")
    freshness_ref = parse_filter_projection_freshness_ref(ordered["freshness_ref"])
    readiness_ref = parse_filter_projection_readiness_ref(ordered["readiness_ref"])
    # The nested refs are field-bound to the enclosing terminal core, not only
    # to the copied core digest: every shared field must exact-compare.
    for shared in (
        "product_terminal_id",
        "terminal_core_digest",
        "route_kind",
        "route_key",
        "route_revision_token",
        "membership_revision",
        "candidate_set_digest",
    ):
        if freshness_ref[shared] != ordered[shared]:
            _fail(f"freshness_ref.{shared}", "must equal the enclosing terminal value")
    for shared in ("product_terminal_id", "terminal_core_digest", "membership_revision", "candidate_set_digest"):
        if readiness_ref[shared] != ordered[shared]:
            _fail(f"readiness_ref.{shared}", "must equal the enclosing terminal value")
    if ordered["visible_member_count"] != ordered["candidate_count"]:
        _fail("visible_member_count", "must equal candidate_count")
    if readiness_ref["candidate_count"] != ordered["candidate_count"]:
        _fail("readiness_ref.candidate_count", "must equal the enclosing terminal candidate_count")
    if readiness_ref["visible_member_count"] != ordered["visible_member_count"]:
        _fail("readiness_ref.visible_member_count", "must equal the enclosing terminal visible_member_count")
    if readiness_ref["row_ready_count"] != readiness_ref["visible_member_count"]:
        _fail("readiness_ref.row_ready_count", "must equal visible_member_count for status=ready")
    if ordered["terminal_digest"] != compute_filter_projection_product_terminal_digest(ordered):
        _fail("terminal_digest", "does not recompute over every preceding field")
    return _canonical_json_object(ordered)


def validate_filter_projection_product_terminal(value: Any) -> None:
    """Fail closed unless ``value`` is one exact ``filter_projection_product_terminal.v1`` record."""

    parse_filter_projection_product_terminal(value)


def parse_filter_projection_product_ref(value: Any) -> CanonicalJsonObject:
    """Validate one referenced ``filter_projection_product_ref.v1`` record and return it in manifest field order."""

    record = _coerce_boundary(value, "filter_projection_product_ref")
    _validate_object(record, FILTER_PROJECTION_PRODUCT_REF_V1_SCHEMA["fields"], "filter_projection_product_ref")
    ordered = {field: record[field] for field in FILTER_PROJECTION_PRODUCT_REF_V1_ORDERED_FIELDS}
    return _canonical_json_object(ordered)


def validate_filter_projection_product_ref(value: Any) -> None:
    """Fail closed unless ``value`` is one exact referenced ``filter_projection_product_ref.v1`` record."""

    parse_filter_projection_product_ref(value)


def _parse_owner_ref(value: Any, schema: dict[str, Any], ordered_fields: tuple[str, ...], label: str) -> CanonicalJsonObject:
    record = _coerce_boundary(value, label)
    _validate_object(record, schema["fields"], label)
    ordered = {field: record[field] for field in ordered_fields}
    if ordered["result_contract_digest"] != FILTER_PROJECTION_RESULT_V3_CONTRACT_DIGEST:
        _fail(f"{label}.result_contract_digest", "must equal the filter_projection_result_v3 contract_digest")
    if ordered["serializer_contract_digest"] != FILTER_PROJECTION_RESULT_SERIALIZER_V3_CONTRACT_DIGEST:
        _fail(
            f"{label}.serializer_contract_digest",
            "must equal the filter_projection_result_serializer_v3 contract_digest",
        )
    if ordered["owner_ref_digest"] != compute_filter_projection_owner_ref_digest(ordered):
        _fail(f"{label}.owner_ref_digest", "does not recompute over the complete owner ref except itself")
    return _canonical_json_object(ordered)


def parse_filter_projection_success_owner_ref(value: Any) -> CanonicalJsonObject:
    """Validate one ``filter_projection_success_owner_ref.v1`` record and return it in manifest field order."""

    return _parse_owner_ref(
        value,
        FILTER_PROJECTION_SUCCESS_OWNER_REF_V1_SCHEMA,
        FILTER_PROJECTION_SUCCESS_OWNER_REF_V1_ORDERED_FIELDS,
        "filter_projection_success_owner_ref",
    )


def validate_filter_projection_success_owner_ref(value: Any) -> None:
    """Fail closed unless ``value`` is one exact ``filter_projection_success_owner_ref.v1`` record."""

    parse_filter_projection_success_owner_ref(value)


def parse_filter_projection_stale_owner_ref(value: Any) -> CanonicalJsonObject:
    """Validate one ``filter_projection_stale_owner_ref.v1`` record and return it in manifest field order."""

    return _parse_owner_ref(
        value,
        FILTER_PROJECTION_STALE_OWNER_REF_V1_SCHEMA,
        FILTER_PROJECTION_STALE_OWNER_REF_V1_ORDERED_FIELDS,
        "filter_projection_stale_owner_ref",
    )


def validate_filter_projection_stale_owner_ref(value: Any) -> None:
    """Fail closed unless ``value`` is one exact ``filter_projection_stale_owner_ref.v1`` record."""

    parse_filter_projection_stale_owner_ref(value)


def parse_filter_projection_not_ready_owner_ref(value: Any) -> CanonicalJsonObject:
    """Validate one ``filter_projection_not_ready_owner_ref.v1`` record and return it in manifest field order."""

    return _parse_owner_ref(
        value,
        FILTER_PROJECTION_NOT_READY_OWNER_REF_V1_SCHEMA,
        FILTER_PROJECTION_NOT_READY_OWNER_REF_V1_ORDERED_FIELDS,
        "filter_projection_not_ready_owner_ref",
    )


def validate_filter_projection_not_ready_owner_ref(value: Any) -> None:
    """Fail closed unless ``value`` is one exact ``filter_projection_not_ready_owner_ref.v1`` record."""

    parse_filter_projection_not_ready_owner_ref(value)


def parse_filter_projection_masked_absence_owner_ref(value: Any) -> CanonicalJsonObject:
    """Validate one ``filter_projection_masked_absence_owner_ref.v1`` record and return it in manifest field order.

    The masked absence ref carries no target id/workspace/existence/foreign
    owner bytes; the occurrence digest already binds the request.
    """

    return _parse_owner_ref(
        value,
        FILTER_PROJECTION_MASKED_ABSENCE_OWNER_REF_V1_SCHEMA,
        FILTER_PROJECTION_MASKED_ABSENCE_OWNER_REF_V1_ORDERED_FIELDS,
        "filter_projection_masked_absence_owner_ref",
    )


def validate_filter_projection_masked_absence_owner_ref(value: Any) -> None:
    """Fail closed unless ``value`` is one exact ``filter_projection_masked_absence_owner_ref.v1`` record."""

    parse_filter_projection_masked_absence_owner_ref(value)


def filter_projection_result_v3_variant_fields(variant: str) -> list[dict[str, Any]]:
    """Return the exact closed field descriptors for one V3 variant (union discrimination is exact)."""

    if variant not in FILTER_PROJECTION_RESULT_V3_VARIANTS:
        _fail("variant", f"unknown result variant {variant!r}")
    return [
        field
        for field in FILTER_PROJECTION_RESULT_V3_SCHEMA["fields"]
        if variant in field.get("variants", ())
    ]


def parse_filter_projection_result_v3(value: Any, *, variant: str | None = None) -> CanonicalJsonObject:
    """Validate one ``filter_projection_result_v3`` root and return it in manifest field order.

    The variant is exact: when ``variant`` is omitted it is taken from the
    record's ``variant`` discriminator, and the record is then validated
    closed against exactly that variant's field set.  Success roots must also
    satisfy the model-visible equations (canonical sibling
    ``cohort_selection_digest`` and registry pin, ``v3_page_equations``,
    freshness/readiness parity); deferred roots must carry the
    status-dependent requested-target shape (stale-only terminal fields
    present exactly when ``status == "stale"``).
    """

    record = _coerce_boundary(value, "filter_projection_result_v3")
    if variant is None:
        if record.get("variant") not in FILTER_PROJECTION_RESULT_V3_VARIANTS:
            _fail("variant", "missing or unknown result variant discriminator")
        variant = record["variant"]
    fields = filter_projection_result_v3_variant_fields(variant)
    _validate_object(record, fields, f"filter_projection_result_v3[{variant}]")
    ordered = {field: record[field] for field in FILTER_PROJECTION_RESULT_V3_ORDERED_FIELDS if field in record}
    if variant == "success":
        _enforce_success_equations(ordered)
    elif variant == "deferred":
        _enforce_deferred_target_shape(ordered)
    _enforce_value_roles(ordered, variant)
    # The canonical 64-KiB model-safety limit binds every variant: parser and
    # authoritative serializer accept exactly the same bytes.
    serialized = canonical_json(ordered).encode("utf-8")
    if len(serialized) > FILTER_PROJECTION_RESULT_V3_MAX_SERIALIZED_BYTES:
        _fail("serialized_bytes", f"canonical result exceeds {FILTER_PROJECTION_RESULT_V3_MAX_SERIALIZED_BYTES} UTF-8 bytes")
    return _canonical_json_object(ordered)


def _enforce_value_roles(record: dict[str, Any], variant: str) -> None:
    """Enforce the decision-locked ``action_result_positive_value_roles_v3`` map by JSON path.

    The map pins every model-visible string path's role: ``identifier``
    values are transport-neutral and separator-free, ``display_text``
    carries non-whitespace content, and ``web_url`` values are HTTPS URLs —
    so the schema preflight never approves values the authoritative
    serializer rejects for the pinned role.
    """

    roles = FILTER_PROJECTION_RESULT_V3_FIELD_VALUE_ROLES[variant]

    def _walk(value: Any, path: str) -> None:
        if isinstance(value, dict):
            for key, child in value.items():
                _walk(child, f"{path}/{key}")
            return
        if isinstance(value, list):
            for child in value:
                _walk(child, f"{path}/*")
            return
        if type(value) is not str:
            return
        role = roles.get(path)
        if role == "identifier":
            if (
                not value
                or value != value.strip()
                or any(character.isspace() for character in value)
                or "/" in value
                or "\\" in value
                or _URI_SCHEME_VALUE_RE.search(value) is not None
                or any(ord(character) < 0x20 or 0x7F <= ord(character) <= 0x9F for character in value)
            ):
                _fail(path, "identifier value-role violation")
        elif role == "display_text":
            if not _DISPLAY_TEXT_RE.search(value):
                _fail(path, "display_text must contain non-whitespace content")
        elif role == "web_url":
            if not _HTTPS_URL_RE.fullmatch(value):
                _fail(path, "web_url value-role violation")

    _walk(record, "")


def _enforce_success_equations(record: dict[str, Any]) -> None:
    # The sibling cohort_selection_digest equals the canonical
    # cohort_selection_digest(cohort_selection) computed by the owning
    # cohort_selection module over the registry-pinned selection record; the
    # model-visible selection and the execution commitment can never diverge.
    try:
        selection_digest = cohort_selection_digest(record["cohort_selection"])
    except ValueError as exc:
        raise FilterProjectionTerminalContractError(
            "filter projection terminal contract cohort_selection invalid: not a canonical selection"
        ) from exc
    if record["cohort_selection_digest"] != selection_digest:
        _fail("cohort_selection_digest", "must equal cohort_selection_digest(cohort_selection)")
    if record["cohort_selection_registry_digest"] != cohort_selection_registry_digest():
        _fail("cohort_selection_registry_digest", "must equal the exact pinned cohort selection registry digest")
    # v3_page_equations.
    if record["returned_count"] != len(record["candidates"]):
        _fail("returned_count", "must equal len(candidates)")
    expected_returned = min(record["limit"], max(0, record["total_count"] - record["offset"]))
    if record["returned_count"] != expected_returned:
        _fail("returned_count", "must equal min(limit, max(0, total_count - offset))")
    if record["truncated"] != (record["offset"] + record["returned_count"] < record["total_count"]):
        _fail("truncated", "must equal (offset + returned_count < total_count)")
    # The page is a window of the unique member set M: candidate refs are
    # unique within the returned page.
    candidate_refs = [candidate["candidate_ref"] for candidate in record["candidates"]]
    if len(set(candidate_refs)) != len(candidate_refs):
        _fail("candidates", "candidate_ref values must be unique within the returned page (M is unique)")
    # Lane-coverage derivation table (exact, closed): counts satisfy
    # requested = completed + missing, and the status discriminator is exact
    # per row — a result can never report completion and missing coverage
    # simultaneously:
    #   complete    <=> missing_lane_count == 0
    #   partial     <=> completed_lane_count > 0 and missing_lane_count > 0
    #   unavailable <=> completed_lane_count == 0 and missing_lane_count == requested_lane_count
    coverage = record["requested_lane_coverage"]
    if coverage["requested_lane_count"] != coverage["completed_lane_count"] + coverage["missing_lane_count"]:
        _fail("requested_lane_coverage", "requested_lane_count must equal completed + missing")
    status = coverage["status"]
    if status == "complete":
        consistent = coverage["missing_lane_count"] == 0
    elif status == "partial":
        consistent = coverage["completed_lane_count"] > 0 and coverage["missing_lane_count"] > 0
    else:  # unavailable
        consistent = coverage["completed_lane_count"] == 0 and coverage["missing_lane_count"] == coverage["requested_lane_count"]
    if not consistent:
        _fail("requested_lane_coverage", f"status/count contradiction for status={status}")
    # Coverage counts are bound to unique per-lane summary evidence: one
    # summary per requested lane with unique lane_id, and the counts derive
    # from the summary states (missing summaries are the missing lanes).
    summaries = record["lane_summaries"]
    summary_lane_ids = [summary["lane_id"] for summary in summaries]
    if len(set(summary_lane_ids)) != len(summary_lane_ids):
        _fail("lane_summaries", "lane_id values must be unique (per-lane exact summaries)")
    if len(summaries) != coverage["requested_lane_count"]:
        _fail("lane_summaries", "exactly one summary per requested lane")
    missing_from_summaries = sum(1 for summary in summaries if summary["coverage_status"] == "missing")
    if coverage["missing_lane_count"] != missing_from_summaries:
        _fail("requested_lane_coverage", "missing_lane_count must derive from summary states")
    if coverage["completed_lane_count"] != len(summaries) - missing_from_summaries:
        _fail("requested_lane_coverage", "completed_lane_count must derive from summary states")
    # Freshness/readiness parity with the root and with each other.
    freshness = parse_filter_projection_freshness_ref(record["freshness"])
    readiness = parse_filter_projection_readiness_ref(record["readiness"])
    if freshness["candidate_set_digest"] != record["candidate_set_digest"]:
        _fail("freshness.candidate_set_digest", "must equal the root candidate_set_digest")
    if readiness["candidate_set_digest"] != record["candidate_set_digest"]:
        _fail("readiness.candidate_set_digest", "must equal the root candidate_set_digest")
    if freshness["terminal_core_digest"] != readiness["terminal_core_digest"]:
        _fail("terminal_core_digest", "freshness and readiness refs must share the exact terminal core digest")
    if freshness["product_terminal_id"] != readiness["product_terminal_id"]:
        _fail("product_terminal_id", "freshness and readiness refs must share the exact product terminal id")
    if (
        freshness["membership_revision"] != readiness["membership_revision"]
        or freshness["membership_revision"] != record["projection_ref"]["membership_revision"]
    ):
        _fail("membership_revision", "freshness, readiness, and projection ref must share the exact membership revision")


def _enforce_deferred_target_shape(record: dict[str, Any]) -> None:
    stale_only_fields = {"requested_terminal_id", "requested_terminal_digest", "route_revision_token"}
    target = record["requested_target_ref"]
    if record["status"] == "stale":
        missing = stale_only_fields - set(target)
        if missing:
            _fail("requested_target_ref", f"status=stale requires fields {sorted(missing)}")
    else:
        present = set(target) & stale_only_fields
        if present:
            _fail("requested_target_ref", f"stale-only fields {sorted(present)} are absent for not_ready")
    # The decision-locked zero-member outcome is exact: an empty candidate
    # set is deferred not_ready, never retryable, always reselection-required.
    if record["reason"] == "projection_candidate_set_empty":
        if record["status"] != "not_ready":
            _fail("status", "projection_candidate_set_empty is exactly not_ready")
        if record["retryable"] is not False:
            _fail("retryable", "projection_candidate_set_empty is exactly retryable=false")
        if record["reselection_required"] is not True:
            _fail("reselection_required", "projection_candidate_set_empty is exactly reselection_required=true")


def validate_filter_projection_result_v3(value: Any, *, variant: str | None = None) -> None:
    """Fail closed unless ``value`` is one exact ``filter_projection_result_v3`` root for its variant."""

    parse_filter_projection_result_v3(value, variant=variant)


_REF_PARSERS.update(
    {
        AGENT_RUNTIME_NAMESPACE_REF_SCHEMA_VERSION: parse_agent_runtime_namespace_ref,
        FILTER_PROJECTION_FRESHNESS_REF_V1_SCHEMA_VERSION: parse_filter_projection_freshness_ref,
        FILTER_PROJECTION_READINESS_REF_V1_SCHEMA_VERSION: parse_filter_projection_readiness_ref,
        FILTER_PROJECTION_PRODUCT_REF_V1_SCHEMA_VERSION: parse_filter_projection_product_ref,
    }
)


def _iter_ref_descriptors(descriptors: list[dict[str, Any]]) -> list[dict[str, Any]]:
    refs: list[dict[str, Any]] = []
    for descriptor in descriptors:
        if "ref" in descriptor:
            refs.append(descriptor)
        items = descriptor.get("items")
        if isinstance(items, dict):
            refs.extend(_iter_ref_descriptors([items]))
        refs.extend(_iter_ref_descriptors(descriptor.get("fields", [])))
    return refs


def _check_internal_ref_pins() -> None:
    """Import-time drift guard: every module ref is an immutable exact-version-and-digest reference."""

    digests = {literal: contract_digest(schema) for literal, schema in CONTRACT_SCHEMAS.items()}
    digests[AGENT_RUNTIME_NAMESPACE_REF_SCHEMA_VERSION] = AGENT_RUNTIME_NAMESPACE_REF_CONTRACT_DIGEST
    for literal, schema in CONTRACT_SCHEMAS.items():
        for descriptor in _iter_ref_descriptors(schema["fields"]):
            ref = descriptor["ref"]
            if ref not in digests:
                raise FilterProjectionTerminalContractError(
                    f"filter projection terminal contract {literal} invalid: unresolvable ref {ref!r}"
                )
            if descriptor.get("ref_digest") != digests[ref]:
                raise FilterProjectionTerminalContractError(
                    f"filter projection terminal contract {literal} invalid: ref_digest drift for {ref!r}"
                )


_check_internal_ref_pins()


__all__ = [
    "AGENT_RUNTIME_NAMESPACE_REF_CONTRACT_DIGEST",
    "CONTRACT_SCHEMAS",
    "FILTER_PROJECTION_DEFERRED_REASON_PRECEDENCE",
    "FILTER_PROJECTION_FRESHNESS_REF_V1_CONTRACT_DIGEST",
    "FILTER_PROJECTION_FRESHNESS_REF_V1_ORDERED_FIELDS",
    "FILTER_PROJECTION_FRESHNESS_REF_V1_OWNER",
    "FILTER_PROJECTION_FRESHNESS_REF_V1_SCHEMA",
    "FILTER_PROJECTION_FRESHNESS_REF_V1_SCHEMA_VERSION",
    "FILTER_PROJECTION_MASKED_ABSENCE_OWNER_REF_V1_CONTRACT_DIGEST",
    "FILTER_PROJECTION_MASKED_ABSENCE_OWNER_REF_V1_ORDERED_FIELDS",
    "FILTER_PROJECTION_MASKED_ABSENCE_OWNER_REF_V1_OWNER",
    "FILTER_PROJECTION_MASKED_ABSENCE_OWNER_REF_V1_SCHEMA",
    "FILTER_PROJECTION_MASKED_ABSENCE_OWNER_REF_V1_SCHEMA_VERSION",
    "FILTER_PROJECTION_NOT_READY_OWNER_REF_V1_CONTRACT_DIGEST",
    "FILTER_PROJECTION_NOT_READY_OWNER_REF_V1_ORDERED_FIELDS",
    "FILTER_PROJECTION_NOT_READY_OWNER_REF_V1_OWNER",
    "FILTER_PROJECTION_NOT_READY_OWNER_REF_V1_SCHEMA",
    "FILTER_PROJECTION_NOT_READY_OWNER_REF_V1_SCHEMA_VERSION",
    "FILTER_PROJECTION_PRODUCT_REF_V1_CONTRACT_DIGEST",
    "FILTER_PROJECTION_PRODUCT_REF_V1_ORDERED_FIELDS",
    "FILTER_PROJECTION_PRODUCT_REF_V1_OWNER",
    "FILTER_PROJECTION_PRODUCT_REF_V1_SCHEMA",
    "FILTER_PROJECTION_PRODUCT_REF_V1_SCHEMA_VERSION",
    "FILTER_PROJECTION_PRODUCT_TERMINAL_V1_CONTRACT_DIGEST",
    "FILTER_PROJECTION_PRODUCT_TERMINAL_V1_ORDERED_FIELDS",
    "FILTER_PROJECTION_PRODUCT_TERMINAL_V1_OWNER",
    "FILTER_PROJECTION_PRODUCT_TERMINAL_V1_SCHEMA",
    "FILTER_PROJECTION_PRODUCT_TERMINAL_V1_SCHEMA_VERSION",
    "FILTER_PROJECTION_READINESS_PREREQUISITE_SET",
    "FILTER_PROJECTION_READINESS_REF_V1_CONTRACT_DIGEST",
    "FILTER_PROJECTION_READINESS_REF_V1_ORDERED_FIELDS",
    "FILTER_PROJECTION_READINESS_REF_V1_OWNER",
    "FILTER_PROJECTION_READINESS_REF_V1_SCHEMA",
    "FILTER_PROJECTION_READINESS_REF_V1_SCHEMA_VERSION",
    "FILTER_PROJECTION_RESULT_SERIALIZER_V3_CONTRACT_DIGEST",
    "FILTER_PROJECTION_RESULT_SERIALIZER_V3_OWNER_NAME",
    "FILTER_PROJECTION_RESULT_SERIALIZER_V3_REVISION",
    "FILTER_PROJECTION_RESULT_V3_CONTRACT_DIGEST",
    "FILTER_PROJECTION_RESULT_V3_DEFERRED_ROOT_FIELDS",
    "FILTER_PROJECTION_RESULT_V3_FIELD_VALUE_ROLES",
    "FILTER_PROJECTION_RESULT_V3_MASKED_ROOT_CONSTANTS",
    "FILTER_PROJECTION_RESULT_V3_MAX_SERIALIZED_BYTES",
    "FILTER_PROJECTION_RESULT_V3_MASKED_ROOT_FIELDS",
    "FILTER_PROJECTION_RESULT_V3_ORDERED_FIELDS",
    "FILTER_PROJECTION_RESULT_V3_OWNER",
    "FILTER_PROJECTION_RESULT_V3_SCHEMA",
    "FILTER_PROJECTION_RESULT_V3_SCHEMA_VERSION",
    "FILTER_PROJECTION_RESULT_V3_SUCCESS_ROOT_FIELDS",
    "FILTER_PROJECTION_RESULT_V3_VARIANTS",
    "FILTER_PROJECTION_STALE_OWNER_REF_V1_CONTRACT_DIGEST",
    "FILTER_PROJECTION_STALE_OWNER_REF_V1_ORDERED_FIELDS",
    "FILTER_PROJECTION_STALE_OWNER_REF_V1_OWNER",
    "FILTER_PROJECTION_STALE_OWNER_REF_V1_SCHEMA",
    "FILTER_PROJECTION_STALE_OWNER_REF_V1_SCHEMA_VERSION",
    "FILTER_PROJECTION_SUCCESS_OWNER_REF_V1_CONTRACT_DIGEST",
    "FILTER_PROJECTION_SUCCESS_OWNER_REF_V1_ORDERED_FIELDS",
    "FILTER_PROJECTION_SUCCESS_OWNER_REF_V1_OWNER",
    "FILTER_PROJECTION_SUCCESS_OWNER_REF_V1_SCHEMA",
    "FILTER_PROJECTION_SUCCESS_OWNER_REF_V1_SCHEMA_VERSION",
    "FilterProjectionTerminalContractError",
    "REJECTED_FILTER_PROJECTION_LITERALS",
    "RETAINED_FILTER_PROJECTION_HISTORY_LITERALS",
    "RETAINED_LOOKUP_FORBIDDEN",
    "canonical_json",
    "compute_filter_projection_freshness_ref_digest",
    "compute_filter_projection_owner_ref_digest",
    "compute_filter_projection_product_terminal_core_digest",
    "compute_filter_projection_product_terminal_digest",
    "compute_filter_projection_readiness_prerequisite_set_digest",
    "compute_filter_projection_readiness_ref_digest",
    "contract_digest",
    "filter_projection_result_v3_variant_fields",
    "parse_filter_projection_freshness_ref",
    "parse_filter_projection_masked_absence_owner_ref",
    "parse_filter_projection_not_ready_owner_ref",
    "parse_filter_projection_product_ref",
    "parse_filter_projection_product_terminal",
    "parse_filter_projection_readiness_ref",
    "parse_filter_projection_result_v3",
    "parse_filter_projection_stale_owner_ref",
    "parse_filter_projection_success_owner_ref",
    "validate_filter_projection_freshness_ref",
    "validate_filter_projection_masked_absence_owner_ref",
    "validate_filter_projection_not_ready_owner_ref",
    "validate_filter_projection_product_ref",
    "validate_filter_projection_product_terminal",
    "validate_filter_projection_readiness_ref",
    "validate_filter_projection_result_v3",
    "validate_filter_projection_stale_owner_ref",
    "validate_filter_projection_success_owner_ref",
]
