"""Cohort execution contract family (S1f0c FF-SCHEMA pure contract foundations).

This module consumes the canonical decision manifest
``docs/modules/serving-product/contracts/filter_projection_lineage_fixed_forward_decision_v1.json``
verbatim.  No literal, owner, ordered field set, descriptor, or digest here is
retyped, renamed, or reinterpreted.

The family pins seven adopted contracts:

- ``cohort_execution_capability.v2`` (owner ``cohort_runtime``): the execution
  capability successor; ``provider_mode`` is ``simulate`` or ``scripted`` at
  this release boundary (live remains unauthorized), ``max_output_candidates``
  is bounded at 1000, and ``capability_digest`` covers fields 1-18;
- ``cohort_execution_envelope.v1`` (owner ``cohort_runtime``): the
  capability-bearing execution envelope that replaces the rejected
  ``cohort_provider_execution_manifest.v2`` name; ``envelope_digest`` covers
  all preceding fields;
- ``cohort_execution_lane_result.v2`` (owner ``cohort_runtime``): one result
  lane per planning lane at the same zero-based ordinal;
- ``cohort_candidate_member.v1`` (owner ``cohort_runtime``): the canonical
  committed candidate; identities sort by UTF-8 bytes, memberships sort by
  planning ordinal, and ``public_profile_url`` is either absent or an HTTPS
  URL (the empty-string alias is forbidden);
- ``cohort_candidate_set.v1`` (owner ``cohort_runtime``): the candidate set
  header rebuilt from the authoritative immutable child rows under lock;
  ``candidate_set_digest`` covers all header fields except itself, depends
  only on source/member inputs, and never references
  ``execution_result_digest`` (normative anti-cycle); product publication
  requires a nonempty set of at most 1000 members;
- ``cohort_execution_result.v2`` (owner ``cohort_runtime``): the
  execution-result successor with ``lane_coverage_status == "complete"``;
  ``missing_required_lane_count`` keeps its v1 role-intersection identity-loss
  meaning and is never lane coverage; all counts and ``result_digest`` are
  compiler-derived;
- ``cohort_execution_commit.v1`` (contract owner ``cohort_provider_runtime``):
  the accepted execution fact persisted insert-once; the record carries no
  ``owner`` field by design, persists ``schema_version`` and
  ``commit_contract_digest`` (an exact copy of this contract's
  ``contract_digest``), and ``commit_digest`` covers every preceding contract
  field except timestamps and itself, so no future schema can produce
  indistinguishable commit bytes.

Retained exactly as history and never retyped by this module:
``cohort_execution_capability.v1``, ``cohort_provider_manifest.v1``,
``cohort_provider_manifest.v2``, and ``cohort_execution_result.v1``.  Lookup
is exact version plus contract/result digest; shape inference, lexicographic
latest, mutable current alias, and auto-upgrade are forbidden.

Beyond closed shape and checksum validation, every parser enforces the
manifest count/cross-surface equations that are derivable from the record
itself: capability values must equal their nested runtime namespace ref;
envelope values must equal their nested capability and planned-lane ordinals
are the exact zero-based order; lane occurrence counts are an exact
accepted/rejected/truncated partition; result aggregates equal the lane sums
with exact ordinal mapping; commit values must equal their nested execution
result.  Equations needing external state require explicit owner pins:
``parse_cohort_execution_result`` and ``parse_cohort_execution_commit`` take
the authoritative ``cohort_candidate_set.v1`` record and bind
``unique_candidate_count == candidate_set.member_count`` and the
candidate-set digest exactly.

Decode boundary: all parsers require the decoder-typed
``CanonicalJsonObject`` produced solely by the shared strict decoder in
``agent_runtime_namespace_ref`` (``strict_json_loads`` /
``canonical_json_object``); duplicate keys are rejected at decode, never
collapsed.
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
    CanonicalJsonObject,
    canonical_json_object,
    parse_agent_runtime_namespace_ref,
)

RETAINED_COHORT_HISTORY_LITERALS = (
    "cohort_execution_capability.v1",
    "cohort_provider_manifest.v1",
    "cohort_provider_manifest.v2",
    "cohort_execution_result.v1",
)
REJECTED_COHORT_LITERALS = ("cohort_provider_execution_manifest.v2",)
RETAINED_LOOKUP_FORBIDDEN = (
    "shape inference",
    "lexicographic latest",
    "mutable current alias",
    "auto-upgrade",
)

_SHA256_HEX_RE = re.compile(r"[0-9a-f]{64}")

# Ref resolution is lazy: populated with literal -> parse function after the
# parse functions below are defined.  An unresolvable ref fails closed.
_REF_PARSERS: dict[str, Callable[[Any], dict[str, Any]]] = {}


class CohortExecutionContractError(ValueError):
    """Raised when a Cohort execution contract value fails closed."""


def canonical_json(value: Any) -> str:
    """Manifest canonical JSON: sorted keys, compact separators, Unicode preserved, ``allow_nan=False``."""

    try:
        return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)
    except (TypeError, ValueError, OverflowError, UnicodeError) as exc:
        raise CohortExecutionContractError("cohort execution contract JSON invalid") from exc


def contract_digest(schema: dict[str, Any]) -> str:
    """Manifest-pinned digest equation: ``SHA256(UTF8(canonical_json(schema)))``."""

    return hashlib.sha256(canonical_json(schema).encode("utf-8")).hexdigest()


def _fail(label: str, reason: str) -> None:
    raise CohortExecutionContractError(f"cohort execution contract {label} invalid: {reason}")


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
        ref = descriptor.get("ref")
        if ref is not None:
            parser = _REF_PARSERS.get(ref)
            if parser is None:
                _fail(label, f"unresolvable ref {ref!r}")
            try:
                parser(value)
            except CohortExecutionContractError:
                raise
            except ValueError as exc:
                raise CohortExecutionContractError(
                    f"cohort execution contract {label} invalid: ref {ref!r} rejected the value"
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


COHORT_EXECUTION_CAPABILITY_V2_SCHEMA_VERSION = "cohort_execution_capability.v2"
COHORT_EXECUTION_CAPABILITY_V2_OWNER = "cohort_runtime"


COHORT_EXECUTION_CAPABILITY_V2_ORDERED_FIELDS = ('schema_version',
 'owner',
 'workspace_id',
 'acquisition_run_id',
 'planning_manifest_digest',
 'cohort_selection_digest',
 'provider_mode',
 'runtime_namespace_ref',
 'policy_revision',
 'max_provider_calls',
 'max_provider_items',
 'max_output_candidates',
 'role_proof_verifier_id',
 'role_proof_verifier_revision',
 'cost_policy_revision',
 'retry_policy_revision',
 'circuit_policy_revision',
 'execution_generation',
 'capability_digest')


COHORT_EXECUTION_CAPABILITY_V2_SCHEMA = {'schema_version': 'cohort_execution_capability.v2',
 'owner': 'cohort_runtime',
 'fields': [{'name': 'schema_version',
             'type': 'string',
             'required': True,
             'constant': 'cohort_execution_capability.v2'},
            {'name': 'owner', 'type': 'string', 'required': True, 'constant': 'cohort_runtime'},
            {'name': 'workspace_id', 'type': 'string', 'required': True, 'nonempty': True},
            {'name': 'acquisition_run_id', 'type': 'string', 'required': True, 'nonempty': True},
            {'name': 'planning_manifest_digest',
             'type': 'string',
             'required': True,
             'format': 'sha256_hex',
             'derivation': 'exact validated planning-v2 manifest'},
            {'name': 'cohort_selection_digest',
             'type': 'string',
             'required': True,
             'format': 'sha256_hex',
             'derivation': 'exact pinned cohort selection'},
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
            {'name': 'policy_revision', 'type': 'string', 'required': True, 'nonempty': True},
            {'name': 'max_provider_calls', 'type': 'integer', 'required': True, 'minimum': 1},
            {'name': 'max_provider_items', 'type': 'integer', 'required': True, 'minimum': 1},
            {'name': 'max_output_candidates', 'type': 'integer', 'required': True, 'minimum': 1, 'maximum': 1000},
            {'name': 'role_proof_verifier_id', 'type': 'string', 'required': True, 'nonempty': True},
            {'name': 'role_proof_verifier_revision', 'type': 'string', 'required': True, 'nonempty': True},
            {'name': 'cost_policy_revision', 'type': 'string', 'required': True, 'nonempty': True},
            {'name': 'retry_policy_revision', 'type': 'string', 'required': True, 'nonempty': True},
            {'name': 'circuit_policy_revision', 'type': 'string', 'required': True, 'nonempty': True},
            {'name': 'execution_generation', 'type': 'integer', 'required': True, 'minimum': 1},
            {'name': 'capability_digest',
             'type': 'string',
             'required': True,
             'format': 'sha256_hex',
             'derivation': 'over fields 1-18'}]}


COHORT_EXECUTION_CAPABILITY_V2_CONTRACT_DIGEST = contract_digest(COHORT_EXECUTION_CAPABILITY_V2_SCHEMA)


COHORT_EXECUTION_ENVELOPE_V1_SCHEMA_VERSION = "cohort_execution_envelope.v1"
COHORT_EXECUTION_ENVELOPE_V1_OWNER = "cohort_runtime"


COHORT_EXECUTION_ENVELOPE_V1_ORDERED_FIELDS = ('schema_version',
 'owner',
 'workspace_id',
 'acquisition_run_id',
 'planning_manifest_digest',
 'capability',
 'capability_digest',
 'provider_mode',
 'runtime_namespace_ref',
 'ordered_planned_lane_refs',
 'execution_generation',
 'envelope_digest')


COHORT_EXECUTION_ENVELOPE_V1_SCHEMA = {'schema_version': 'cohort_execution_envelope.v1',
 'owner': 'cohort_runtime',
 'fields': [{'name': 'schema_version', 'type': 'string', 'required': True, 'constant': 'cohort_execution_envelope.v1'},
            {'name': 'owner', 'type': 'string', 'required': True, 'constant': 'cohort_runtime'},
            {'name': 'workspace_id', 'type': 'string', 'required': True, 'nonempty': True},
            {'name': 'acquisition_run_id', 'type': 'string', 'required': True, 'nonempty': True},
            {'name': 'planning_manifest_digest',
             'type': 'string',
             'required': True,
             'format': 'sha256_hex',
             'derivation': 'exact validated planning-v2 manifest'},
            {'name': 'capability',
             'type': 'object',
             'required': True,
             'ref': 'cohort_execution_capability.v2',
             'ref_digest': '1e7439d825dccf31917bd42b0ba9a6d4330cc3af958e287dabb9028fdc11ba1e'},
            {'name': 'capability_digest',
             'type': 'string',
             'required': True,
             'format': 'sha256_hex',
             'derivation': 'equals the nested capability record digest'},
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
            {'name': 'ordered_planned_lane_refs',
             'type': 'array',
             'required': True,
             'items': {'type': 'object',
                       'fields': [{'name': 'ordinal',
                                   'type': 'integer',
                                   'required': True,
                                   'minimum': 0,
                                   'derivation': 'zero-based exact integer'},
                                  {'name': 'lane_id', 'type': 'string', 'required': True, 'nonempty': True},
                                  {'name': 'lane_digest',
                                   'type': 'string',
                                   'required': True,
                                   'format': 'sha256_hex',
                                   'derivation': 'recomputed planning-v2 lane digest'}]},
             'ordering': 'the array is the validated planning-v2 order'},
            {'name': 'execution_generation', 'type': 'integer', 'required': True, 'minimum': 1},
            {'name': 'envelope_digest',
             'type': 'string',
             'required': True,
             'format': 'sha256_hex',
             'derivation': 'covers all preceding fields'}]}


COHORT_EXECUTION_ENVELOPE_V1_CONTRACT_DIGEST = contract_digest(COHORT_EXECUTION_ENVELOPE_V1_SCHEMA)


COHORT_EXECUTION_LANE_RESULT_V2_SCHEMA_VERSION = "cohort_execution_lane_result.v2"
COHORT_EXECUTION_LANE_RESULT_V2_OWNER = "cohort_runtime"


COHORT_EXECUTION_LANE_RESULT_V2_ORDERED_FIELDS = ('schema_version',
 'ordinal',
 'lane_id',
 'planned_lane_digest',
 'provider_exposure_id',
 'provider_call_id',
 'provider_response_digest',
 'evidence_ref',
 'evidence_digest',
 'raw_occurrence_count',
 'accepted_occurrence_count',
 'rejected_occurrence_count',
 'truncated_occurrence_count',
 'ordered_accepted_occurrence_digests',
 'lane_result_digest')


COHORT_EXECUTION_LANE_RESULT_V2_SCHEMA = {'schema_version': 'cohort_execution_lane_result.v2',
 'owner': 'cohort_runtime',
 'fields': [{'name': 'schema_version',
             'type': 'string',
             'required': True,
             'constant': 'cohort_execution_lane_result.v2'},
            {'name': 'ordinal',
             'type': 'integer',
             'required': True,
             'minimum': 0,
             'derivation': 'zero-based exact integer, one per planning lane at the same ordinal'},
            {'name': 'lane_id', 'type': 'string', 'required': True, 'nonempty': True},
            {'name': 'planned_lane_digest',
             'type': 'string',
             'required': True,
             'format': 'sha256_hex',
             'derivation': 'recompute(P[i])'},
            {'name': 'provider_exposure_id', 'type': 'string', 'required': True, 'nonempty': True},
            {'name': 'provider_call_id', 'type': 'string', 'required': True, 'nonempty': True},
            {'name': 'provider_response_digest',
             'type': 'string',
             'required': True,
             'format': 'sha256_hex',
             'derivation': 'provider response bytes'},
            {'name': 'evidence_ref',
             'type': 'string',
             'required': True,
             'nonempty': True,
             'derivation': 'opaque server identifier, never a filesystem path'},
            {'name': 'evidence_digest',
             'type': 'string',
             'required': True,
             'format': 'sha256_hex',
             'derivation': 'evidence bytes'},
            {'name': 'raw_occurrence_count', 'type': 'integer', 'required': True, 'minimum': 0},
            {'name': 'accepted_occurrence_count', 'type': 'integer', 'required': True, 'minimum': 0},
            {'name': 'rejected_occurrence_count', 'type': 'integer', 'required': True, 'minimum': 0},
            {'name': 'truncated_occurrence_count', 'type': 'integer', 'required': True, 'minimum': 0},
            {'name': 'ordered_accepted_occurrence_digests',
             'type': 'array',
             'required': True,
             'items': {'type': 'string', 'format': 'sha256_hex'},
             'ordering': 'provider occurrence order within the lane'},
            {'name': 'lane_result_digest',
             'type': 'string',
             'required': True,
             'format': 'sha256_hex',
             'derivation': 'canonical lane-result fields except itself'}]}


COHORT_EXECUTION_LANE_RESULT_V2_CONTRACT_DIGEST = contract_digest(COHORT_EXECUTION_LANE_RESULT_V2_SCHEMA)


COHORT_CANDIDATE_MEMBER_V1_SCHEMA_VERSION = "cohort_candidate_member.v1"
COHORT_CANDIDATE_MEMBER_V1_OWNER = "cohort_runtime"


COHORT_CANDIDATE_MEMBER_V1_ORDERED_FIELDS = ('schema_version',
 'candidate_identity_key',
 'display_name',
 'headline',
 'public_profile_url',
 'ordered_lane_memberships',
 'member_digest')


COHORT_CANDIDATE_MEMBER_V1_SCHEMA = {'schema_version': 'cohort_candidate_member.v1',
 'owner': 'cohort_runtime',
 'fields': [{'name': 'schema_version', 'type': 'string', 'required': True, 'constant': 'cohort_candidate_member.v1'},
            {'name': 'candidate_identity_key',
             'type': 'string',
             'required': True,
             'nonempty': True,
             'derivation': 'candidate identities sort by UTF-8 bytes'},
            {'name': 'display_name', 'type': 'string', 'required': True},
            {'name': 'headline', 'type': 'string', 'required': True},
            {'name': 'public_profile_url',
             'type': 'string',
             'required': False,
             'format': 'https_url',
             'derivation': 'either absent from the canonical source object or an HTTPS URL; one closed variant schema; '
                           'empty-string alias forbidden'},
            {'name': 'ordered_lane_memberships',
             'type': 'array',
             'required': True,
             'items': {'type': 'object',
                       'fields': [{'name': 'lane_id', 'type': 'string', 'required': True, 'nonempty': True},
                                  {'name': 'planned_lane_digest',
                                   'type': 'string',
                                   'required': True,
                                   'format': 'sha256_hex',
                                   'derivation': 'recomputed planning-v2 lane digest'},
                                  {'name': 'lane_result_digest',
                                   'type': 'string',
                                   'required': True,
                                   'format': 'sha256_hex',
                                   'derivation': 'binds cohort_execution_lane_result.v2'},
                                  {'name': 'employment_status', 'type': 'string', 'required': True, 'nonempty': True},
                                  {'name': 'role_bucket_id', 'type': 'string', 'required': True, 'nonempty': True}]},
             'ordering': 'sorted by planning ordinal'},
            {'name': 'member_digest',
             'type': 'string',
             'required': True,
             'format': 'sha256_hex',
             'derivation': 'canonical member fields except itself'}]}


COHORT_CANDIDATE_MEMBER_V1_CONTRACT_DIGEST = contract_digest(COHORT_CANDIDATE_MEMBER_V1_SCHEMA)


COHORT_CANDIDATE_SET_V1_SCHEMA_VERSION = "cohort_candidate_set.v1"
COHORT_CANDIDATE_SET_V1_OWNER = "cohort_runtime"


COHORT_CANDIDATE_SET_V1_ORDERED_FIELDS = ('schema_version',
 'workspace_id',
 'acquisition_run_id',
 'execution_generation',
 'planning_manifest_digest',
 'member_count',
 'ordered_member_digests',
 'candidate_set_digest')


COHORT_CANDIDATE_SET_V1_SCHEMA = {'schema_version': 'cohort_candidate_set.v1',
 'owner': 'cohort_runtime',
 'fields': [{'name': 'schema_version', 'type': 'string', 'required': True, 'constant': 'cohort_candidate_set.v1'},
            {'name': 'workspace_id', 'type': 'string', 'required': True, 'nonempty': True},
            {'name': 'acquisition_run_id', 'type': 'string', 'required': True, 'nonempty': True},
            {'name': 'execution_generation', 'type': 'integer', 'required': True, 'minimum': 1},
            {'name': 'planning_manifest_digest',
             'type': 'string',
             'required': True,
             'format': 'sha256_hex',
             'derivation': 'exact validated planning-v2 manifest'},
            {'name': 'member_count',
             'type': 'integer',
             'required': True,
             'minimum': 0,
             'maximum': 1000,
             'derivation': '== len(ordered_member_digests); nonempty required for product publication'},
            {'name': 'ordered_member_digests',
             'type': 'array',
             'required': True,
             'items': {'type': 'string', 'format': 'sha256_hex'},
             'ordering': 'UTF-8 identity order'},
            {'name': 'candidate_set_digest',
             'type': 'string',
             'required': True,
             'format': 'sha256_hex',
             'derivation': 'covers all header fields except itself; depends only on source/member inputs and never '
                           'references execution_result_digest; normative anti-cycle per digest_dependency_dag'}]}


COHORT_CANDIDATE_SET_V1_CONTRACT_DIGEST = contract_digest(COHORT_CANDIDATE_SET_V1_SCHEMA)


COHORT_EXECUTION_RESULT_V2_SCHEMA_VERSION = "cohort_execution_result.v2"
COHORT_EXECUTION_RESULT_V2_OWNER = "cohort_runtime"


COHORT_EXECUTION_RESULT_V2_ORDERED_FIELDS = ('schema_version',
 'owner',
 'workspace_id',
 'acquisition_run_id',
 'planning_manifest_digest',
 'capability_digest',
 'execution_envelope_digest',
 'execution_attempt_id',
 'execution_generation',
 'ordered_lane_results',
 'lane_coverage_status',
 'planned_lane_count',
 'executed_lane_count',
 'raw_occurrence_count',
 'accepted_occurrence_count',
 'unique_candidate_count',
 'truncated_count',
 'rejected_unverified_count',
 'missing_required_lane_count',
 'candidate_set_digest',
 'result_digest')


COHORT_EXECUTION_RESULT_V2_SCHEMA = {'schema_version': 'cohort_execution_result.v2',
 'owner': 'cohort_runtime',
 'fields': [{'name': 'schema_version', 'type': 'string', 'required': True, 'constant': 'cohort_execution_result.v2'},
            {'name': 'owner', 'type': 'string', 'required': True, 'constant': 'cohort_runtime'},
            {'name': 'workspace_id', 'type': 'string', 'required': True, 'nonempty': True},
            {'name': 'acquisition_run_id', 'type': 'string', 'required': True, 'nonempty': True},
            {'name': 'planning_manifest_digest',
             'type': 'string',
             'required': True,
             'format': 'sha256_hex',
             'derivation': 'exact validated planning-v2 manifest'},
            {'name': 'capability_digest',
             'type': 'string',
             'required': True,
             'format': 'sha256_hex',
             'derivation': 'binds cohort_execution_capability.v2'},
            {'name': 'execution_envelope_digest',
             'type': 'string',
             'required': True,
             'format': 'sha256_hex',
             'derivation': 'binds cohort_execution_envelope.v1'},
            {'name': 'execution_attempt_id', 'type': 'string', 'required': True, 'nonempty': True},
            {'name': 'execution_generation', 'type': 'integer', 'required': True, 'minimum': 1},
            {'name': 'ordered_lane_results',
             'type': 'array',
             'required': True,
             'items': {'type': 'object',
                       'ref': 'cohort_execution_lane_result.v2',
                       'ref_digest': '686c94b8ff7ed03eb86d3cabf95719f225ad60cbda61f7b07e613a7220196c97'},
             'ordering': 'validated planning-v2 ordinal order with exact ordinal mapping'},
            {'name': 'lane_coverage_status', 'type': 'string', 'required': True, 'constant': 'complete'},
            {'name': 'planned_lane_count', 'type': 'integer', 'required': True, 'minimum': 0},
            {'name': 'executed_lane_count', 'type': 'integer', 'required': True, 'minimum': 0},
            {'name': 'raw_occurrence_count', 'type': 'integer', 'required': True, 'minimum': 0},
            {'name': 'accepted_occurrence_count', 'type': 'integer', 'required': True, 'minimum': 0},
            {'name': 'unique_candidate_count', 'type': 'integer', 'required': True, 'minimum': 0, 'maximum': 1000},
            {'name': 'truncated_count', 'type': 'integer', 'required': True, 'minimum': 0},
            {'name': 'rejected_unverified_count', 'type': 'integer', 'required': True, 'minimum': 0},
            {'name': 'missing_required_lane_count',
             'type': 'integer',
             'required': True,
             'minimum': 0,
             'derivation': 'role-intersection identity loss; never lane coverage'},
            {'name': 'candidate_set_digest',
             'type': 'string',
             'required': True,
             'format': 'sha256_hex',
             'derivation': 'binds the completed cohort_candidate_set.v1 digest; construction order candidate set then '
                           'execution result; normative anti-cycle per digest_dependency_dag'},
            {'name': 'result_digest',
             'type': 'string',
             'required': True,
             'format': 'sha256_hex',
             'derivation': 'compiler-derived over canonical result-v2 fields except itself'}]}


COHORT_EXECUTION_RESULT_V2_CONTRACT_DIGEST = contract_digest(COHORT_EXECUTION_RESULT_V2_SCHEMA)


COHORT_EXECUTION_COMMIT_V1_SCHEMA_VERSION = "cohort_execution_commit.v1"
COHORT_EXECUTION_COMMIT_V1_OWNER = "cohort_provider_runtime"


COHORT_EXECUTION_COMMIT_V1_ORDERED_FIELDS = ('schema_version',
 'execution_commit_id',
 'workspace_id',
 'acquisition_run_id',
 'operation_run_id',
 'workflow_run_id',
 'execution_attempt_id',
 'execution_generation',
 'start_authority_carrier_digest',
 'execution_authority_digest',
 'planning_manifest_digest',
 'capability_digest',
 'execution_envelope_digest',
 'execution_result_json',
 'execution_result_digest',
 'candidate_set_digest',
 'candidate_count',
 'commit_contract_digest',
 'commit_digest')


COHORT_EXECUTION_COMMIT_V1_SCHEMA = {'schema_version': 'cohort_execution_commit.v1',
 'owner': 'cohort_provider_runtime',
 'fields': [{'name': 'schema_version', 'type': 'string', 'required': True, 'constant': 'cohort_execution_commit.v1'},
            {'name': 'execution_commit_id', 'type': 'string', 'required': True, 'nonempty': True},
            {'name': 'workspace_id', 'type': 'string', 'required': True, 'nonempty': True},
            {'name': 'acquisition_run_id', 'type': 'string', 'required': True, 'nonempty': True},
            {'name': 'operation_run_id', 'type': 'string', 'required': True, 'nonempty': True},
            {'name': 'workflow_run_id', 'type': 'string', 'required': True, 'nonempty': True},
            {'name': 'execution_attempt_id', 'type': 'string', 'required': True, 'nonempty': True},
            {'name': 'execution_generation', 'type': 'integer', 'required': True, 'minimum': 1},
            {'name': 'start_authority_carrier_digest',
             'type': 'string',
             'required': True,
             'format': 'sha256_hex',
             'derivation': 'binds acquisition_start_authority_carrier.v1'},
            {'name': 'execution_authority_digest',
             'type': 'string',
             'required': True,
             'format': 'sha256_hex',
             'derivation': 'binds acquisition_execution_authority.v1'},
            {'name': 'planning_manifest_digest',
             'type': 'string',
             'required': True,
             'format': 'sha256_hex',
             'derivation': 'exact validated planning-v2 manifest'},
            {'name': 'capability_digest',
             'type': 'string',
             'required': True,
             'format': 'sha256_hex',
             'derivation': 'binds cohort_execution_capability.v2'},
            {'name': 'execution_envelope_digest',
             'type': 'string',
             'required': True,
             'format': 'sha256_hex',
             'derivation': 'binds cohort_execution_envelope.v1'},
            {'name': 'execution_result_json',
             'type': 'object',
             'required': True,
             'ref': 'cohort_execution_result.v2',
             'derivation': 'exact complete result record bytes',
             'ref_digest': 'ceaaa811c483d24780e6d589e4ed2a4baedbfb89d73714a97bca34ede8b275a2'},
            {'name': 'execution_result_digest',
             'type': 'string',
             'required': True,
             'format': 'sha256_hex',
             'derivation': 'binds cohort_execution_result.v2'},
            {'name': 'candidate_set_digest',
             'type': 'string',
             'required': True,
             'format': 'sha256_hex',
             'derivation': 'binds cohort_candidate_set.v1'},
            {'name': 'candidate_count', 'type': 'integer', 'required': True, 'minimum': 0, 'maximum': 1000},
            {'name': 'commit_contract_digest',
             'type': 'string',
             'required': True,
             'format': 'sha256_hex',
             'derivation': "exact copy of this contract's contract_digest; persists schema identity; constant "
                           'self-binding excluded from the digest_dependency_dag construction edges'},
            {'name': 'commit_digest',
             'type': 'string',
             'required': True,
             'format': 'sha256_hex',
             'derivation': 'covers every preceding contract field including schema_version and commit_contract_digest, '
                           'except timestamps and itself'}]}


COHORT_EXECUTION_COMMIT_V1_CONTRACT_DIGEST = contract_digest(COHORT_EXECUTION_COMMIT_V1_SCHEMA)


# Adopted contract schemas by literal; implementations consume this map instead
# of rebuilding descriptor tables from the manifest.
CONTRACT_SCHEMAS = {
    COHORT_EXECUTION_CAPABILITY_V2_SCHEMA_VERSION: COHORT_EXECUTION_CAPABILITY_V2_SCHEMA,
    COHORT_EXECUTION_ENVELOPE_V1_SCHEMA_VERSION: COHORT_EXECUTION_ENVELOPE_V1_SCHEMA,
    COHORT_EXECUTION_LANE_RESULT_V2_SCHEMA_VERSION: COHORT_EXECUTION_LANE_RESULT_V2_SCHEMA,
    COHORT_CANDIDATE_MEMBER_V1_SCHEMA_VERSION: COHORT_CANDIDATE_MEMBER_V1_SCHEMA,
    COHORT_CANDIDATE_SET_V1_SCHEMA_VERSION: COHORT_CANDIDATE_SET_V1_SCHEMA,
    COHORT_EXECUTION_RESULT_V2_SCHEMA_VERSION: COHORT_EXECUTION_RESULT_V2_SCHEMA,
    COHORT_EXECUTION_COMMIT_V1_SCHEMA_VERSION: COHORT_EXECUTION_COMMIT_V1_SCHEMA,
}


def compute_cohort_execution_capability_digest(record: dict[str, Any]) -> str:
    """Recompute ``capability_digest`` over fields 1-18 (all fields except itself)."""

    return _record_digest(record, "capability_digest")


def compute_cohort_execution_envelope_digest(record: dict[str, Any]) -> str:
    """Recompute ``envelope_digest`` over all preceding fields (all except itself)."""

    return _record_digest(record, "envelope_digest")


def compute_cohort_execution_lane_result_digest(record: dict[str, Any]) -> str:
    """Recompute ``lane_result_digest`` over the canonical lane-result fields except itself."""

    return _record_digest(record, "lane_result_digest")


def compute_cohort_candidate_member_digest(record: dict[str, Any]) -> str:
    """Recompute ``member_digest`` over the canonical member fields except itself."""

    return _record_digest(record, "member_digest")


def compute_cohort_candidate_set_digest(record: dict[str, Any]) -> str:
    """Recompute ``candidate_set_digest`` over all header fields except itself.

    The header depends only on source/member inputs and never references
    ``execution_result_digest`` (normative anti-cycle).
    """

    return _record_digest(record, "candidate_set_digest")


def compute_cohort_execution_result_digest(record: dict[str, Any]) -> str:
    """Recompute the compiler-derived ``result_digest`` over the canonical result-v2 fields except itself."""

    return _record_digest(record, "result_digest")


def compute_cohort_execution_commit_digest(record: dict[str, Any]) -> str:
    """Recompute ``commit_digest`` over every preceding contract field except timestamps and itself."""

    return _record_digest(record, "commit_digest")


def _require_decoder_typed(value: Any, label: str) -> CanonicalJsonObject:
    if not isinstance(value, CanonicalJsonObject):
        _fail(
            label,
            "record must be produced by strict_json_loads or canonical_json_object;"
            " plain dictionaries are not a decode boundary",
        )
    return value


def parse_cohort_execution_capability(value: Any) -> CanonicalJsonObject:
    """Validate one ``cohort_execution_capability.v2`` record and return it in manifest field order.

    Closed structural validation plus ``capability_digest`` recomputation: a
    copied digest is never proof without rebuilding its source object.  The
    nested runtime namespace ref is parsed by its owning contract module, and
    the capability's ``workspace_id``/``provider_mode``/``policy_revision``
    must equal the nested ref values (derivable nested identity).
    """

    record = _require_decoder_typed(value, "cohort_execution_capability")
    _validate_object(record, COHORT_EXECUTION_CAPABILITY_V2_SCHEMA["fields"], "cohort_execution_capability")
    ordered = {field: record[field] for field in COHORT_EXECUTION_CAPABILITY_V2_ORDERED_FIELDS}
    namespace_ref = parse_agent_runtime_namespace_ref(ordered["runtime_namespace_ref"])
    for shared in ("workspace_id", "provider_mode", "policy_revision"):
        if ordered[shared] != namespace_ref[shared]:
            _fail(shared, "must equal the nested runtime namespace ref value")
    if ordered["capability_digest"] != compute_cohort_execution_capability_digest(ordered):
        _fail("capability_digest", "does not recompute from fields 1-18")
    return canonical_json_object(ordered)


def validate_cohort_execution_capability(value: Any) -> None:
    """Fail closed unless ``value`` is one exact ``cohort_execution_capability.v2`` record."""

    parse_cohort_execution_capability(value)


def parse_cohort_execution_envelope(value: Any) -> CanonicalJsonObject:
    """Validate one ``cohort_execution_envelope.v1`` record and return it in manifest field order.

    ``capability_digest`` must equal the nested capability record digest, the
    envelope's shared fields must equal the nested capability values
    (derivable nested identity), planned-lane ordinals must be the exact
    zero-based planning-v2 order, and ``envelope_digest`` must recompute over
    all preceding fields.
    """

    record = _require_decoder_typed(value, "cohort_execution_envelope")
    _validate_object(record, COHORT_EXECUTION_ENVELOPE_V1_SCHEMA["fields"], "cohort_execution_envelope")
    ordered = {field: record[field] for field in COHORT_EXECUTION_ENVELOPE_V1_ORDERED_FIELDS}
    capability = parse_cohort_execution_capability(ordered["capability"])
    if ordered["capability_digest"] != capability["capability_digest"]:
        _fail("capability_digest", "does not equal the nested capability record digest")
    for shared in ("workspace_id", "acquisition_run_id", "planning_manifest_digest", "provider_mode", "execution_generation"):
        if ordered[shared] != capability[shared]:
            _fail(shared, "must equal the nested capability value")
    if ordered["runtime_namespace_ref"] != capability["runtime_namespace_ref"]:
        _fail("runtime_namespace_ref", "must equal the nested capability runtime namespace ref")
    ordinals = [lane["ordinal"] for lane in ordered["ordered_planned_lane_refs"]]
    if ordinals != list(range(len(ordinals))):
        _fail("ordered_planned_lane_refs", "ordinals must be the exact zero-based planning-v2 order")
    if ordered["envelope_digest"] != compute_cohort_execution_envelope_digest(ordered):
        _fail("envelope_digest", "does not recompute over all preceding fields")
    return canonical_json_object(ordered)


def validate_cohort_execution_envelope(value: Any) -> None:
    """Fail closed unless ``value`` is one exact ``cohort_execution_envelope.v1`` record."""

    parse_cohort_execution_envelope(value)


def parse_cohort_execution_lane_result(value: Any) -> CanonicalJsonObject:
    """Validate one ``cohort_execution_lane_result.v2`` record and return it in manifest field order.

    Occurrence counts are an exact partition (``|O_i| = |A_i|+|R_i|+|T_i|``)
    and the accepted count equals the accepted digest enumeration.
    """

    record = _require_decoder_typed(value, "cohort_execution_lane_result")
    _validate_object(record, COHORT_EXECUTION_LANE_RESULT_V2_SCHEMA["fields"], "cohort_execution_lane_result")
    ordered = {field: record[field] for field in COHORT_EXECUTION_LANE_RESULT_V2_ORDERED_FIELDS}
    partition = (
        ordered["accepted_occurrence_count"] + ordered["rejected_occurrence_count"] + ordered["truncated_occurrence_count"]
    )
    if ordered["raw_occurrence_count"] != partition:
        _fail("raw_occurrence_count", "must equal accepted + rejected + truncated (exact occurrence partition)")
    if ordered["accepted_occurrence_count"] != len(ordered["ordered_accepted_occurrence_digests"]):
        _fail("accepted_occurrence_count", "must equal len(ordered_accepted_occurrence_digests)")
    if ordered["lane_result_digest"] != compute_cohort_execution_lane_result_digest(ordered):
        _fail("lane_result_digest", "does not recompute from the canonical lane-result fields")
    return canonical_json_object(ordered)


def validate_cohort_execution_lane_result(value: Any) -> None:
    """Fail closed unless ``value`` is one exact ``cohort_execution_lane_result.v2`` record."""

    parse_cohort_execution_lane_result(value)


def parse_cohort_candidate_member(value: Any) -> CanonicalJsonObject:
    """Validate one ``cohort_candidate_member.v1`` record and return it in manifest field order.

    ``public_profile_url`` is either absent from the canonical record or an
    HTTPS URL; the empty-string alias fails closed.
    """

    record = _require_decoder_typed(value, "cohort_candidate_member")
    _validate_object(record, COHORT_CANDIDATE_MEMBER_V1_SCHEMA["fields"], "cohort_candidate_member")
    ordered = {
        field: record[field] for field in COHORT_CANDIDATE_MEMBER_V1_ORDERED_FIELDS if field in record
    }
    if ordered["member_digest"] != compute_cohort_candidate_member_digest(ordered):
        _fail("member_digest", "does not recompute from the canonical member fields")
    return canonical_json_object(ordered)


def validate_cohort_candidate_member(value: Any) -> None:
    """Fail closed unless ``value`` is one exact ``cohort_candidate_member.v1`` record."""

    parse_cohort_candidate_member(value)


def parse_cohort_candidate_set(value: Any) -> CanonicalJsonObject:
    """Validate one ``cohort_candidate_set.v1`` header and return it in manifest field order.

    ``member_count`` must equal ``len(ordered_member_digests)`` and
    ``candidate_set_digest`` must recompute over all header fields except
    itself.
    """

    record = _require_decoder_typed(value, "cohort_candidate_set")
    _validate_object(record, COHORT_CANDIDATE_SET_V1_SCHEMA["fields"], "cohort_candidate_set")
    ordered = {field: record[field] for field in COHORT_CANDIDATE_SET_V1_ORDERED_FIELDS}
    if ordered["member_count"] != len(ordered["ordered_member_digests"]):
        _fail("member_count", "must equal len(ordered_member_digests)")
    if ordered["candidate_set_digest"] != compute_cohort_candidate_set_digest(ordered):
        _fail("candidate_set_digest", "does not recompute from the header fields")
    return canonical_json_object(ordered)


def validate_cohort_candidate_set(value: Any) -> None:
    """Fail closed unless ``value`` is one exact ``cohort_candidate_set.v1`` header."""

    parse_cohort_candidate_set(value)


def _parse_cohort_execution_result_fields(value: Any, candidate_set: Any | None) -> CanonicalJsonObject:
    record = _require_decoder_typed(value, "cohort_execution_result")
    _validate_object(record, COHORT_EXECUTION_RESULT_V2_SCHEMA["fields"], "cohort_execution_result")
    ordered = {field: record[field] for field in COHORT_EXECUTION_RESULT_V2_ORDERED_FIELDS}
    lane_results = [parse_cohort_execution_lane_result(item) for item in ordered["ordered_lane_results"]]
    if [item["ordinal"] for item in lane_results] != list(range(len(lane_results))):
        _fail("ordered_lane_results", "ordinals must be the exact zero-based planning-v2 order")
    if ordered["planned_lane_count"] != len(lane_results) or ordered["executed_lane_count"] != len(lane_results):
        _fail("lane_counts", "planned_lane_count == executed_lane_count == len(ordered_lane_results) violated")
    # Occurrence count equations: every aggregate equals the exact lane sum.
    aggregate_equations = (
        ("raw_occurrence_count", "raw_occurrence_count"),
        ("accepted_occurrence_count", "accepted_occurrence_count"),
        ("truncated_count", "truncated_occurrence_count"),
        ("rejected_unverified_count", "rejected_occurrence_count"),
    )
    for aggregate_field, lane_field in aggregate_equations:
        if ordered[aggregate_field] != sum(item[lane_field] for item in lane_results):
            _fail(aggregate_field, f"must equal the exact lane {lane_field} sum")
    if candidate_set is not None:
        # Member-set equations needing external state: bind the authoritative
        # candidate-set header exactly (unique_candidate_count = |M| =
        # candidate_set.member_count).
        parsed_set = parse_cohort_candidate_set(candidate_set)
        if parsed_set["candidate_set_digest"] != ordered["candidate_set_digest"]:
            _fail("candidate_set_digest", "must equal the authoritative candidate-set digest")
        if parsed_set["member_count"] != ordered["unique_candidate_count"]:
            _fail("unique_candidate_count", "must equal candidate_set.member_count")
    if ordered["result_digest"] != compute_cohort_execution_result_digest(ordered):
        _fail("result_digest", "does not recompute from the canonical result-v2 fields")
    return canonical_json_object(ordered)


def parse_cohort_execution_result(value: Any, *, candidate_set: Any) -> CanonicalJsonObject:
    """Validate one ``cohort_execution_result.v2`` record and return it in manifest field order.

    Lane completeness is exact: ``planned_lane_count == executed_lane_count ==
    len(ordered_lane_results)`` with the zero-based ordinal mapping, every
    aggregate count equals the exact lane sum, and the explicit
    ``candidate_set`` owner pin binds ``unique_candidate_count`` and
    ``candidate_set_digest`` to the authoritative header (never a
    ``missing_required_lane_count`` substitution).
    """

    if candidate_set is None:
        _fail("candidate_set", "the authoritative candidate-set owner pin is required")
    return _parse_cohort_execution_result_fields(value, candidate_set)


def validate_cohort_execution_result(value: Any, *, candidate_set: Any) -> None:
    """Fail closed unless ``value`` is one exact ``cohort_execution_result.v2`` record."""

    parse_cohort_execution_result(value, candidate_set=candidate_set)


def parse_cohort_execution_commit(value: Any, *, candidate_set: Any) -> CanonicalJsonObject:
    """Validate one ``cohort_execution_commit.v1`` record and return it in manifest field order.

    ``commit_contract_digest`` must be an exact copy of this contract's
    ``contract_digest`` (a constant self-binding, not a construction edge),
    ``execution_result_digest`` must equal the nested result record digest,
    every shared field must equal the nested execution result value,
    ``candidate_count`` must equal the nested result's
    ``unique_candidate_count`` and the authoritative candidate-set
    ``member_count`` (explicit owner pin), and ``commit_digest`` must
    recompute over every preceding contract field.
    """

    if candidate_set is None:
        _fail("candidate_set", "the authoritative candidate-set owner pin is required")
    record = _require_decoder_typed(value, "cohort_execution_commit")
    _validate_object(record, COHORT_EXECUTION_COMMIT_V1_SCHEMA["fields"], "cohort_execution_commit")
    ordered = {field: record[field] for field in COHORT_EXECUTION_COMMIT_V1_ORDERED_FIELDS}
    result = parse_cohort_execution_result(ordered["execution_result_json"], candidate_set=candidate_set)
    if ordered["execution_result_digest"] != result["result_digest"]:
        _fail("execution_result_digest", "does not equal the nested execution result digest")
    shared_fields = (
        "workspace_id",
        "acquisition_run_id",
        "execution_attempt_id",
        "execution_generation",
        "planning_manifest_digest",
        "capability_digest",
        "execution_envelope_digest",
        "candidate_set_digest",
    )
    for shared in shared_fields:
        if ordered[shared] != result[shared]:
            _fail(shared, "must equal the nested execution result value")
    parsed_set = parse_cohort_candidate_set(candidate_set)
    if ordered["candidate_count"] != result["unique_candidate_count"]:
        _fail("candidate_count", "must equal the nested execution result unique_candidate_count")
    if ordered["candidate_count"] != parsed_set["member_count"]:
        _fail("candidate_count", "must equal the authoritative candidate-set member_count")
    if ordered["commit_contract_digest"] != COHORT_EXECUTION_COMMIT_V1_CONTRACT_DIGEST:
        _fail("commit_contract_digest", "must equal this contract's contract_digest")
    if ordered["commit_digest"] != compute_cohort_execution_commit_digest(ordered):
        _fail("commit_digest", "does not recompute over every preceding contract field")
    return canonical_json_object(ordered)


def validate_cohort_execution_commit(value: Any, *, candidate_set: Any) -> None:
    """Fail closed unless ``value`` is one exact ``cohort_execution_commit.v1`` record."""

    parse_cohort_execution_commit(value, candidate_set=candidate_set)


_REF_PARSERS.update(
    {
        AGENT_RUNTIME_NAMESPACE_REF_SCHEMA_VERSION: parse_agent_runtime_namespace_ref,
        COHORT_EXECUTION_CAPABILITY_V2_SCHEMA_VERSION: parse_cohort_execution_capability,
        COHORT_EXECUTION_LANE_RESULT_V2_SCHEMA_VERSION: parse_cohort_execution_lane_result,
        # Internal ref resolution only: the public result/commit parsers
        # additionally require the explicit candidate-set owner pin.
        COHORT_EXECUTION_RESULT_V2_SCHEMA_VERSION: lambda value: _parse_cohort_execution_result_fields(value, None),
    }
)

def _namespace_ref_pin(schema: dict[str, Any], label: str) -> None:
    descriptor = next(
        (field for field in schema["fields"] if field["name"] == "runtime_namespace_ref"),
        None,
    )
    if (
        descriptor is None
        or descriptor.get("ref") != AGENT_RUNTIME_NAMESPACE_REF_SCHEMA_VERSION
        or descriptor.get("ref_digest") != AGENT_RUNTIME_NAMESPACE_REF_CONTRACT_DIGEST
    ):
        raise CohortExecutionContractError(f"cohort execution contract {label} invalid: namespace ref pin drift")


# Import-time drift guard: the capability and envelope bind the namespace ref
# contract as an immutable exact-version-and-digest reference.
_namespace_ref_pin(COHORT_EXECUTION_CAPABILITY_V2_SCHEMA, "capability.runtime_namespace_ref")
_namespace_ref_pin(COHORT_EXECUTION_ENVELOPE_V1_SCHEMA, "envelope.runtime_namespace_ref")


__all__ = [
    "AGENT_RUNTIME_NAMESPACE_REF_CONTRACT_DIGEST",
    "COHORT_CANDIDATE_MEMBER_V1_CONTRACT_DIGEST",
    "COHORT_CANDIDATE_MEMBER_V1_ORDERED_FIELDS",
    "COHORT_CANDIDATE_MEMBER_V1_OWNER",
    "COHORT_CANDIDATE_MEMBER_V1_SCHEMA",
    "COHORT_CANDIDATE_MEMBER_V1_SCHEMA_VERSION",
    "COHORT_CANDIDATE_SET_V1_CONTRACT_DIGEST",
    "COHORT_CANDIDATE_SET_V1_ORDERED_FIELDS",
    "COHORT_CANDIDATE_SET_V1_OWNER",
    "COHORT_CANDIDATE_SET_V1_SCHEMA",
    "COHORT_CANDIDATE_SET_V1_SCHEMA_VERSION",
    "COHORT_EXECUTION_CAPABILITY_V2_CONTRACT_DIGEST",
    "COHORT_EXECUTION_CAPABILITY_V2_ORDERED_FIELDS",
    "COHORT_EXECUTION_CAPABILITY_V2_OWNER",
    "COHORT_EXECUTION_CAPABILITY_V2_SCHEMA",
    "COHORT_EXECUTION_CAPABILITY_V2_SCHEMA_VERSION",
    "COHORT_EXECUTION_COMMIT_V1_CONTRACT_DIGEST",
    "COHORT_EXECUTION_COMMIT_V1_ORDERED_FIELDS",
    "COHORT_EXECUTION_COMMIT_V1_OWNER",
    "COHORT_EXECUTION_COMMIT_V1_SCHEMA",
    "COHORT_EXECUTION_COMMIT_V1_SCHEMA_VERSION",
    "COHORT_EXECUTION_ENVELOPE_V1_CONTRACT_DIGEST",
    "COHORT_EXECUTION_ENVELOPE_V1_ORDERED_FIELDS",
    "COHORT_EXECUTION_ENVELOPE_V1_OWNER",
    "COHORT_EXECUTION_ENVELOPE_V1_SCHEMA",
    "COHORT_EXECUTION_ENVELOPE_V1_SCHEMA_VERSION",
    "COHORT_EXECUTION_LANE_RESULT_V2_CONTRACT_DIGEST",
    "COHORT_EXECUTION_LANE_RESULT_V2_ORDERED_FIELDS",
    "COHORT_EXECUTION_LANE_RESULT_V2_OWNER",
    "COHORT_EXECUTION_LANE_RESULT_V2_SCHEMA",
    "COHORT_EXECUTION_LANE_RESULT_V2_SCHEMA_VERSION",
    "COHORT_EXECUTION_RESULT_V2_CONTRACT_DIGEST",
    "COHORT_EXECUTION_RESULT_V2_ORDERED_FIELDS",
    "COHORT_EXECUTION_RESULT_V2_OWNER",
    "COHORT_EXECUTION_RESULT_V2_SCHEMA",
    "COHORT_EXECUTION_RESULT_V2_SCHEMA_VERSION",
    "CONTRACT_SCHEMAS",
    "CohortExecutionContractError",
    "REJECTED_COHORT_LITERALS",
    "RETAINED_COHORT_HISTORY_LITERALS",
    "RETAINED_LOOKUP_FORBIDDEN",
    "canonical_json",
    "compute_cohort_candidate_member_digest",
    "compute_cohort_candidate_set_digest",
    "compute_cohort_execution_capability_digest",
    "compute_cohort_execution_commit_digest",
    "compute_cohort_execution_envelope_digest",
    "compute_cohort_execution_lane_result_digest",
    "compute_cohort_execution_result_digest",
    "contract_digest",
    "parse_cohort_candidate_member",
    "parse_cohort_candidate_set",
    "parse_cohort_execution_capability",
    "parse_cohort_execution_commit",
    "parse_cohort_execution_envelope",
    "parse_cohort_execution_lane_result",
    "parse_cohort_execution_result",
    "validate_cohort_candidate_member",
    "validate_cohort_candidate_set",
    "validate_cohort_execution_capability",
    "validate_cohort_execution_commit",
    "validate_cohort_execution_envelope",
    "validate_cohort_execution_lane_result",
    "validate_cohort_execution_result",
]
