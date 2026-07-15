"""Sole compiler from CohortSelection into deterministic provider lanes."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Protocol
from urllib import parse

from .cohort_selection import (
    COHORT_SELECTION_REGISTRY_VERSION,
    cohort_selection_digest,
    cohort_selection_registry_digest,
    explicit_cohort_selection,
)
from .person_identity import resolve_person_identity_key
from .query_signal_knowledge import (
    ROLE_BUCKET_KNOWLEDGE,
    role_bucket_function_ids,
    role_bucket_role_hints,
    role_buckets_from_text,
)
from .runtime_environment import (
    ISOLATED_RUNTIME_ENVIRONMENTS,
    NON_LIVE_PROVIDER_MODES,
    current_runtime_environment,
    validate_runtime_environment,
)

COHORT_PROVIDER_MANIFEST_VERSION = "cohort_provider_manifest.v1"
COHORT_PROVIDER = "harvest_profile_search"
COHORT_EXECUTION_NOT_READY = "cohort_selection_execution_not_ready"
COHORT_EXECUTION_CAPABILITY_VERSION = "cohort_execution_capability.v1"
COHORT_EXECUTION_CAPABILITY_OWNER = "cohort_runtime"
COHORT_NON_LIVE_RUNTIME_POLICY_VERSION = "cohort_non_live_runtime.v1"
COHORT_HEADLINE_ROLE_PROOF_VERIFIER_ID = "cohort_headline_role_classifier"
COHORT_HEADLINE_ROLE_PROOF_VERIFIER_REVISION = "cohort_headline_role_classifier.v1"
COHORT_PUBLIC_HEADLINE_SOURCE = "harvest_profile_search.headline"
COHORT_CANONICAL_PROFILE_URL_FIELD = "cohort_canonical_profile_url"
DEFAULT_COHORT_RESULT_LIMIT = 25
MAX_COHORT_PROVIDER_LANES = 10


@dataclass(frozen=True, slots=True)
class CohortProviderCompilationError(ValueError):
    code: str
    field: str = ""
    detail: str = ""

    def __str__(self) -> str:
        return f"{self.code}: {self.field}" if self.field else self.code

    def to_result(self) -> dict[str, Any]:
        result: dict[str, Any] = {"status": "invalid", "reason": self.code}
        if self.field:
            result["field"] = self.field
        if self.detail:
            result["detail"] = self.detail
        return result


@dataclass(frozen=True, slots=True)
class CohortProviderExecutionError(RuntimeError):
    """Stable whole-manifest failure; partial lane rows are never published."""

    code: str
    lane_id: str = ""
    completed_lane_ids: tuple[str, ...] = ()
    detail: str = ""

    def __str__(self) -> str:
        return f"{self.code}: {self.lane_id}" if self.lane_id else self.code

    def to_result(self) -> dict[str, Any]:
        result: dict[str, Any] = {
            "status": "failed",
            "reason": self.code,
            "completed_lane_ids": list(self.completed_lane_ids),
        }
        if self.lane_id:
            result["lane_id"] = self.lane_id
        if self.detail:
            result["detail"] = self.detail
        return result


@dataclass(frozen=True, slots=True)
class CohortExecutionCapability:
    """Execution-owner input kept separate from the serializable manifest."""

    policy_revision: str
    provider_mode: str
    runtime_namespace: str
    max_provider_calls: int = MAX_COHORT_PROVIDER_LANES
    max_provider_items: int = DEFAULT_COHORT_RESULT_LIMIT
    max_output_candidates: int = DEFAULT_COHORT_RESULT_LIMIT
    role_proof_verifier_id: str = ""
    role_proof_verifier_revision: str = ""
    owner: str = COHORT_EXECUTION_CAPABILITY_OWNER
    schema_version: str = COHORT_EXECUTION_CAPABILITY_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != COHORT_EXECUTION_CAPABILITY_VERSION:
            raise CohortProviderCompilationError(
                "cohort_execution_capability_invalid",
                "schema_version",
            )
        if self.owner != COHORT_EXECUTION_CAPABILITY_OWNER:
            raise CohortProviderCompilationError(
                "cohort_execution_capability_invalid",
                "owner",
            )
        if not str(self.policy_revision or "").strip():
            raise CohortProviderCompilationError(
                "cohort_execution_capability_invalid",
                "policy_revision",
            )
        normalized_provider_mode = str(self.provider_mode or "").strip().lower()
        if normalized_provider_mode not in NON_LIVE_PROVIDER_MODES:
            raise CohortProviderCompilationError(
                "cohort_execution_capability_invalid",
                "provider_mode",
            )
        object.__setattr__(self, "provider_mode", normalized_provider_mode)
        normalized_runtime_namespace = _canonical_runtime_namespace(self.runtime_namespace)
        if not normalized_runtime_namespace:
            raise CohortProviderCompilationError(
                "cohort_execution_capability_invalid",
                "runtime_namespace",
            )
        object.__setattr__(self, "runtime_namespace", normalized_runtime_namespace)
        for field_name in ("max_provider_calls", "max_provider_items", "max_output_candidates"):
            value = getattr(self, field_name)
            if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                raise CohortProviderCompilationError(
                    "cohort_execution_capability_invalid",
                    field_name,
                )
        proof_id = str(self.role_proof_verifier_id or "").strip()
        proof_revision = str(self.role_proof_verifier_revision or "").strip()
        if bool(proof_id) != bool(proof_revision):
            raise CohortProviderCompilationError(
                "cohort_execution_capability_invalid",
                "role_proof_verifier",
            )

    def to_record(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "owner": self.owner,
            "policy_revision": self.policy_revision,
            "provider_mode": self.provider_mode,
            "runtime_namespace": self.runtime_namespace,
            "max_provider_calls": self.max_provider_calls,
            "max_provider_items": self.max_provider_items,
            "max_output_candidates": self.max_output_candidates,
            "role_proof_verifier_id": self.role_proof_verifier_id,
            "role_proof_verifier_revision": self.role_proof_verifier_revision,
        }

    @classmethod
    def from_record(cls, value: dict[str, Any]) -> "CohortExecutionCapability":
        record = dict(value or {})
        expected_fields = {
            "schema_version",
            "owner",
            "policy_revision",
            "provider_mode",
            "runtime_namespace",
            "max_provider_calls",
            "max_provider_items",
            "max_output_candidates",
            "role_proof_verifier_id",
            "role_proof_verifier_revision",
        }
        if set(record) != expected_fields:
            raise CohortProviderCompilationError(
                "cohort_execution_capability_invalid",
                "fields",
            )
        return cls(
            schema_version=str(record.get("schema_version") or ""),
            owner=str(record.get("owner") or ""),
            policy_revision=str(record.get("policy_revision") or ""),
            provider_mode=str(record.get("provider_mode") or ""),
            runtime_namespace=str(record.get("runtime_namespace") or ""),
            max_provider_calls=_require_positive_int(
                record.get("max_provider_calls"),
                "max_provider_calls",
                code="cohort_execution_capability_invalid",
            ),
            max_provider_items=_require_positive_int(
                record.get("max_provider_items"),
                "max_provider_items",
                code="cohort_execution_capability_invalid",
            ),
            max_output_candidates=_require_positive_int(
                record.get("max_output_candidates"),
                "max_output_candidates",
                code="cohort_execution_capability_invalid",
            ),
            role_proof_verifier_id=str(record.get("role_proof_verifier_id") or ""),
            role_proof_verifier_revision=str(record.get("role_proof_verifier_revision") or ""),
        )


@dataclass(frozen=True, slots=True)
class VerifiedCohortRoleProof:
    role_bucket_ids: tuple[str, ...]
    evidence_digest: str


class CohortRoleProofVerifier(Protocol):
    verifier_id: str
    verifier_revision: str

    def verify(self, row: dict[str, Any]) -> VerifiedCohortRoleProof | None: ...


class CohortHeadlineRoleProofVerifier:
    """Deterministic proof owner for the provider's public headline field."""

    verifier_id = COHORT_HEADLINE_ROLE_PROOF_VERIFIER_ID
    verifier_revision = COHORT_HEADLINE_ROLE_PROOF_VERIFIER_REVISION

    @staticmethod
    def verify(row: dict[str, Any]) -> VerifiedCohortRoleProof | None:
        normalized_row = dict(row or {})
        if str(normalized_row.get("public_headline_source") or "").strip() != COHORT_PUBLIC_HEADLINE_SOURCE:
            return None
        raw_headline = normalized_row.get("public_headline")
        if not isinstance(raw_headline, str):
            return None
        headline = " ".join(raw_headline.split()).strip()
        if not headline:
            return None
        role_bucket_ids = tuple(role_buckets_from_text(headline))
        if not role_bucket_ids:
            return None
        return VerifiedCohortRoleProof(
            role_bucket_ids=role_bucket_ids,
            evidence_digest=_sha256_json(
                {
                    "verifier_id": COHORT_HEADLINE_ROLE_PROOF_VERIFIER_ID,
                    "verifier_revision": COHORT_HEADLINE_ROLE_PROOF_VERIFIER_REVISION,
                    "public_headline_source": COHORT_PUBLIC_HEADLINE_SOURCE,
                    "headline": headline,
                    "role_bucket_ids": list(role_bucket_ids),
                }
            ),
        )


def resolve_effective_role_targeting(
    request_payload: dict[str, Any] | None,
    *,
    legacy_resolved_role_buckets: Iterable[str] = (),
    legacy_function_target_groups: Iterable[dict[str, Any]] = (),
) -> dict[str, Any]:
    """Resolve role authority once while preserving the absent-object path.

    A user-explicit object is the complete role authority, including the empty
    list (all roles).  No text, category, facet, model, or refinement-derived
    role may be added.  With no user object, callers receive their existing
    legacy resolution byte-for-byte apart from defensive container copies.
    """

    cohort = explicit_cohort_selection(request_payload)
    if cohort is None or str(cohort.get("source") or "") != "user_explicit":
        return {
            "authority": "legacy",
            "inference_allowed": True,
            "all_roles": False,
            "resolved_role_buckets": [str(item) for item in legacy_resolved_role_buckets],
            "function_target_groups": [dict(item) for item in legacy_function_target_groups],
        }

    role_bucket_ids = [str(item) for item in list(cohort.get("role_bucket_ids") or [])]
    groups = [_role_target_group(role_bucket_id) for role_bucket_id in role_bucket_ids]
    return {
        "authority": "user_explicit",
        "inference_allowed": False,
        "all_roles": not role_bucket_ids,
        "resolved_role_buckets": role_bucket_ids,
        "function_target_groups": groups,
    }


class CohortProviderCompiler:
    """Compile, validate, and combine one cohort provider manifest."""

    def compile(
        self,
        request_payload: dict[str, Any] | None,
        *,
        base_filter_hints: dict[str, Any] | None = None,
        execution_capability: CohortExecutionCapability | None = None,
        requested_result_limit: int = DEFAULT_COHORT_RESULT_LIMIT,
    ) -> dict[str, Any]:
        cohort = explicit_cohort_selection(request_payload)
        if cohort is None or str(cohort.get("source") or "") != "user_explicit":
            raise CohortProviderCompilationError(
                "cohort_selection_required",
                "cohort_selection",
            )
        if (
            isinstance(requested_result_limit, bool)
            or not isinstance(requested_result_limit, int)
            or requested_result_limit <= 0
        ):
            raise CohortProviderCompilationError(
                "cohort_provider_budget_invalid",
                "requested_result_limit",
            )

        selection_digest = cohort_selection_digest(cohort)
        roles = [str(item) for item in list(cohort["role_bucket_ids"])]
        statuses = [str(item) for item in list(cohort["employment_statuses"])]
        role_match = str(cohort["role_match"])
        canonical_base_filters = _canonical_base_filter_hints(base_filter_hints)
        lane_count = len(statuses) * (len(roles) or 1)
        lane_limits = _allocate_lane_item_limits(requested_result_limit, lane_count)
        lanes: list[dict[str, Any]] = []
        role_axis: list[str | None] = [*roles] if roles else [None]
        lane_index = 0
        for status in statuses:
            for role_bucket_id in role_axis:
                lanes.append(
                    self._compile_lane(
                        status=status,
                        role_bucket_id=role_bucket_id,
                        selection_digest=selection_digest,
                        base_filter_hints=canonical_base_filters,
                        provider_item_limit=lane_limits[lane_index],
                    )
                )
                lane_index += 1

        proof_required = role_match == "all" and bool(roles)
        execution_blocker = ""
        if execution_capability is None:
            execution_blocker = COHORT_EXECUTION_NOT_READY
        elif lane_count > execution_capability.max_provider_calls:
            execution_blocker = "cohort_provider_call_budget_exceeded"
        elif requested_result_limit > execution_capability.max_provider_items:
            execution_blocker = "cohort_provider_item_budget_exceeded"
        elif requested_result_limit > execution_capability.max_output_candidates:
            execution_blocker = "cohort_provider_output_budget_exceeded"
        elif proof_required and not execution_capability.role_proof_verifier_id:
            execution_blocker = "cohort_selection_all_role_proof_unavailable"
        capability_record = execution_capability.to_record() if execution_capability is not None else {}
        manifest: dict[str, Any] = {
            "schema_version": COHORT_PROVIDER_MANIFEST_VERSION,
            "source": "cohort_provider_compiler",
            "provider": COHORT_PROVIDER,
            "registry_version": COHORT_SELECTION_REGISTRY_VERSION,
            "registry_digest": cohort_selection_registry_digest(),
            "cohort_selection_digest": selection_digest,
            "role_match": role_match,
            "execution_ready": not bool(execution_blocker),
            "execution_blocker": execution_blocker,
            "compiler_inputs": {
                "cohort_selection": dict(cohort),
                "base_filter_hints": canonical_base_filters,
                "execution_capability": capability_record,
                "requested_result_limit": requested_result_limit,
            },
            "budget": {
                "planned_provider_calls": lane_count,
                "planned_provider_items": sum(lane_limits),
                "max_output_candidates": requested_result_limit,
                "lane_item_limits": lane_limits,
            },
            "aggregation": {
                "operation": ("intersection_by_status_then_union" if proof_required else "union_dedupe"),
                "dedupe_identity_order": [
                    "person_identity_key",
                    "linkedin_profile_key",
                    "candidate_id",
                ],
                "required_role_bucket_ids": roles if proof_required else [],
                "verified_post_filter_required": proof_required,
                "proof_owner": (
                    {
                        "verifier_id": execution_capability.role_proof_verifier_id,
                        "verifier_revision": execution_capability.role_proof_verifier_revision,
                    }
                    if proof_required and execution_capability is not None
                    else {}
                ),
            },
            "lanes": lanes,
        }
        manifest["manifest_digest"] = _sha256_json(manifest)
        return manifest

    def assert_execution_ready(
        self,
        manifest: dict[str, Any],
        *,
        execution_capability: CohortExecutionCapability | None,
    ) -> None:
        self._validate_manifest(manifest)
        expected_capability = dict(dict(manifest.get("compiler_inputs") or {}).get("execution_capability") or {})
        actual_capability = execution_capability.to_record() if execution_capability is not None else {}
        if expected_capability != actual_capability:
            raise CohortProviderCompilationError(
                "cohort_execution_capability_mismatch",
                "execution_capability",
            )
        if not bool(manifest.get("execution_ready")):
            raise CohortProviderCompilationError(
                str(manifest.get("execution_blocker") or COHORT_EXECUTION_NOT_READY),
                "cohort_selection",
            )

    @staticmethod
    def assert_role_proof_verifier(
        manifest: dict[str, Any],
        *,
        role_proof_verifier: CohortRoleProofVerifier | None,
    ) -> None:
        aggregation = dict(manifest.get("aggregation") or {})
        if not bool(aggregation.get("verified_post_filter_required")):
            return
        proof_owner = dict(aggregation.get("proof_owner") or {})
        if (
            role_proof_verifier is None
            or str(getattr(role_proof_verifier, "verifier_id", "")) != str(proof_owner.get("verifier_id") or "")
            or str(getattr(role_proof_verifier, "verifier_revision", ""))
            != str(proof_owner.get("verifier_revision") or "")
            or not callable(getattr(role_proof_verifier, "verify", None))
        ):
            raise CohortProviderCompilationError(
                "cohort_selection_all_role_proof_unavailable",
                "role_proof_verifier",
            )

    @staticmethod
    def validate_lane_result_rows(
        rows: list[dict[str, Any]],
        *,
        lane_id: str,
    ) -> list[dict[str, Any]]:
        """Validate one lane before the connector is allowed to call the next."""

        return _normalize_lane_rows(rows, lane_id=lane_id)

    def combine_lane_results(
        self,
        manifest: dict[str, Any],
        lane_results: dict[str, list[dict[str, Any]]],
        *,
        execution_capability: CohortExecutionCapability | None,
        role_proof_verifier: CohortRoleProofVerifier | None = None,
    ) -> dict[str, Any]:
        self.assert_execution_ready(
            manifest,
            execution_capability=execution_capability,
        )
        lanes = [dict(item) for item in list(manifest.get("lanes") or [])]
        expected_lane_ids = [str(lane.get("lane_id") or "") for lane in lanes]
        if set(lane_results) != set(expected_lane_ids) or len(lane_results) != len(expected_lane_ids):
            raise CohortProviderExecutionError(
                "cohort_provider_lane_results_incomplete",
            )
        normalized_lane_results = {
            lane_id: _normalize_lane_rows(lane_results[lane_id], lane_id=lane_id) for lane_id in expected_lane_ids
        }
        aggregation = dict(manifest.get("aggregation") or {})
        required_roles = [str(item) for item in list(aggregation.get("required_role_bucket_ids") or [])]
        output_limit = int(dict(manifest.get("budget") or {}).get("max_output_candidates") or 0)
        if str(aggregation.get("operation") or "") != "intersection_by_status_then_union":
            union_rows = _ordered_union_rows(lanes, normalized_lane_results)
            return _combined_result(
                union_rows,
                output_limit=output_limit,
                rejected_unverified_count=0,
                missing_required_lane_count=0,
            )

        self.assert_role_proof_verifier(
            manifest,
            role_proof_verifier=role_proof_verifier,
        )
        assert role_proof_verifier is not None
        proof_owner = dict(aggregation.get("proof_owner") or {})

        rows: list[dict[str, Any]] = []
        accepted_by_identity: dict[str, dict[str, Any]] = {}
        rejected_unverified_count = 0
        missing_required_lane_count = 0
        statuses = list(dict.fromkeys(str(lane.get("employment_status") or "") for lane in lanes))
        for status in statuses:
            status_lanes = [lane for lane in lanes if str(lane.get("employment_status") or "") == status]
            indexes = [
                _rows_by_identity(normalized_lane_results[str(lane.get("lane_id") or "")]) for lane in status_lanes
            ]
            if not indexes:
                continue
            shared_identities = set(indexes[0])
            all_identities = set(indexes[0])
            for index in indexes[1:]:
                shared_identities.intersection_update(index)
                all_identities.update(index)
            missing_required_lane_count += len(all_identities - shared_identities)
            for identity in sorted(shared_identities):
                evidence_rows = [index[identity] for index in indexes]
                try:
                    proofs = [role_proof_verifier.verify(dict(row)) for row in evidence_rows]
                except Exception as exc:
                    raise CohortProviderExecutionError(
                        "cohort_role_proof_verification_failed",
                        detail=type(exc).__name__,
                    ) from exc
                proof_roles = {role for proof in proofs if proof is not None for role in _validated_role_proof(proof)}
                if not set(required_roles).issubset(proof_roles):
                    rejected_unverified_count += 1
                    continue
                merged = dict(evidence_rows[0])
                merged["normalized_role_bucket_ids"] = [role for role in required_roles if role in proof_roles]
                merged["cohort_role_proof"] = {
                    "verifier_id": str(proof_owner.get("verifier_id") or ""),
                    "verifier_revision": str(proof_owner.get("verifier_revision") or ""),
                    "evidence_digests": sorted(
                        {
                            str(proof.evidence_digest or "")
                            for proof in proofs
                            if proof is not None and str(proof.evidence_digest or "")
                        }
                    ),
                }
                merged["cohort_lane_membership"] = [_lane_membership_entry(lane) for lane in status_lanes]
                accepted = accepted_by_identity.get(identity)
                if accepted is None:
                    accepted_by_identity[identity] = merged
                    rows.append(merged)
                    continue
                _extend_lane_membership(
                    accepted,
                    list(merged["cohort_lane_membership"]),
                )
                accepted_proof = dict(accepted.get("cohort_role_proof") or {})
                accepted_proof["evidence_digests"] = sorted(
                    {
                        *list(accepted_proof.get("evidence_digests") or []),
                        *list(dict(merged.get("cohort_role_proof") or {}).get("evidence_digests") or []),
                    }
                )
                accepted["cohort_role_proof"] = accepted_proof
        return _combined_result(
            rows,
            output_limit=output_limit,
            rejected_unverified_count=rejected_unverified_count,
            missing_required_lane_count=missing_required_lane_count,
        )

    def _compile_lane(
        self,
        *,
        status: str,
        role_bucket_id: str | None,
        selection_digest: str,
        base_filter_hints: dict[str, Any] | None,
        provider_item_limit: int,
    ) -> dict[str, Any]:
        filter_hints = _base_lane_filters(base_filter_hints, status=status)
        role_hints: list[str] = []
        function_ids: list[str] = []
        if role_bucket_id:
            role_hints = role_bucket_role_hints([role_bucket_id])
            function_ids = role_bucket_function_ids([role_bucket_id])
            if role_hints:
                filter_hints["job_titles"] = role_hints
            if function_ids:
                filter_hints["function_ids"] = function_ids
        lane_role = role_bucket_id or "all_roles"
        lane_id = f"cohort_{status}_{lane_role}_{selection_digest[:12]}"
        lane: dict[str, Any] = {
            "lane_id": lane_id,
            "provider": COHORT_PROVIDER,
            "operation": "profile_search",
            "employment_status": status,
            "role_bucket_id": role_bucket_id or "",
            "provider_item_limit": provider_item_limit,
            "provider_payload": {
                "query_text": "",
                "employment_status": status,
                "filter_hints": filter_hints,
            },
            "post_filter": {
                "employment_status": status,
                "required_role_bucket_ids": [role_bucket_id] if role_bucket_id else [],
            },
        }
        lane["lane_digest"] = _sha256_json(lane)
        return lane

    def _validate_manifest(self, manifest: dict[str, Any]) -> None:
        candidate = dict(manifest or {})
        if candidate.get("schema_version") != COHORT_PROVIDER_MANIFEST_VERSION:
            raise CohortProviderCompilationError(
                "cohort_provider_manifest_invalid",
                "schema_version",
            )
        compiler_inputs = dict(candidate.get("compiler_inputs") or {})
        if set(compiler_inputs) != {
            "cohort_selection",
            "base_filter_hints",
            "execution_capability",
            "requested_result_limit",
        }:
            raise CohortProviderCompilationError(
                "cohort_provider_manifest_invalid",
                "compiler_inputs",
            )
        capability_record = dict(compiler_inputs.get("execution_capability") or {})
        capability = CohortExecutionCapability.from_record(capability_record) if capability_record else None
        try:
            expected = self.compile(
                {"cohort_selection": dict(compiler_inputs.get("cohort_selection") or {})},
                base_filter_hints=dict(compiler_inputs.get("base_filter_hints") or {}),
                execution_capability=capability,
                requested_result_limit=_require_positive_int(
                    compiler_inputs.get("requested_result_limit"),
                    "requested_result_limit",
                    code="cohort_provider_budget_invalid",
                ),
            )
        except (CohortProviderCompilationError, TypeError, ValueError) as exc:
            raise CohortProviderCompilationError(
                "cohort_provider_manifest_invalid",
                "compiler_inputs",
            ) from exc
        if candidate != expected:
            raise CohortProviderCompilationError(
                "cohort_provider_manifest_semantic_mismatch",
                "manifest",
            )


def cohort_execution_not_ready_result(
    request_payload: dict[str, Any] | None,
    *,
    base_filter_hints: dict[str, Any] | None = None,
    runtime_dir: str | Path | None = None,
) -> dict[str, Any] | None:
    cohort = explicit_cohort_selection(request_payload)
    if cohort is None or str(cohort.get("source") or "") != "user_explicit":
        return None
    execution_capability = cohort_execution_capability_for_runtime(runtime_dir=runtime_dir)
    if execution_capability is None:
        return cohort_execution_unavailable_result(
            request_payload,
            base_filter_hints=base_filter_hints,
        )
    manifest = CohortProviderCompiler().compile(
        request_payload,
        base_filter_hints=base_filter_hints,
        execution_capability=execution_capability,
    )
    if bool(manifest.get("execution_ready")):
        return None
    return {
        "status": "invalid",
        "reason": str(manifest.get("execution_blocker") or COHORT_EXECUTION_NOT_READY),
        "cohort_provider_manifest": manifest,
    }


def cohort_execution_unavailable_result(
    request_payload: dict[str, Any] | None,
    *,
    base_filter_hints: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    """Return the stable gate for a surface that cannot execute cohort lanes."""

    cohort = explicit_cohort_selection(request_payload)
    if cohort is None or str(cohort.get("source") or "") != "user_explicit":
        return None
    manifest = CohortProviderCompiler().compile(
        request_payload,
        base_filter_hints=base_filter_hints,
    )
    return {
        "status": "invalid",
        "reason": str(manifest.get("execution_blocker") or COHORT_EXECUTION_NOT_READY),
        "cohort_provider_manifest": manifest,
    }


def cohort_execution_capability_for_runtime(
    *,
    runtime_dir: str | Path | None = None,
) -> CohortExecutionCapability | None:
    """Issue the server-owned capability for the currently isolated runtime.

    Non-live providers are deterministic and do not create a billed remote run,
    so they may execute the complete cohort manifest while the durable live-lane
    checkpoint is still being built.  Live intentionally receives no capability:
    callers cannot promote it with a request flag or a serialized manifest edit.
    """

    runtime = current_runtime_environment(runtime_dir=runtime_dir)
    if (
        runtime.provider_mode not in NON_LIVE_PROVIDER_MODES
        or runtime.runtime_dir is None
        or not runtime.runtime_dir.is_dir()
        or runtime.is_production
        or runtime.name not in ISOLATED_RUNTIME_ENVIRONMENTS
    ):
        return None
    try:
        validate_runtime_environment(
            runtime_dir=runtime.runtime_dir,
            provider_mode=runtime.provider_mode,
            runtime_environment=runtime.name,
        )
    except RuntimeError:
        return None
    runtime_namespace = _canonical_runtime_namespace(runtime.runtime_dir)
    if not runtime_namespace:
        return None
    return CohortExecutionCapability(
        policy_revision=f"{COHORT_NON_LIVE_RUNTIME_POLICY_VERSION}:{runtime.provider_mode}",
        provider_mode=runtime.provider_mode,
        runtime_namespace=runtime_namespace,
        role_proof_verifier_id=COHORT_HEADLINE_ROLE_PROOF_VERIFIER_ID,
        role_proof_verifier_revision=COHORT_HEADLINE_ROLE_PROOF_VERIFIER_REVISION,
    )


def _role_target_group(role_bucket_id: str) -> dict[str, Any]:
    role_hints = role_bucket_role_hints([role_bucket_id])
    return {
        "group_id": f"cohort_role:{role_bucket_id}",
        "role_bucket_id": role_bucket_id,
        "primary_role_hint": role_hints[0] if role_hints else "",
        "role_hints": role_hints,
        "function_ids": role_bucket_function_ids([role_bucket_id]),
        "source": "user_explicit",
    }


def _canonical_base_filter_hints(value: dict[str, Any] | None) -> dict[str, list[str]]:
    role_keys = {
        "function_ids",
        "exclude_function_ids",
        "job_titles",
        "exclude_job_titles",
    }
    filters: dict[str, list[str]] = {}
    for key, raw_values in dict(value or {}).items():
        normalized_key = str(key)
        if normalized_key in role_keys:
            continue
        normalized_values = _normalized_strings(raw_values)
        if normalized_key in {"keywords", "scope_keywords"}:
            normalized_values = [item for item in normalized_values if not _is_role_like_value(item)]
        if normalized_values:
            filters[normalized_key] = normalized_values
    return filters


def _base_lane_filters(value: dict[str, Any] | None, *, status: str) -> dict[str, list[str]]:
    filters = {str(key): list(items) for key, items in dict(value or {}).items()}
    current_companies = list(filters.get("current_companies") or [])
    past_companies = list(filters.get("past_companies") or [])
    if status == "former":
        filters.pop("current_companies", None)
        if not past_companies and current_companies:
            filters["past_companies"] = current_companies
    else:
        filters.pop("past_companies", None)
        if not current_companies and past_companies:
            filters["current_companies"] = past_companies
    return filters


def _allocate_lane_item_limits(total: int, lane_count: int) -> list[int]:
    if lane_count <= 0 or lane_count > MAX_COHORT_PROVIDER_LANES or total < lane_count:
        raise CohortProviderCompilationError(
            "cohort_provider_budget_too_small",
            "requested_result_limit",
        )
    quotient, remainder = divmod(total, lane_count)
    return [quotient + (1 if index < remainder else 0) for index in range(lane_count)]


def _require_positive_int(value: Any, field: str, *, code: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise CohortProviderCompilationError(code, field)
    return value


def _normalized_strings(value: Any) -> list[str]:
    if isinstance(value, str):
        items = [value]
    elif isinstance(value, (list, tuple, set)):
        items = list(value)
    else:
        return []
    return list(dict.fromkeys(str(item).strip() for item in items if str(item).strip()))


def _is_role_like_value(value: str) -> bool:
    token = _normalized_role_token(value)
    if not token:
        return False
    for role_id, spec in ROLE_BUCKET_KNOWLEDGE.items():
        candidates = (
            role_id,
            spec.get("selectable_label"),
            *tuple(spec.get("aliases") or ()),
            *tuple(spec.get("role_hints") or ()),
        )
        for candidate in candidates:
            candidate_token = _normalized_role_token(candidate)
            if candidate_token and f" {candidate_token} " in f" {token} ":
                return True
    return False


def _normalized_role_token(value: Any) -> str:
    return " ".join(str(value or "").strip().lower().replace("-", " ").replace("_", " ").split())


def _ordered_union_rows(
    lanes: list[dict[str, Any]],
    lane_results: dict[str, list[dict[str, Any]]],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    accepted_by_identity: dict[str, dict[str, Any]] = {}
    for lane in lanes:
        for row in list(lane_results.get(str(lane.get("lane_id") or ""), []) or []):
            identity = _candidate_identity(row)
            membership = _lane_membership_entry(lane)
            accepted = accepted_by_identity.get(identity)
            if accepted is not None:
                _extend_lane_membership(accepted, [membership])
                continue
            accepted = dict(row)
            accepted["cohort_lane_membership"] = [membership]
            accepted_by_identity[identity] = accepted
            rows.append(accepted)
    return rows


def _lane_membership_entry(lane: dict[str, Any]) -> dict[str, str]:
    return {
        "lane_id": str(lane.get("lane_id") or ""),
        "employment_status": str(lane.get("employment_status") or ""),
        "role_bucket_id": str(lane.get("role_bucket_id") or ""),
    }


def _extend_lane_membership(row: dict[str, Any], memberships: list[dict[str, str]]) -> None:
    existing = [dict(item) for item in list(row.get("cohort_lane_membership") or [])]
    seen = {str(item.get("lane_id") or "") for item in existing}
    for membership in memberships:
        lane_id = str(membership.get("lane_id") or "")
        if not lane_id or lane_id in seen:
            continue
        existing.append(dict(membership))
        seen.add(lane_id)
    row["cohort_lane_membership"] = existing


def _rows_by_identity(rows: Iterable[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    return {_candidate_identity(row): dict(row) for row in rows}


def _candidate_identity(row: dict[str, Any]) -> str:
    identity = resolve_person_identity_key(
        person_identity_key=_validated_cohort_identity_key(row.get("person_identity_key")),
        profile_url_key="",
        linkedin_url=canonical_cohort_profile_url(row),
        candidate_identity_key=_validated_cohort_identity_key(row.get("candidate_identity_key")),
        candidate_id=str(row.get("candidate_id") or row.get("id") or ""),
    )
    if not identity:
        raise CohortProviderExecutionError(
            "cohort_provider_candidate_identity_missing",
        )
    normalized_identity = identity.lower().rstrip("/")
    if normalized_identity.startswith("linkedin:https://www.linkedin.com/"):
        normalized_identity = normalized_identity.replace(
            "linkedin:https://www.linkedin.com/",
            "linkedin:https://linkedin.com/",
            1,
        )
    return normalized_identity


def canonical_cohort_profile_url(row: dict[str, Any]) -> str:
    """Return the only LinkedIn URL accepted by Cohort identity resolution.

    Raw non-LinkedIn URLs are discarded. A valid public identifier is promoted
    to the same canonical LinkedIn URL used by the person-identity owner.
    """

    return _candidate_linkedin_url(dict(row or {}))


def _candidate_linkedin_url(row: dict[str, Any]) -> str:
    for raw_url in (
        row.get("linkedin_url"),
        row.get("profile_url"),
        row.get("url"),
        row.get("profile_url_key"),
    ):
        normalized = _validated_linkedin_profile_url(raw_url)
        if normalized:
            return normalized
    for raw_identifier in (
        row.get("username"),
        row.get("public_identifier"),
        row.get("slug"),
        row.get("profile_url_key"),
    ):
        public_identifier = str(raw_identifier or "").strip()
        if public_identifier and re.fullmatch(r"[A-Za-z0-9._~-]+", public_identifier):
            return f"https://linkedin.com/in/{public_identifier}"
    return ""


def _validated_linkedin_profile_url(value: Any) -> str:
    raw_value = str(value or "").strip()
    if not raw_value:
        return ""
    if "://" not in raw_value and raw_value.lower().startswith(("linkedin.com/", "www.linkedin.com/")):
        raw_value = f"https://{raw_value}"
    parsed = parse.urlsplit(raw_value)
    hostname = str(parsed.hostname or "").lower()
    if hostname not in {"linkedin.com", "www.linkedin.com"}:
        return ""
    path_parts = [item for item in parsed.path.split("/") if item]
    if len(path_parts) < 2 or path_parts[0].lower() != "in":
        return ""
    public_identifier = parse.unquote(path_parts[1]).strip()
    if re.fullmatch(r"[A-Za-z0-9._~-]+", public_identifier) is None:
        return ""
    return f"https://linkedin.com/in/{public_identifier}"


def _validated_cohort_identity_key(value: Any) -> str:
    identity = str(value or "").strip()
    if not identity.startswith("linkedin:"):
        return identity
    profile_value = identity.removeprefix("linkedin:").strip()
    if re.fullmatch(r"[A-Za-z0-9._~-]+", profile_value):
        return f"linkedin:{profile_value}"
    linkedin_url = _validated_linkedin_profile_url(profile_value)
    return f"linkedin:{linkedin_url}" if linkedin_url else ""


def _normalize_lane_rows(rows: Any, *, lane_id: str) -> list[dict[str, Any]]:
    if not isinstance(rows, list):
        raise CohortProviderExecutionError(
            "cohort_provider_lane_rows_invalid",
            lane_id=lane_id,
        )
    normalized: list[dict[str, Any]] = []
    for item in rows:
        if not isinstance(item, dict):
            raise CohortProviderExecutionError(
                "cohort_provider_lane_rows_invalid",
                lane_id=lane_id,
            )
        row = dict(item)
        row.pop(COHORT_CANONICAL_PROFILE_URL_FIELD, None)
        _candidate_identity(row)
        canonical_profile_url = canonical_cohort_profile_url(row)
        if canonical_profile_url:
            row[COHORT_CANONICAL_PROFILE_URL_FIELD] = canonical_profile_url
        row.pop("normalized_role_bucket_ids", None)
        row.pop("cohort_role_proof", None)
        row.pop("cohort_lane_membership", None)
        normalized.append(row)
    return normalized


def _validated_role_proof(proof: VerifiedCohortRoleProof) -> list[str]:
    if not isinstance(proof, VerifiedCohortRoleProof) or not str(proof.evidence_digest or "").strip():
        return []
    valid_roles = set(ROLE_BUCKET_KNOWLEDGE)
    normalized = _normalized_strings(list(proof.role_bucket_ids))
    return [role for role in normalized if role in valid_roles]


def _combined_result(
    rows: list[dict[str, Any]],
    *,
    output_limit: int,
    rejected_unverified_count: int,
    missing_required_lane_count: int,
) -> dict[str, Any]:
    bounded_rows = rows[:output_limit]
    payload: dict[str, Any] = {
        "rows": bounded_rows,
        "candidate_count": len(bounded_rows),
        "truncated_count": max(0, len(rows) - len(bounded_rows)),
        "rejected_unverified_count": rejected_unverified_count,
        "missing_required_lane_count": missing_required_lane_count,
    }
    payload["result_digest"] = _sha256_json(payload)
    return payload


def _sha256_json(payload: dict[str, Any]) -> str:
    encoded = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _canonical_runtime_namespace(value: Any) -> str:
    raw_value = str(value or "").strip()
    if not raw_value:
        return ""
    try:
        return str(Path(raw_value).expanduser().resolve(strict=False))
    except OSError:
        return str(Path(raw_value).expanduser().absolute())
