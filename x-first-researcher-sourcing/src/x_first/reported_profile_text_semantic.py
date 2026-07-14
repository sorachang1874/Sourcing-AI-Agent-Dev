"""Offline semantic review for model-mediated, unverified reported profile text.

This module deliberately does not accept an X profile snapshot.  It reviews
only caller-supplied text observations whose source trust remains
``grok_model_mediated_unverified_text``.  The output can feed a separately
governed verification queue, but it cannot establish platform identity,
physical-region experience, employment, eligibility, ranking, outreach, or a
canonical product write.

There is no provider, transport, credential, filesystem writer, retry, or live
execution path in this module.
"""

from __future__ import annotations

import copy
import hashlib
import json
import math
import re
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from x_first import profile_bio_semantic_v2 as semantic

OBSERVATION_SCHEMA_VERSION = "x.reported_profile_text.observation.v1"
BATCH_REQUEST_SCHEMA_VERSION = "x.reported_profile_text.semantic.batch_request.v1"
MODEL_OUTPUT_SCHEMA_VERSION = "x.reported_profile_text.semantic.model_output.v1"
ITEM_REVIEW_SCHEMA_VERSION = "x.reported_profile_text.semantic.item_review.v1"
BATCH_CLOSURE_VERSION = "x.reported_profile_text.semantic.batch_closure.runtime.v1"

SOURCE_KIND = "grok_model_mediated_unverified_text"
SOURCE_TRUST = "model_mediated_unverified"
EXECUTION_MODE = "offline_fixture"
SEMANTIC_VOCABULARY_VERSION = "x.profile.bio_semantic.v2.2"
EVIDENCE_BASIS = "model_mediated_unverified_text_only"
VERIFICATION_STATUS = "unverified_reported_profile_text_proposal"

MAX_ITEMS = 10_000
MAX_TEXT_CHARACTERS = 2_000
MAX_TOTAL_TEXT_CHARACTERS = 20_000_000
MAX_BATCH_CANONICAL_BYTES = 32_000_000
MAX_MODEL_OUTPUT_CANONICAL_BYTES = 262_144
MAX_VALIDATION_DEPTH = 64
MAX_VALIDATION_NODES = 500_000
MAX_PROPOSALS_PER_ITEM = 12

OPERATIONAL_LIMITS = {
    "max_items": MAX_ITEMS,
    "max_text_characters_per_item": MAX_TEXT_CHARACTERS,
    "max_total_text_characters": MAX_TOTAL_TEXT_CHARACTERS,
    "max_batch_canonical_bytes": MAX_BATCH_CANONICAL_BYTES,
    "max_model_output_canonical_bytes": MAX_MODEL_OUTPUT_CANONICAL_BYTES,
    "max_validation_depth": MAX_VALIDATION_DEPTH,
    "max_validation_nodes": MAX_VALIDATION_NODES,
    "max_proposals_per_item": MAX_PROPOSALS_PER_ITEM,
}

OBSERVATION_AUTHORITY = {
    "x_profile_snapshot_claimed": False,
    "stable_platform_identity_verified": False,
    "automatic_identity_merge_authorized": False,
}
BATCH_AUTHORITY = {
    "provider_or_network_allowed": False,
    "external_facts_allowed": False,
    "protected_identity_inference_allowed": False,
    "physical_region_inference_allowed": False,
    "discovery_or_ranking_allowed": False,
    "eligibility_decision_allowed": False,
    "canonical_employment_allowed": False,
    "canonical_write_allowed": False,
    "outreach_allowed": False,
}
MODEL_AUTHORITY = copy.deepcopy(semantic.MODEL_AUTHORITY)
REVIEW_AUTHORITY = {
    "source_bound_x_profile_accepted": False,
    "stable_platform_identity_verified": False,
    "external_facts_accepted": False,
    "protected_identity_inferred": False,
    "physical_region_inferred": False,
    "discovery_or_ranking_authorized": False,
    "eligibility_decided": False,
    "canonical_employment_confirmed": False,
    "canonical_write_authorized": False,
    "outreach_authorized": False,
    "verification_queue_only": True,
}

SEMANTIC_POLICY = {
    "execution_mode": EXECUTION_MODE,
    "intended_model_id": semantic.MODEL_ID,
    "reasoning_effort": "low",
    "semantic_vocabulary_version": SEMANTIC_VOCABULARY_VERSION,
    "model_output_schema_version": MODEL_OUTPUT_SCHEMA_VERSION,
    "professional_experience_proxy_policy_version": semantic.PROXY_POLICY_VERSION,
    "professional_experience_proxy_policy_sha256": semantic.CANONICAL_PROXY_POLICY_SHA256,
    "strict_structured_output": True,
    "fallback_model": None,
}

_OBSERVATION_KEYS = {
    "schema_version",
    "observation_id",
    "source_kind",
    "candidate_ref",
    "campaign_result_sha256",
    "candidate_row_sha256",
    "text",
    "text_sha256",
    "reported_platform_user_id",
    "reported_platform_user_id_status",
    "authority",
}
_BATCH_REQUEST_KEYS = {
    "schema_version",
    "batch_id",
    "source_manifest",
    "semantic_policy",
    "operational_limits",
    "observations",
    "authority",
}
_SOURCE_MANIFEST_KEYS = {
    "campaign_result_sha256",
    "observation_count",
    "observations_sha256",
}
_MODEL_OUTPUT_KEYS = {
    "schema_version",
    "item_request_id",
    "observation_id",
    "text_sha256",
    "verdict",
    "proposals",
    "authority",
}
_MODEL_PROPOSAL_KEYS = {
    "proposal_type",
    "relation_state",
    "span_start",
    "span_end",
    "excerpt",
    "reason_codes",
    "reason",
    "reason_source",
    "confidence",
    "evidence_basis",
    "requires_independent_verification",
}
_ITEM_REVIEW_KEYS = {
    "schema_version",
    "status",
    "batch_id",
    "item_request_id",
    "observation_id",
    "observation_sha256",
    "text_sha256",
    "source_trust",
    "reported_platform_user_id_diagnostic",
    "model_output_sha256",
    "verdict",
    "proposals",
    "professional_experience_proxy_policy",
    "professional_experience_proxy_rollup",
    "error_codes",
    "authority",
}
_REVIEW_PROPOSAL_KEYS = {
    "proposal_id",
    "proposal_type",
    "relation_state",
    "span_start",
    "span_end",
    "excerpt",
    "excerpt_sha256",
    "reason_codes",
    "reason",
    "reason_source",
    "confidence",
    "evidence_basis",
    "requires_independent_verification",
    "verification_status",
}

_SHA256_RE = re.compile(r"[0-9a-f]{64}")
_OBSERVATION_ID_RE = re.compile(r"xrpt_obs_[0-9a-f]{24}")
_CANDIDATE_REF_RE = re.compile(r"xrpt_candidate_[0-9a-f]{24}")
_BATCH_ID_RE = re.compile(r"xrpt_batch_[0-9a-f]{24}")
_ITEM_REQUEST_ID_RE = re.compile(r"xrpt_item_[0-9a-f]{24}")
_PROPOSAL_ID_RE = re.compile(r"xrpt_prop_[0-9a-f]{24}")
_PLATFORM_USER_ID_RE = re.compile(r"[1-9][0-9]{1,24}")


def canonical_json(value: Any) -> str:
    """Return deterministic strict JSON without accepting non-finite numbers."""

    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    )


def canonical_sha256(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def text_sha256(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def load_json(path: str | Path) -> Any:
    return json.loads(
        Path(path).read_text(encoding="utf-8"),
        parse_constant=lambda value: (_ for _ in ()).throw(ValueError(f"non_finite:{value}")),
    )


def _is_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _is_utf8_scalar_text(value: Any, *, minimum: int = 0, maximum: int) -> bool:
    if not isinstance(value, str) or not minimum <= len(value) <= maximum:
        return False
    try:
        value.encode("utf-8")
    except UnicodeEncodeError:
        return False
    return True


def _exact_keys(value: Any, expected: set[str], *, path: str, errors: list[str]) -> bool:
    if not isinstance(value, dict):
        errors.append(f"{path}: must be an object")
        return False
    observed = set(value)
    if observed != expected:
        missing = sorted(expected - observed)
        unexpected = sorted(observed - expected)
        if missing:
            errors.append(f"{path}: missing keys {missing}")
        if unexpected:
            errors.append(f"{path}: unexpected keys {unexpected}")
        return False
    return True


def _scan_json(value: Any, *, max_depth: int, max_nodes: int) -> list[str]:
    """Iteratively reject hostile or non-JSON trees before hashes or equality."""

    errors: list[str] = []
    pending: list[tuple[Any, int]] = [(value, 0)]
    nodes = 0
    while pending:
        current, depth = pending.pop()
        nodes += 1
        if nodes > max_nodes:
            return ["nested_node_budget_exceeded"]
        if depth > max_depth:
            return ["nested_depth_budget_exceeded"]
        if current is None or isinstance(current, (str, bool)):
            if isinstance(current, str):
                try:
                    current.encode("utf-8")
                except UnicodeEncodeError:
                    return ["non_unicode_scalar_string"]
            continue
        if _is_int(current):
            continue
        if isinstance(current, float):
            return ["non_integer_number" if math.isfinite(current) else "non_finite_number"]
        if isinstance(current, list):
            if nodes + len(pending) + len(current) > max_nodes:
                return ["nested_node_budget_exceeded"]
            pending.extend((child, depth + 1) for child in current)
            continue
        if isinstance(current, dict):
            if any(not isinstance(key, str) for key in current):
                return ["non_string_object_key"]
            if nodes + len(pending) + (2 * len(current)) > max_nodes:
                return ["nested_node_budget_exceeded"]
            for key, child in current.items():
                pending.append((key, depth + 1))
                pending.append((child, depth + 1))
            continue
        errors.append("non_json_value")
        return errors
    return errors


def _canonical_byte_size(value: Any) -> int | None:
    try:
        return len(canonical_json(value).encode("utf-8"))
    except (TypeError, ValueError, RecursionError, UnicodeEncodeError):
        return None


def _bounded_canonical_sha256(value: Any, *, maximum_bytes: int) -> tuple[str | None, str]:
    """Hash canonical JSON incrementally and stop at the byte ceiling.

    The returned status is ``valid``, ``ceiling_exceeded`` or ``invalid``.
    This avoids building a second unbounded canonical string for hostile model
    output and gives validation one bounded serialization pass.
    """

    try:
        encoder = json.JSONEncoder(
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        digest = hashlib.sha256()
        total = 0
        for chunk in encoder.iterencode(value):
            encoded = chunk.encode("utf-8")
            total += len(encoded)
            if total > maximum_bytes:
                return None, "ceiling_exceeded"
            digest.update(encoded)
    except (TypeError, ValueError, RecursionError, UnicodeEncodeError):
        return None, "invalid"
    return digest.hexdigest(), "valid"


def _valid_sha(value: Any) -> bool:
    return isinstance(value, str) and _SHA256_RE.fullmatch(value) is not None and value != "0" * 64


def _json_equal(left: Any, right: Any) -> bool:
    return semantic.json_type_strict_equal(left, right)


def validate_observation(observation: Any) -> list[str]:
    errors = [f"$.validation: {error}" for error in _scan_json(
        observation,
        max_depth=MAX_VALIDATION_DEPTH,
        max_nodes=MAX_VALIDATION_NODES,
    )]
    if errors or not _exact_keys(observation, _OBSERVATION_KEYS, path="$", errors=errors):
        return errors
    if observation["schema_version"] != OBSERVATION_SCHEMA_VERSION:
        errors.append("$.schema_version: unsupported")
    if not isinstance(observation["observation_id"], str) or _OBSERVATION_ID_RE.fullmatch(
        observation["observation_id"]
    ) is None:
        errors.append("$.observation_id: invalid")
    if observation["source_kind"] != SOURCE_KIND:
        errors.append("$.source_kind: must remain model-mediated and unverified")
    if not isinstance(observation["candidate_ref"], str) or _CANDIDATE_REF_RE.fullmatch(
        observation["candidate_ref"]
    ) is None:
        errors.append("$.candidate_ref: invalid opaque reference")
    for field in ("campaign_result_sha256", "candidate_row_sha256", "text_sha256"):
        if not _valid_sha(observation[field]):
            errors.append(f"$.{field}: invalid")
    text = observation["text"]
    if not _is_utf8_scalar_text(text, minimum=1, maximum=MAX_TEXT_CHARACTERS) or not text.strip():
        errors.append("$.text: invalid or technical ceiling exceeded")
    elif observation["text_sha256"] != text_sha256(text):
        errors.append("$.text_sha256: mismatch")
    campaign_sha = observation["campaign_result_sha256"]
    candidate_row_sha = observation["candidate_row_sha256"]
    text_digest = observation["text_sha256"]
    if _valid_sha(campaign_sha) and _valid_sha(candidate_row_sha):
        expected_candidate_ref = candidate_ref_for(
            campaign_result_sha256=campaign_sha,
            candidate_row_sha256=candidate_row_sha,
        )
        if observation["candidate_ref"] != expected_candidate_ref:
            errors.append("$.candidate_ref: does not bind campaign and candidate row")
    if _valid_sha(campaign_sha) and _valid_sha(candidate_row_sha) and _valid_sha(text_digest):
        expected_observation_id = observation_id_for(
            campaign_result_sha256=campaign_sha,
            candidate_row_sha256=candidate_row_sha,
            text_sha256_value=text_digest,
        )
        if observation["observation_id"] != expected_observation_id:
            errors.append("$.observation_id: does not bind campaign, candidate row and text")
    reported_id = observation["reported_platform_user_id"]
    reported_status = observation["reported_platform_user_id_status"]
    if reported_id is None:
        if reported_status != "absent":
            errors.append("$.reported_platform_user_id_status: null ID requires absent")
    elif not isinstance(reported_id, str) or _PLATFORM_USER_ID_RE.fullmatch(reported_id) is None:
        errors.append("$.reported_platform_user_id: invalid diagnostic value")
    elif reported_status != "model_mediated_unverified":
        errors.append("$.reported_platform_user_id_status: reported ID must remain unverified")
    if not _json_equal(observation["authority"], OBSERVATION_AUTHORITY):
        errors.append("$.authority: source and identity authority must remain false")
    return errors


def observation_id_for(*, campaign_result_sha256: str, candidate_row_sha256: str, text_sha256_value: str) -> str:
    digest = canonical_sha256(
        {
            "campaign_result_sha256": campaign_result_sha256,
            "candidate_row_sha256": candidate_row_sha256,
            "text_sha256": text_sha256_value,
        }
    )
    return f"xrpt_obs_{digest[:24]}"


def candidate_ref_for(*, campaign_result_sha256: str, candidate_row_sha256: str) -> str:
    digest = canonical_sha256(
        {
            "campaign_result_sha256": campaign_result_sha256,
            "candidate_row_sha256": candidate_row_sha256,
        }
    )
    return f"xrpt_candidate_{digest[:24]}"


def build_observation(
    *,
    campaign_result_sha256: str,
    candidate_row_sha256: str,
    text: str,
    reported_platform_user_id: str | None = None,
) -> dict[str, Any]:
    text_digest = text_sha256(text)
    observation = {
        "schema_version": OBSERVATION_SCHEMA_VERSION,
        "observation_id": observation_id_for(
            campaign_result_sha256=campaign_result_sha256,
            candidate_row_sha256=candidate_row_sha256,
            text_sha256_value=text_digest,
        ),
        "source_kind": SOURCE_KIND,
        "candidate_ref": candidate_ref_for(
            campaign_result_sha256=campaign_result_sha256,
            candidate_row_sha256=candidate_row_sha256,
        ),
        "campaign_result_sha256": campaign_result_sha256,
        "candidate_row_sha256": candidate_row_sha256,
        "text": text,
        "text_sha256": text_digest,
        "reported_platform_user_id": reported_platform_user_id,
        "reported_platform_user_id_status": (
            "absent" if reported_platform_user_id is None else "model_mediated_unverified"
        ),
        "authority": copy.deepcopy(OBSERVATION_AUTHORITY),
    }
    errors = validate_observation(observation)
    if errors:
        raise ValueError(f"observation_invalid:{errors[0]}")
    return observation


def item_request_id_for(*, batch_id: str, observation: Mapping[str, Any]) -> str:
    digest = canonical_sha256(
        {
            "batch_id": batch_id,
            "observation_id": observation["observation_id"],
            "text_sha256": observation["text_sha256"],
        }
    )
    return f"xrpt_item_{digest[:24]}"


def build_batch_request(observations: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    snapshots = copy.deepcopy(list(observations))
    if not snapshots:
        raise ValueError("batch_request_invalid:observations_empty")
    campaign_digest = snapshots[0].get("campaign_result_sha256") if isinstance(snapshots[0], dict) else None
    manifest_seed = {
        "campaign_result_sha256": campaign_digest,
        "observation_count": len(snapshots),
        "observations_sha256": canonical_sha256(snapshots),
    }
    batch_id = f"xrpt_batch_{canonical_sha256(manifest_seed)[:24]}"
    request = {
        "schema_version": BATCH_REQUEST_SCHEMA_VERSION,
        "batch_id": batch_id,
        "source_manifest": manifest_seed,
        "semantic_policy": copy.deepcopy(SEMANTIC_POLICY),
        "operational_limits": copy.deepcopy(OPERATIONAL_LIMITS),
        "observations": snapshots,
        "authority": copy.deepcopy(BATCH_AUTHORITY),
    }
    errors = validate_batch_request(request)
    if errors:
        raise ValueError(f"batch_request_invalid:{errors[0]}")
    return request


def validate_batch_request(request: Any) -> list[str]:
    traversal = _scan_json(request, max_depth=MAX_VALIDATION_DEPTH, max_nodes=MAX_VALIDATION_NODES)
    if traversal:
        return [f"$.validation: {error}" for error in traversal]
    size = _canonical_byte_size(request)
    if size is None:
        return ["$.validation: request_not_canonical_json"]
    if size > MAX_BATCH_CANONICAL_BYTES:
        return ["$.validation: batch_canonical_byte_ceiling_exceeded"]
    errors: list[str] = []
    if not _exact_keys(request, _BATCH_REQUEST_KEYS, path="$", errors=errors):
        return errors
    if request["schema_version"] != BATCH_REQUEST_SCHEMA_VERSION:
        errors.append("$.schema_version: unsupported")
    if not isinstance(request["batch_id"], str) or _BATCH_ID_RE.fullmatch(request["batch_id"]) is None:
        errors.append("$.batch_id: invalid")
    manifest = request["source_manifest"]
    if _exact_keys(manifest, _SOURCE_MANIFEST_KEYS, path="$.source_manifest", errors=errors):
        if not _valid_sha(manifest["campaign_result_sha256"]):
            errors.append("$.source_manifest.campaign_result_sha256: invalid")
        if not _is_int(manifest["observation_count"]) or manifest["observation_count"] < 1:
            errors.append("$.source_manifest.observation_count: invalid")
        if not _valid_sha(manifest["observations_sha256"]):
            errors.append("$.source_manifest.observations_sha256: invalid")
    if not _json_equal(request["semantic_policy"], SEMANTIC_POLICY):
        errors.append("$.semantic_policy: must equal the closed offline policy")
    if not _json_equal(request["operational_limits"], OPERATIONAL_LIMITS):
        errors.append("$.operational_limits: technical ceilings are versioned and immutable")
    observations = request["observations"]
    if not isinstance(observations, list) or not observations:
        errors.append("$.observations: must be a non-empty array")
        observations = []
    if len(observations) > MAX_ITEMS:
        errors.append("$.observations: technical item ceiling exceeded")
    total_text = 0
    ids: set[str] = set()
    candidates: set[str] = set()
    for index, observation in enumerate(observations):
        for error in validate_observation(observation):
            errors.append(f"$.observations[{index}]{error[1:]}")
        if isinstance(observation, dict):
            identifier = observation.get("observation_id")
            candidate = observation.get("candidate_ref")
            if isinstance(identifier, str) and identifier in ids:
                errors.append(f"$.observations[{index}].observation_id: duplicate")
            if isinstance(identifier, str):
                ids.add(identifier)
            if isinstance(candidate, str) and candidate in candidates:
                errors.append(f"$.observations[{index}].candidate_ref: duplicate candidate in one batch")
            if isinstance(candidate, str):
                candidates.add(candidate)
            text = observation.get("text")
            if isinstance(text, str):
                total_text += len(text)
            if isinstance(manifest, dict) and observation.get("campaign_result_sha256") != manifest.get(
                "campaign_result_sha256"
            ):
                errors.append(f"$.observations[{index}].campaign_result_sha256: manifest mismatch")
    if total_text > MAX_TOTAL_TEXT_CHARACTERS:
        errors.append("$.observations: total text technical ceiling exceeded")
    if isinstance(manifest, dict):
        if manifest.get("observation_count") != len(observations):
            errors.append("$.source_manifest.observation_count: denominator mismatch")
        try:
            observed_sha = canonical_sha256(observations)
        except (TypeError, ValueError, RecursionError, UnicodeEncodeError):
            observed_sha = None
        if manifest.get("observations_sha256") != observed_sha:
            errors.append("$.source_manifest.observations_sha256: mismatch")
        if _valid_sha(manifest.get("campaign_result_sha256")):
            expected_batch = f"xrpt_batch_{canonical_sha256(manifest)[:24]}"
            if request["batch_id"] != expected_batch:
                errors.append("$.batch_id: does not bind the source manifest")
    if not _json_equal(request["authority"], BATCH_AUTHORITY):
        errors.append("$.authority: all live and downstream authority must remain false")
    return errors


def _proposal_semantic_identity(proposal: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "proposal_type": proposal.get("proposal_type"),
        "relation_state": proposal.get("relation_state"),
        "span_start": proposal.get("span_start"),
        "span_end": proposal.get("span_end"),
        "excerpt": proposal.get("excerpt"),
        "reason_codes": proposal.get("reason_codes"),
    }


def _build_observation_index_validated(
    batch_request: Mapping[str, Any],
) -> dict[str, Mapping[str, Any]]:
    """Build the unique O(1) lookup owned by an already-valid batch."""

    return {
        observation["observation_id"]: observation
        for observation in batch_request["observations"]
    }


def _is_strict_index_member(
    observation: Any,
    *,
    observation_index: Mapping[str, Mapping[str, Any]],
) -> bool:
    if not isinstance(observation, dict):
        return False
    observation_id = observation.get("observation_id")
    if not isinstance(observation_id, str):
        return False
    indexed = observation_index.get(observation_id)
    return indexed is not None and _json_equal(indexed, observation)


def _validate_model_output_validated(
    model_output: Any,
    *,
    batch_request: Mapping[str, Any],
    observation: Any,
    observation_index: Mapping[str, Mapping[str, Any]],
) -> list[str]:
    traversal = _scan_json(model_output, max_depth=MAX_VALIDATION_DEPTH, max_nodes=MAX_VALIDATION_NODES)
    if traversal:
        return [f"$.validation: {error}" for error in traversal]
    _, canonical_status = _bounded_canonical_sha256(
        model_output,
        maximum_bytes=MAX_MODEL_OUTPUT_CANONICAL_BYTES,
    )
    if canonical_status == "invalid":
        return ["$.validation: model_output_not_canonical_json"]
    if canonical_status == "ceiling_exceeded":
        return ["$.validation: model_output_canonical_byte_ceiling_exceeded"]
    errors: list[str] = []
    if not _is_strict_index_member(observation, observation_index=observation_index):
        return ["$.observation: must be the unique strict-equal member of batch observations"]
    if not _exact_keys(model_output, _MODEL_OUTPUT_KEYS, path="$", errors=errors):
        return errors
    expected_bindings = {
        "schema_version": MODEL_OUTPUT_SCHEMA_VERSION,
        "item_request_id": item_request_id_for(batch_id=batch_request["batch_id"], observation=observation),
        "observation_id": observation["observation_id"],
        "text_sha256": observation["text_sha256"],
    }
    for key, expected in expected_bindings.items():
        if model_output[key] != expected:
            errors.append(f"$.{key}: source binding mismatch")
    verdict = model_output["verdict"]
    if not isinstance(verdict, str) or verdict not in semantic.VERDICTS:
        errors.append("$.verdict: unsupported")
    proposals = model_output["proposals"]
    if not isinstance(proposals, list) or len(proposals) > MAX_PROPOSALS_PER_ITEM:
        errors.append("$.proposals: must be a bounded array")
        proposals = []
    if verdict == "proposals_available" and not proposals:
        errors.append("$.verdict: proposals_available requires proposals")
    if isinstance(verdict, str) and verdict in {"no_supported_professional_context", "abstained"} and proposals:
        errors.append("$.verdict: empty verdict requires no proposals")
    text = observation["text"]
    signatures: set[str] = set()
    for index, proposal in enumerate(proposals):
        path = f"$.proposals[{index}]"
        if not _exact_keys(proposal, _MODEL_PROPOSAL_KEYS, path=path, errors=errors):
            continue
        proposal_type = proposal["proposal_type"]
        relation = proposal["relation_state"]
        if not isinstance(proposal_type, str) or proposal_type not in semantic.PROPOSAL_TYPES:
            errors.append(f"{path}.proposal_type: unsupported")
        if not isinstance(relation, str) or relation not in semantic.RELATION_STATES:
            errors.append(f"{path}.relation_state: unsupported")
        if proposal_type == "professional_affiliation" and relation == "not_applicable":
            errors.append(f"{path}.relation_state: affiliation requires a claimed timing state")
        if proposal_type != "professional_affiliation" and relation != "not_applicable":
            errors.append(f"{path}.relation_state: non-affiliation must be not_applicable")
        start = proposal["span_start"]
        end = proposal["span_end"]
        excerpt = proposal["excerpt"]
        if not _is_int(start) or not _is_int(end) or start < 0 or end <= start or end > len(text):
            errors.append(f"{path}.span: invalid")
        elif excerpt != text[start:end]:
            errors.append(f"{path}.excerpt: must equal the exact Unicode text span")
        if not _is_utf8_scalar_text(excerpt, minimum=1, maximum=500) or not excerpt.strip():
            errors.append(f"{path}.excerpt: invalid")
        reason_codes = proposal["reason_codes"]
        expected_codes = (
            semantic.REASON_CODES_BY_SEMANTIC_STATE.get((proposal_type, relation))
            if isinstance(proposal_type, str) and isinstance(relation, str)
            else None
        )
        if (
            not isinstance(reason_codes, list)
            or len(reason_codes) != 1
            or any(not isinstance(code, str) for code in reason_codes)
            or frozenset(reason_codes) != expected_codes
        ):
            errors.append(f"{path}.reason_codes: must match the closed semantic state")
        expected_reason = (
            semantic.EXPLANATION_TEMPLATES_BY_SEMANTIC_STATE.get((proposal_type, relation))
            if isinstance(proposal_type, str) and isinstance(relation, str)
            else None
        )
        if proposal["reason"] != expected_reason:
            errors.append(f"{path}.reason: must equal the closed v2.2 explanation")
        if proposal["reason_source"] != semantic.REASON_SOURCE:
            errors.append(f"{path}.reason_source: invalid")
        if not isinstance(proposal["confidence"], str) or proposal["confidence"] not in semantic.CONFIDENCE_STATES:
            errors.append(f"{path}.confidence: unsupported")
        if proposal["evidence_basis"] != EVIDENCE_BASIS:
            errors.append(f"{path}.evidence_basis: must remain model-mediated text only")
        if proposal["requires_independent_verification"] is not True:
            errors.append(f"{path}.requires_independent_verification: must be true")
        try:
            signature = canonical_json(_proposal_semantic_identity(proposal))
        except (TypeError, ValueError, RecursionError, UnicodeEncodeError):
            errors.append(f"{path}: not canonical JSON")
        else:
            if signature in signatures:
                errors.append(f"{path}: duplicate semantic proposal")
            signatures.add(signature)
    if not _json_equal(model_output["authority"], MODEL_AUTHORITY):
        errors.append("$.authority: model authority must remain false")
    return errors


def validate_model_output(
    model_output: Any,
    *,
    batch_request: Any,
    observation: Any,
) -> list[str]:
    """Strict one-off public validation with a fresh trusted batch index."""

    if validate_batch_request(batch_request):
        return ["$.batch_request: invalid"]
    if validate_observation(observation):
        return ["$.observation: invalid"]
    observation_index = _build_observation_index_validated(batch_request)
    return _validate_model_output_validated(
        model_output,
        batch_request=batch_request,
        observation=observation,
        observation_index=observation_index,
    )


def build_model_output(
    *,
    batch_request: Mapping[str, Any],
    observation: Mapping[str, Any],
    verdict: str,
    proposals: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    output = {
        "schema_version": MODEL_OUTPUT_SCHEMA_VERSION,
        "item_request_id": item_request_id_for(batch_id=batch_request["batch_id"], observation=observation),
        "observation_id": observation["observation_id"],
        "text_sha256": observation["text_sha256"],
        "verdict": verdict,
        "proposals": copy.deepcopy(list(proposals)),
        "authority": copy.deepcopy(MODEL_AUTHORITY),
    }
    errors = validate_model_output(output, batch_request=batch_request, observation=observation)
    if errors:
        raise ValueError(f"model_output_invalid:{errors[0]}")
    return output


def _proposal_id(*, observation: Mapping[str, Any], proposal: Mapping[str, Any]) -> str:
    digest = canonical_sha256(
        {
            "observation_id": observation["observation_id"],
            "text_sha256": observation["text_sha256"],
            "semantic_identity": _proposal_semantic_identity(proposal),
        }
    )
    return f"xrpt_prop_{digest[:24]}"


def _review_proposals(
    *,
    observation: Mapping[str, Any],
    model_proposals: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    reviews: list[dict[str, Any]] = []
    for source in model_proposals:
        proposal = copy.deepcopy(dict(source))
        proposal["proposal_id"] = _proposal_id(observation=observation, proposal=source)
        proposal["excerpt_sha256"] = text_sha256(proposal["excerpt"])
        proposal["verification_status"] = VERIFICATION_STATUS
        reviews.append(proposal)
    return reviews


def _empty_rollup() -> dict[str, Any]:
    return {
        "status": "none",
        "regions": {
            "china": {"strength": "none", "contributing_proposals": []},
            "asia": {"strength": "none", "contributing_proposals": []},
        },
    }


def _proxy_rollup(proposals: Sequence[Mapping[str, Any]], *, proxy_policy: Mapping[str, Any]) -> dict[str, Any]:
    rollup = _empty_rollup()
    strength_order = {strength: index for index, strength in enumerate(proxy_policy["strength_order"])}
    mappings = proxy_policy["proposal_mappings"]
    for proposal in proposals:
        mapping = mappings.get(proposal["proposal_type"])
        if not isinstance(mapping, dict):
            continue
        for region in mapping["regions"]:
            target = rollup["regions"][region]
            contribution = {
                "proposal_id": proposal["proposal_id"],
                "proposal_type": proposal["proposal_type"],
                "confidence": proposal["confidence"],
                "mapped_strength": mapping["strength"],
            }
            target["contributing_proposals"].append(contribution)
            if strength_order[mapping["strength"]] > strength_order[target["strength"]]:
                target["strength"] = mapping["strength"]
    for region in ("china", "asia"):
        rollup["regions"][region]["contributing_proposals"].sort(key=lambda value: value["proposal_id"])
    if any(rollup["regions"][region]["strength"] != "none" for region in ("china", "asia")):
        rollup["status"] = "unverified_model_derived"
    return rollup


def _reported_id_diagnostic(observation: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "status": observation["reported_platform_user_id_status"],
        "value": observation["reported_platform_user_id"],
        "used_as_identity_key": False,
    }


def _policy_receipt() -> dict[str, Any]:
    return {
        "policy_version": semantic.PROXY_POLICY_VERSION,
        "policy_sha256": semantic.CANONICAL_PROXY_POLICY_SHA256,
        "validation_status": "validated_canonical",
    }


def _failed_item_review(
    *,
    batch_request: Mapping[str, Any],
    observation: Mapping[str, Any],
    model_output: Any,
    error_code: str,
) -> dict[str, Any]:
    return {
        "schema_version": ITEM_REVIEW_SCHEMA_VERSION,
        "status": "failed",
        "batch_id": batch_request["batch_id"],
        "item_request_id": item_request_id_for(batch_id=batch_request["batch_id"], observation=observation),
        "observation_id": observation["observation_id"],
        "observation_sha256": canonical_sha256(observation),
        "text_sha256": observation["text_sha256"],
        "source_trust": SOURCE_TRUST,
        "reported_platform_user_id_diagnostic": _reported_id_diagnostic(observation),
        # Invalid or over-ceiling model output is deliberately not hashed a
        # second time. The failure remains source-bound by item/text IDs.
        "model_output_sha256": None,
        "verdict": "unavailable",
        "proposals": [],
        "professional_experience_proxy_policy": _policy_receipt(),
        "professional_experience_proxy_rollup": _empty_rollup(),
        "error_codes": [error_code],
        "authority": copy.deepcopy(REVIEW_AUTHORITY),
    }


def _completed_item_review(
    *,
    batch_request: Mapping[str, Any],
    observation: Mapping[str, Any],
    model_output: Mapping[str, Any],
    proxy_policy: Mapping[str, Any],
) -> dict[str, Any]:
    proposals = _review_proposals(observation=observation, model_proposals=model_output["proposals"])
    return {
        "schema_version": ITEM_REVIEW_SCHEMA_VERSION,
        "status": "completed",
        "batch_id": batch_request["batch_id"],
        "item_request_id": model_output["item_request_id"],
        "observation_id": observation["observation_id"],
        "observation_sha256": canonical_sha256(observation),
        "text_sha256": observation["text_sha256"],
        "source_trust": SOURCE_TRUST,
        "reported_platform_user_id_diagnostic": _reported_id_diagnostic(observation),
        "model_output_sha256": canonical_sha256(model_output),
        "verdict": model_output["verdict"],
        "proposals": proposals,
        "professional_experience_proxy_policy": _policy_receipt(),
        "professional_experience_proxy_rollup": _proxy_rollup(proposals, proxy_policy=proxy_policy),
        "error_codes": [],
        "authority": copy.deepcopy(REVIEW_AUTHORITY),
    }


def _resolve_proxy_policy(proxy_policy: Any = None) -> dict[str, Any]:
    value = proxy_policy
    if value is None:
        value = semantic.load_json(semantic.PROXY_POLICY_PATH)
    if semantic.validate_proxy_policy(value):
        raise ValueError("proxy_policy_invalid")
    return copy.deepcopy(value)


def adjudicate_item(
    *,
    batch_request: Mapping[str, Any],
    observation_id: str,
    model_output: Any,
    proxy_policy: Any = None,
) -> dict[str, Any]:
    if validate_batch_request(batch_request):
        raise ValueError("batch_request_invalid")
    policy = _resolve_proxy_policy(proxy_policy)
    observation_index = _build_observation_index_validated(batch_request)
    observation = observation_index.get(observation_id)
    if observation is None:
        raise ValueError("observation_not_in_batch")
    return _adjudicate_item_validated(
        batch_request=batch_request,
        observation=observation,
        model_output=model_output,
        proxy_policy=policy,
        observation_index=observation_index,
    )


def _adjudicate_item_validated(
    *,
    batch_request: Mapping[str, Any],
    observation: Mapping[str, Any],
    model_output: Any,
    proxy_policy: Mapping[str, Any],
    observation_index: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    if model_output is None:
        return _failed_item_review(
            batch_request=batch_request,
            observation=observation,
            model_output=None,
            error_code="model_output_missing",
        )
    if _validate_model_output_validated(
        model_output,
        batch_request=batch_request,
        observation=observation,
        observation_index=observation_index,
    ):
        return _failed_item_review(
            batch_request=batch_request,
            observation=observation,
            model_output=model_output,
            error_code="model_output_invalid",
        )
    return _completed_item_review(
        batch_request=batch_request,
        observation=observation,
        model_output=model_output,
        proxy_policy=proxy_policy,
    )


def validate_item_review(
    review: Any,
    *,
    batch_request: Mapping[str, Any],
    model_output: Any,
    proxy_policy: Any = None,
) -> list[str]:
    traversal = _scan_json(review, max_depth=MAX_VALIDATION_DEPTH, max_nodes=MAX_VALIDATION_NODES)
    if traversal:
        return [f"$.validation: {error}" for error in traversal]
    errors: list[str] = []
    if not _exact_keys(review, _ITEM_REVIEW_KEYS, path="$", errors=errors):
        return errors
    if review["schema_version"] != ITEM_REVIEW_SCHEMA_VERSION:
        errors.append("$.schema_version: unsupported")
    if not isinstance(review["status"], str) or review["status"] not in {"completed", "failed"}:
        errors.append("$.status: unsupported")
    if not isinstance(review["batch_id"], str) or _BATCH_ID_RE.fullmatch(review["batch_id"]) is None:
        errors.append("$.batch_id: invalid")
    if not isinstance(review["item_request_id"], str) or _ITEM_REQUEST_ID_RE.fullmatch(
        review["item_request_id"]
    ) is None:
        errors.append("$.item_request_id: invalid")
    if not isinstance(review["observation_id"], str) or _OBSERVATION_ID_RE.fullmatch(review["observation_id"]) is None:
        errors.append("$.observation_id: invalid")
    for field in ("observation_sha256", "text_sha256"):
        if not _valid_sha(review[field]):
            errors.append(f"$.{field}: invalid")
    output_sha = review["model_output_sha256"]
    if output_sha is not None and not _valid_sha(output_sha):
        errors.append("$.model_output_sha256: invalid")
    if review["source_trust"] != SOURCE_TRUST:
        errors.append("$.source_trust: cannot be upgraded by semantic review")
    proposals = review["proposals"]
    if not isinstance(proposals, list) or len(proposals) > MAX_PROPOSALS_PER_ITEM:
        errors.append("$.proposals: invalid")
        proposals = []
    for index, proposal in enumerate(proposals):
        path = f"$.proposals[{index}]"
        if not _exact_keys(proposal, _REVIEW_PROPOSAL_KEYS, path=path, errors=errors):
            continue
        if not isinstance(proposal["proposal_id"], str) or _PROPOSAL_ID_RE.fullmatch(proposal["proposal_id"]) is None:
            errors.append(f"{path}.proposal_id: invalid")
        if not _valid_sha(proposal["excerpt_sha256"]):
            errors.append(f"{path}.excerpt_sha256: invalid")
        if proposal["verification_status"] != VERIFICATION_STATUS:
            errors.append(f"{path}.verification_status: must remain unverified")
    if not _json_equal(review["professional_experience_proxy_policy"], _policy_receipt()):
        errors.append("$.professional_experience_proxy_policy: invalid")
    if not _json_equal(review["authority"], REVIEW_AUTHORITY):
        errors.append("$.authority: downstream authority must remain closed")
    if validate_batch_request(batch_request):
        errors.append("$.batch_request: invalid")
        return errors
    observation_index = _build_observation_index_validated(batch_request)
    observation = (
        observation_index.get(review["observation_id"])
        if isinstance(review.get("observation_id"), str)
        else None
    )
    if observation is None:
        errors.append("$.observation_id: not in batch")
        return errors
    try:
        policy = _resolve_proxy_policy(proxy_policy)
        expected = _adjudicate_item_validated(
            batch_request=batch_request,
            observation=observation,
            model_output=model_output,
            proxy_policy=policy,
            observation_index=observation_index,
        )
    except (TypeError, ValueError, KeyError, RecursionError):
        errors.append("$: deterministic recomputation failed")
        return errors
    if not _json_equal(review, expected):
        errors.append("$: review does not equal deterministic recomputation")
    return errors


def adjudicate_batch(
    *,
    batch_request: Mapping[str, Any],
    model_outputs_by_observation_id: Mapping[str, Any],
    proxy_policy: Any = None,
) -> dict[str, Any]:
    request_errors = validate_batch_request(batch_request)
    if request_errors:
        raise ValueError(f"batch_request_invalid:{request_errors[0]}")
    if not isinstance(model_outputs_by_observation_id, Mapping) or any(
        not isinstance(key, str) for key in model_outputs_by_observation_id
    ):
        raise ValueError("model_outputs_invalid")
    policy = _resolve_proxy_policy(proxy_policy)
    observation_index = _build_observation_index_validated(batch_request)
    expected_ids = set(observation_index)
    extra_ids = set(model_outputs_by_observation_id) - expected_ids
    if extra_ids:
        raise ValueError("model_outputs_contain_unknown_observation")
    reviews = [
        _adjudicate_item_validated(
            batch_request=batch_request,
            observation=observation,
            model_output=model_outputs_by_observation_id.get(observation["observation_id"]),
            proxy_policy=policy,
            observation_index=observation_index,
        )
        for observation in batch_request["observations"]
    ]
    completed = sum(review["status"] == "completed" for review in reviews)
    failed = len(reviews) - completed
    result = {
        "schema_version": BATCH_CLOSURE_VERSION,
        "batch_id": batch_request["batch_id"],
        "batch_request_sha256": canonical_sha256(batch_request),
        "denominator": len(batch_request["observations"]),
        "terminal_reviews": len(reviews),
        "completed": completed,
        "failed": failed,
        "reviews": reviews,
        "authority": copy.deepcopy(REVIEW_AUTHORITY),
    }
    closure_errors = _validate_batch_closure_structure(
        result,
        batch_request=batch_request,
    )
    if closure_errors:
        raise RuntimeError(f"batch_closure_invalid:{closure_errors[0]}")
    return result


def _validate_batch_closure_structure(
    result: Any,
    *,
    batch_request: Mapping[str, Any],
) -> list[str]:
    expected_keys = {
        "schema_version",
        "batch_id",
        "batch_request_sha256",
        "denominator",
        "terminal_reviews",
        "completed",
        "failed",
        "reviews",
        "authority",
    }
    errors: list[str] = []
    traversal = _scan_json(result, max_depth=MAX_VALIDATION_DEPTH, max_nodes=MAX_VALIDATION_NODES)
    if traversal:
        return [f"$.validation: {error}" for error in traversal]
    if not _exact_keys(result, expected_keys, path="$", errors=errors):
        return errors
    if result["schema_version"] != BATCH_CLOSURE_VERSION:
        errors.append("$.schema_version: unsupported")
    reviews = result["reviews"]
    if not isinstance(reviews, list):
        errors.append("$.reviews: must be an array")
        reviews = []
    denominator = len(batch_request.get("observations", [])) if isinstance(batch_request, Mapping) else -1
    if result["denominator"] != denominator:
        errors.append("$.denominator: must equal every input observation")
    if result["terminal_reviews"] != len(reviews) or len(reviews) != denominator:
        errors.append("$.terminal_reviews: every denominator row must terminate")
    completed = sum(isinstance(review, dict) and review.get("status") == "completed" for review in reviews)
    failed = sum(isinstance(review, dict) and review.get("status") == "failed" for review in reviews)
    if result["completed"] != completed or result["failed"] != failed or completed + failed != denominator:
        errors.append("$.counts: completed plus failed must equal denominator")
    if len({review.get("observation_id") for review in reviews if isinstance(review, dict)}) != len(reviews):
        errors.append("$.reviews: duplicate observation terminal")
    if not _json_equal(result["authority"], REVIEW_AUTHORITY):
        errors.append("$.authority: invalid")
    return errors


def validate_batch_closure(
    result: Any,
    *,
    batch_request: Mapping[str, Any],
    model_outputs_by_observation_id: Mapping[str, Any],
    proxy_policy: Any = None,
) -> list[str]:
    """Validate structure and always recompute the complete public closure."""

    errors = _validate_batch_closure_structure(result, batch_request=batch_request)
    if errors:
        return errors
    try:
        expected = adjudicate_batch(
            batch_request=batch_request,
            model_outputs_by_observation_id=model_outputs_by_observation_id,
            proxy_policy=proxy_policy,
        )
    except (TypeError, ValueError, RuntimeError, KeyError, RecursionError):
        return ["$: deterministic batch recomputation failed"]
    if not _json_equal(result, expected):
        return ["$: batch closure does not equal deterministic recomputation"]
    return []


__all__ = [
    "BATCH_AUTHORITY",
    "BATCH_CLOSURE_VERSION",
    "BATCH_REQUEST_SCHEMA_VERSION",
    "EVIDENCE_BASIS",
    "ITEM_REVIEW_SCHEMA_VERSION",
    "MODEL_AUTHORITY",
    "MODEL_OUTPUT_SCHEMA_VERSION",
    "OBSERVATION_AUTHORITY",
    "OBSERVATION_SCHEMA_VERSION",
    "OPERATIONAL_LIMITS",
    "REVIEW_AUTHORITY",
    "SEMANTIC_POLICY",
    "SOURCE_KIND",
    "SOURCE_TRUST",
    "VERIFICATION_STATUS",
    "adjudicate_batch",
    "adjudicate_item",
    "build_batch_request",
    "build_model_output",
    "build_observation",
    "candidate_ref_for",
    "canonical_json",
    "canonical_sha256",
    "item_request_id_for",
    "load_json",
    "observation_id_for",
    "text_sha256",
    "validate_batch_closure",
    "validate_batch_request",
    "validate_item_review",
    "validate_model_output",
    "validate_observation",
]
