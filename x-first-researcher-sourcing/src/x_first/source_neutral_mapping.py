"""Provider-free controller for calibrated, source-neutral X mapping.

This module plans candidate-authored search work and evaluates already-retained
Grok raw-session artifacts.  It never launches Grok, X, or a semantic model.
Execution proof is accepted only by replaying the existing six-file Grok
session contract; caller-supplied normalized ledgers are diagnostic-only.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from x_first.grok_operator_session_replay import (
    FrozenRawSessionArtifact,
    GrokOperatorSessionPrecommit,
    GrokOperatorSessionReplayError,
    replay_grok_operator_session,
    session_precommit_sha256,
    thaw_raw_session_artifacts,
)
from x_first.recall_pool_campaign import RAW_SESSION_FILES
from x_first.recall_pool_schema import assert_schema_valid

POLICY_SCHEMA_VERSION = "x.source_neutral.mapping.policy.v1"
MANIFEST_SCHEMA_VERSION = "x.source_neutral.mapping.candidate_manifest.v1"
PLAN_SCHEMA_VERSION = "x.source_neutral.mapping.wave_plan.v1"
PRECOMMIT_SCHEMA_VERSION = "x.source_neutral.mapping.session_precommit.v1"
TERMINAL_SCHEMA_VERSION = "x.source_neutral.mapping.flat_terminal.v1"
RECEIPT_SCHEMA_VERSION = "x.source_neutral.mapping.session_receipt.v1"
AGGREGATE_SCHEMA_VERSION = "x.source_neutral.mapping.candidate_free_aggregate.v1"
CALIBRATION_SCHEMA_VERSION = "x.source_neutral.mapping.calibration_aggregate.v1"
EXACT_HYDRATION_SCHEMA_VERSION = "x.source_neutral.mapping.exact_post_hydration.v1"
LUNA_REVIEW_SCHEMA_VERSION = "x.source_neutral.mapping.luna_state_review.v1"
LUNA_AXIS_REDUCTION_SCHEMA_VERSION = "x.source_neutral.mapping.luna_axis_reduction.v1"

POLICY_SCHEMA_FILE = "x.source_neutral.mapping.policy.v1.schema.json"
MANIFEST_SCHEMA_FILE = "x.source_neutral.mapping.candidate_manifest.v1.schema.json"
PLAN_SCHEMA_FILE = "x.source_neutral.mapping.wave_plan.v1.schema.json"
RECEIPT_SCHEMA_FILE = "x.source_neutral.mapping.session_receipt.v1.schema.json"
AGGREGATE_SCHEMA_FILE = "x.source_neutral.mapping.candidate_free_aggregate.v1.schema.json"
CALIBRATION_SCHEMA_FILE = "x.source_neutral.mapping.calibration_aggregate.v1.schema.json"
EXACT_HYDRATION_SCHEMA_FILE = "x.source_neutral.mapping.exact_post_hydration.v1.schema.json"
LUNA_REVIEW_SCHEMA_FILE = "x.source_neutral.mapping.luna_state_review.v1.schema.json"
LUNA_AXIS_REDUCTION_SCHEMA_FILE = "x.source_neutral.mapping.luna_axis_reduction.v1.schema.json"

CONTRACT_SCHEMA_FILES = (
    POLICY_SCHEMA_FILE,
    MANIFEST_SCHEMA_FILE,
    PLAN_SCHEMA_FILE,
    RECEIPT_SCHEMA_FILE,
    EXACT_HYDRATION_SCHEMA_FILE,
    LUNA_REVIEW_SCHEMA_FILE,
    LUNA_AXIS_REDUCTION_SCHEMA_FILE,
    AGGREGATE_SCHEMA_FILE,
    CALIBRATION_SCHEMA_FILE,
)

_SHA_RE = re.compile(r"[0-9a-f]{64}")
_ID_RE = re.compile(r"[a-z0-9][a-z0-9_.-]{0,127}")
_HANDLE_RE = re.compile(r"[A-Za-z0-9_]{1,15}")
_STATUS_ID_RE = re.compile(r"[0-9]{1,32}")
_QUERY_TERM_RE = re.compile(r'(?:[A-Za-z0-9_-]+|"[A-Za-z0-9 -]+")')
_AXIS_STATES = frozenset({"current", "historical", "ambiguous", "unsupported"})
_PRIOR_EVIDENCE = frozenset({"fixture_asserted", "source_bound", "model_mediated_unverified", "unsupported"})
QUEUE_KEYS = (
    "pending_wave_p_queue",
    "retry_split_queue",
    "saturation_queue",
    "thread_hydration_queue",
    "luna_input_queue",
    "challenger_queue",
)


class SourceNeutralMappingError(ValueError):
    """Stable fail-closed error for the provider-free mapping controller."""


@dataclass(frozen=True)
class MappingSessionProjection:
    """Immutable inputs required to replay one planned mapping batch."""

    mapping_precommit_json: bytes
    terminal_text: str
    lab_descriptor_json: bytes
    grok_precommit: GrokOperatorSessionPrecommit
    raw_session_artifacts: tuple[FrozenRawSessionArtifact, ...]
    receipt_sha256: str


@dataclass(frozen=True)
class ExactPostHydrationProjection:
    """One exact x_thread_fetch result with retained replayable source bytes."""

    task_sha256: str
    terminal_json: bytes
    grok_precommit: GrokOperatorSessionPrecommit
    raw_session_artifacts: tuple[FrozenRawSessionArtifact, ...]
    terminal_sha256: str
    source_text: bytes
    projection_sha256: str


@dataclass(frozen=True)
class LunaStateReviewProjection:
    """Unattested diagnostic state proposal bound to one exact hydration.

    This projection deliberately carries no transition authority.  A future
    receipt-first model execution contract may consume the same source-bound
    hydration, but caller-created result JSON can never authorize upgrades or
    structural stopping.
    """

    result_json: bytes
    result_sha256: str


@dataclass(frozen=True)
class ExecutedWaveFacts:
    """Mechanically derived zero-wave eligibility facts.

    The constructor is public Python syntax, so consumers revalidate every
    relation and digest instead of treating dataclass identity as authority.
    """

    campaign_id: str
    campaign_ordinal: int
    wave_id: str
    predecessor_wave_facts: ExecutedWaveFacts | None
    predecessor_wave_facts_sha256: str | None
    prior_frontier_sha256: str
    cumulative_stable_post_ids: tuple[str, ...]
    cumulative_frontier_sha256: str
    manifest_json: bytes
    policy_json: bytes
    plan_json: bytes
    strategy_payload_json: bytes
    session_projections: tuple[MappingSessionProjection, ...]
    hydration_projections: tuple[ExactPostHydrationProjection, ...]
    luna_review_projections: tuple[LunaStateReviewProjection, ...]
    strategy_signature_sha256: str
    planned_work_item_ids: tuple[str, ...]
    completed_work_item_ids: tuple[str, ...]
    rejected_work_item_ids: tuple[str, ...]
    covered_candidate_refs: tuple[str, ...]
    expected_candidate_refs: tuple[str, ...]
    session_projection_sha256s: tuple[str, ...]
    exact_hydration_projection_sha256s: tuple[str, ...]
    luna_result_sha256s: tuple[str, ...]
    new_stable_post_ids: tuple[str, ...]
    luna_axis_reduction_json: bytes
    luna_axis_reduction_sha256: str
    semantic_review_coverage_status: str
    semantic_transition_authority: str
    authorized_semantic_transition_ids: tuple[str, ...]
    remaining_queues_json: bytes
    remaining_queue_state_sha256: str
    wave_facts_sha256: str


def project_root() -> Path:
    return Path(__file__).resolve().parents[2]


def canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, allow_nan=False, separators=(",", ":"), sort_keys=True)


def canonical_sha256(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def _content_sha256(value: Mapping[str, Any], hash_field: str) -> str:
    return canonical_sha256({key: item for key, item in value.items() if key != hash_field})


def _strict_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise SourceNeutralMappingError("duplicate_json_key")
        result[key] = value
    return result


def _reject_constant(value: str) -> None:
    raise SourceNeutralMappingError(f"non_finite_json_number:{value}")


def strict_load_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(
            path.read_text(encoding="utf-8"),
            object_pairs_hook=_strict_object,
            parse_constant=_reject_constant,
        )
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise SourceNeutralMappingError("json_file_invalid") from exc
    if not isinstance(value, dict):
        raise SourceNeutralMappingError("json_root_not_object")
    return value


def strict_load_json_bytes(raw: bytes, *, error: str) -> dict[str, Any]:
    if type(raw) is not bytes or not raw:
        raise SourceNeutralMappingError(error)
    try:
        value = json.loads(
            raw.decode("utf-8"),
            object_pairs_hook=_strict_object,
            parse_constant=_reject_constant,
        )
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise SourceNeutralMappingError(error) from exc
    if type(value) is not dict:
        raise SourceNeutralMappingError(error)
    return value


def _canonical_json_bytes(value: Any) -> bytes:
    return canonical_json(value).encode("utf-8")


def _freeze_raw_session_files(raw_session_files: Mapping[str, bytes]) -> tuple[FrozenRawSessionArtifact, ...]:
    if type(raw_session_files) is not dict or set(raw_session_files) != RAW_SESSION_FILES:
        raise SourceNeutralMappingError("mapping_projection_raw_artifact_set_invalid")
    if any(type(value) is not bytes for value in raw_session_files.values()):
        raise SourceNeutralMappingError("mapping_projection_raw_artifact_bytes_invalid")
    return tuple(
        FrozenRawSessionArtifact(name=name, content=raw_session_files[name]) for name in sorted(RAW_SESSION_FILES)
    )


def _exact_keys(value: Any, keys: set[str], error: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping) or set(value) != keys:
        raise SourceNeutralMappingError(error)
    return value


def _sha(value: Any, error: str) -> str:
    if not isinstance(value, str) or _SHA_RE.fullmatch(value) is None:
        raise SourceNeutralMappingError(error)
    return value


def _identifier(value: Any, error: str) -> str:
    if not isinstance(value, str) or _ID_RE.fullmatch(value) is None:
        raise SourceNeutralMappingError(error)
    return value


def _handle(value: Any, error: str) -> str:
    if not isinstance(value, str) or _HANDLE_RE.fullmatch(value) is None:
        raise SourceNeutralMappingError(error)
    return value


def validate_lab_descriptor(value: Any) -> None:
    descriptor = _exact_keys(
        value,
        {
            "descriptor_id",
            "lab_id",
            "display_name",
            "official_handles",
            "affiliation_aliases",
            "project_aliases",
            "x_url_host",
        },
        "lab_descriptor_shape_invalid",
    )
    _identifier(descriptor["descriptor_id"], "lab_descriptor_id_invalid")
    _identifier(descriptor["lab_id"], "lab_id_invalid")
    if not isinstance(descriptor["display_name"], str) or not descriptor["display_name"].strip():
        raise SourceNeutralMappingError("lab_display_name_invalid")
    official = descriptor["official_handles"]
    if not isinstance(official, list) or not official:
        raise SourceNeutralMappingError("lab_official_handles_invalid")
    handles = [_handle(item, "lab_official_handle_invalid") for item in official]
    if len({item.casefold() for item in handles}) != len(handles):
        raise SourceNeutralMappingError("lab_official_handle_duplicate")
    for field in ("affiliation_aliases", "project_aliases"):
        values = descriptor[field]
        if (
            not isinstance(values, list)
            or not values
            or any(not isinstance(item, str) or not item.strip() or len(item) > 160 for item in values)
        ):
            raise SourceNeutralMappingError(f"lab_{field}_invalid")
        if len({item.casefold() for item in values}) != len(values):
            raise SourceNeutralMappingError(f"lab_{field}_duplicate")
    host = descriptor["x_url_host"]
    if not isinstance(host, str) or re.fullmatch(r"[a-z0-9.-]{1,253}", host) is None:
        raise SourceNeutralMappingError("lab_x_url_host_invalid")


def validate_policy(policy: Any) -> None:
    if not isinstance(policy, Mapping):
        raise SourceNeutralMappingError("mapping_policy_not_object")
    assert_schema_valid(policy, POLICY_SCHEMA_FILE)
    if policy.get("schema_version") != POLICY_SCHEMA_VERSION:
        raise SourceNeutralMappingError("mapping_policy_version_invalid")
    wave = policy["wave_p"]
    cells = wave["champion_cells"]
    if wave["calls_per_candidate"] != len(cells):
        raise SourceNeutralMappingError("mapping_policy_call_count_mismatch")
    group_ids = {row["group_id"] for row in policy["concept_alias_groups"]}
    if len(group_ids) != len(policy["concept_alias_groups"]):
        raise SourceNeutralMappingError("mapping_policy_alias_group_duplicate")
    if any(cell["concept_alias_group_id"] not in group_ids for cell in cells):
        raise SourceNeutralMappingError("mapping_policy_champion_group_unknown")
    if any(
        _QUERY_TERM_RE.fullmatch(term) is None for group in policy["concept_alias_groups"] for term in group["aliases"]
    ):
        raise SourceNeutralMappingError("mapping_policy_query_term_invalid")
    if policy["saturation"]["child_lineage"] != ["topic", "mode", "time"]:
        raise SourceNeutralMappingError("mapping_policy_child_lineage_invalid")
    if tuple(policy["queues"]["queue_order"]) != QUEUE_KEYS:
        raise SourceNeutralMappingError("mapping_policy_queue_registry_invalid")
    expected_denominators = [
        "execution_compliance",
        "stable_post_id_retrieval",
        "exact_hydration",
        "luna_diagnostic_review_coverage",
    ]
    if policy["kpis"]["denominator_registry"] != expected_denominators:
        raise SourceNeutralMappingError("mapping_policy_denominator_registry_invalid")


def load_policy(path: Path | None = None) -> dict[str, Any]:
    resolved = path or project_root() / "configs" / "source_neutral_mapping_policy.v1.json"
    policy = strict_load_json(resolved)
    validate_policy(policy)
    return policy


def _validate_prior(value: Any, *, axis: str) -> None:
    prior = _exact_keys(value, {"state", "evidence_status"}, f"candidate_{axis}_prior_shape_invalid")
    if prior["state"] not in _AXIS_STATES:
        raise SourceNeutralMappingError(f"candidate_{axis}_state_invalid")
    if prior["evidence_status"] not in _PRIOR_EVIDENCE:
        raise SourceNeutralMappingError(f"candidate_{axis}_evidence_status_invalid")


def validate_candidate_manifest(manifest: Any) -> None:
    if not isinstance(manifest, Mapping):
        raise SourceNeutralMappingError("candidate_manifest_not_object")
    assert_schema_valid(manifest, MANIFEST_SCHEMA_FILE)
    if manifest.get("schema_version") != MANIFEST_SCHEMA_VERSION:
        raise SourceNeutralMappingError("candidate_manifest_version_invalid")
    validate_lab_descriptor(manifest["lab_descriptor"])
    if manifest["lab_descriptor_sha256"] != canonical_sha256(manifest["lab_descriptor"]):
        raise SourceNeutralMappingError("candidate_manifest_lab_descriptor_hash_mismatch")
    candidates = manifest["candidates"]
    if manifest["candidate_count"] != len(candidates):
        raise SourceNeutralMappingError("candidate_manifest_count_mismatch")
    refs: set[str] = set()
    handles: set[str] = set()
    platform_ids: set[str] = set()
    for row in candidates:
        candidate = _exact_keys(
            row,
            {
                "candidate_ref",
                "platform_user_id",
                "current_handle",
                "profile_url",
                "lab_affiliation_prior",
                "pretraining_experience_prior",
            },
            "candidate_manifest_row_shape_invalid",
        )
        ref = _identifier(candidate["candidate_ref"], "candidate_ref_invalid")
        handle = _handle(candidate["current_handle"], "candidate_handle_invalid")
        if ref in refs or handle.casefold() in handles:
            raise SourceNeutralMappingError("candidate_manifest_identity_duplicate")
        refs.add(ref)
        handles.add(handle.casefold())
        platform_id = candidate["platform_user_id"]
        if platform_id is not None:
            if not isinstance(platform_id, str) or not platform_id.isdecimal() or len(platform_id) > 32:
                raise SourceNeutralMappingError("candidate_platform_user_id_invalid")
            if platform_id in platform_ids:
                raise SourceNeutralMappingError("candidate_platform_user_id_duplicate")
            platform_ids.add(platform_id)
        parsed = urlsplit(candidate["profile_url"])
        if (
            parsed.scheme != "https"
            or parsed.netloc != manifest["lab_descriptor"]["x_url_host"]
            or parsed.query
            or parsed.fragment
            or parsed.path != f"/{handle}"
        ):
            raise SourceNeutralMappingError("candidate_profile_url_invalid")
        _validate_prior(candidate["lab_affiliation_prior"], axis="lab_affiliation")
        _validate_prior(candidate["pretraining_experience_prior"], axis="pretraining_experience")
    if manifest["candidate_refs_sha256"] != canonical_sha256([row["candidate_ref"] for row in candidates]):
        raise SourceNeutralMappingError("candidate_manifest_ref_hash_mismatch")
    if manifest["manifest_sha256"] != _content_sha256(manifest, "manifest_sha256"):
        raise SourceNeutralMappingError("candidate_manifest_hash_mismatch")


def freeze_candidate_manifest(
    *,
    manifest_id: str,
    lab_descriptor: Mapping[str, Any],
    candidates: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    _identifier(manifest_id, "candidate_manifest_id_invalid")
    validate_lab_descriptor(lab_descriptor)
    if not isinstance(candidates, Sequence) or isinstance(candidates, (str, bytes)) or not candidates:
        raise SourceNeutralMappingError("candidate_manifest_candidates_invalid")
    frozen_candidates = json.loads(canonical_json(list(candidates)))
    payload: dict[str, Any] = {
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "manifest_id": manifest_id,
        "lab_descriptor": json.loads(canonical_json(lab_descriptor)),
        "lab_descriptor_sha256": canonical_sha256(lab_descriptor),
        "candidate_count": len(frozen_candidates),
        "candidate_refs_sha256": canonical_sha256([row["candidate_ref"] for row in frozen_candidates]),
        "candidates": frozen_candidates,
        "manifest_sha256": "",
    }
    payload["manifest_sha256"] = _content_sha256(payload, "manifest_sha256")
    validate_candidate_manifest(payload)
    return payload


def _alias_groups(policy: Mapping[str, Any]) -> dict[str, list[str]]:
    return {row["group_id"]: list(row["aliases"]) for row in policy["concept_alias_groups"]}


def _query_for(handle: str, aliases: Sequence[str]) -> str:
    query_terms = " OR ".join(aliases)
    return f"from:{handle} ({query_terms})"


def plan_wave_p(
    manifest: Mapping[str, Any],
    policy: Mapping[str, Any],
    *,
    plan_id: str,
) -> dict[str, Any]:
    validate_candidate_manifest(manifest)
    validate_policy(policy)
    _identifier(plan_id, "wave_plan_id_invalid")
    payload = _construct_wave_plan(manifest=manifest, policy=policy, plan_id=plan_id)
    validate_wave_plan(payload, manifest=manifest, policy=policy)
    return payload


def _construct_wave_plan(
    *,
    manifest: Mapping[str, Any],
    policy: Mapping[str, Any],
    plan_id: str,
) -> dict[str, Any]:
    """Construct the sole policy-owned Wave P value without recursive validation."""

    aliases = _alias_groups(policy)
    grain = policy["wave_p"]["candidate_grain"]
    candidates = manifest["candidates"]
    batches: list[dict[str, Any]] = []
    global_ordinal = 0
    for batch_index, offset in enumerate(range(0, len(candidates), grain), 1):
        rows = candidates[offset : offset + grain]
        calls: list[dict[str, Any]] = []
        for candidate_ordinal, candidate in enumerate(rows, 1):
            for cell in policy["wave_p"]["champion_cells"]:
                global_ordinal += 1
                group_aliases = aliases[cell["concept_alias_group_id"]]
                calls.append(
                    {
                        "global_call_ordinal": global_ordinal,
                        "batch_call_ordinal": len(calls) + 1,
                        "batch_candidate_ordinal": candidate_ordinal,
                        "candidate_ref": candidate["candidate_ref"],
                        "expected_author_handle": candidate["current_handle"],
                        "champion_cell_id": cell["cell_id"],
                        "concept_alias_group_id": cell["concept_alias_group_id"],
                        "topic_aliases": group_aliases,
                        "tool_name": policy["search"]["native_tool_name"],
                        "arguments": {
                            "query": _query_for(candidate["current_handle"], group_aliases),
                            "limit": str(policy["search"]["request_limit"]),
                            "mode": cell["mode"],
                        },
                    }
                )
        refs = [row["candidate_ref"] for row in rows]
        batch_body = {
            "batch_id": f"{plan_id}.batch-{batch_index:04d}",
            "candidate_refs": refs,
            "candidate_refs_sha256": canonical_sha256(refs),
            "calls": calls,
        }
        batch_body["batch_sha256"] = canonical_sha256(batch_body)
        batches.append(batch_body)
    payload: dict[str, Any] = {
        "schema_version": PLAN_SCHEMA_VERSION,
        "plan_id": plan_id,
        "policy_sha256": canonical_sha256(policy),
        "manifest_sha256": manifest["manifest_sha256"],
        "lab_descriptor_sha256": manifest["lab_descriptor_sha256"],
        "candidate_count": len(candidates),
        "planned_batch_count": len(batches),
        "planned_native_x_call_count": global_ordinal,
        "candidate_refs_sha256": manifest["candidate_refs_sha256"],
        "batches": batches,
        "plan_sha256": "",
    }
    payload["plan_sha256"] = _content_sha256(payload, "plan_sha256")
    return payload


def validate_wave_plan(
    plan: Any,
    *,
    manifest: Mapping[str, Any],
    policy: Mapping[str, Any],
) -> None:
    validate_candidate_manifest(manifest)
    validate_policy(policy)
    if not isinstance(plan, Mapping):
        raise SourceNeutralMappingError("wave_plan_not_object")
    try:
        assert_schema_valid(plan, PLAN_SCHEMA_FILE)
    except Exception as exc:
        raise SourceNeutralMappingError("wave_plan_schema_invalid") from exc
    if plan.get("schema_version") != PLAN_SCHEMA_VERSION:
        raise SourceNeutralMappingError("wave_plan_version_invalid")
    plan_id = _identifier(plan.get("plan_id"), "wave_plan_id_invalid")
    if (
        plan["policy_sha256"] != canonical_sha256(policy)
        or plan["manifest_sha256"] != manifest["manifest_sha256"]
        or plan["lab_descriptor_sha256"] != manifest["lab_descriptor_sha256"]
        or plan["candidate_refs_sha256"] != manifest["candidate_refs_sha256"]
    ):
        raise SourceNeutralMappingError("wave_plan_source_binding_mismatch")
    batches = plan["batches"]
    if plan["planned_batch_count"] != len(batches):
        raise SourceNeutralMappingError("wave_plan_batch_count_mismatch")
    planned_refs: list[str] = []
    call_ordinals: list[int] = []
    for batch in batches:
        body = {key: value for key, value in batch.items() if key != "batch_sha256"}
        if batch["batch_sha256"] != canonical_sha256(body):
            raise SourceNeutralMappingError("wave_plan_batch_hash_mismatch")
        if batch["candidate_refs_sha256"] != canonical_sha256(batch["candidate_refs"]):
            raise SourceNeutralMappingError("wave_plan_batch_ref_hash_mismatch")
        planned_refs.extend(batch["candidate_refs"])
        expected_calls = len(batch["candidate_refs"]) * policy["wave_p"]["calls_per_candidate"]
        if len(batch["calls"]) != expected_calls:
            raise SourceNeutralMappingError("wave_plan_batch_call_count_mismatch")
        call_ordinals.extend(row["global_call_ordinal"] for row in batch["calls"])
    manifest_refs = [row["candidate_ref"] for row in manifest["candidates"]]
    if planned_refs != manifest_refs:
        raise SourceNeutralMappingError("wave_plan_candidate_coverage_invalid")
    if call_ordinals != list(range(1, len(call_ordinals) + 1)):
        raise SourceNeutralMappingError("wave_plan_call_order_invalid")
    if plan["candidate_count"] != len(planned_refs) or plan["planned_native_x_call_count"] != len(call_ordinals):
        raise SourceNeutralMappingError("wave_plan_denominator_mismatch")
    if plan["plan_sha256"] != _content_sha256(plan, "plan_sha256"):
        raise SourceNeutralMappingError("wave_plan_hash_mismatch")
    expected = _construct_wave_plan(manifest=manifest, policy=policy, plan_id=plan_id)
    if canonical_json(plan) != canonical_json(expected):
        raise SourceNeutralMappingError("wave_plan_exact_policy_reconstruction_mismatch")


def build_session_precommit(
    *,
    plan: Mapping[str, Any],
    manifest: Mapping[str, Any],
    policy: Mapping[str, Any],
    batch_index: int,
    grok_precommit: GrokOperatorSessionPrecommit,
) -> dict[str, Any]:
    validate_wave_plan(plan, manifest=manifest, policy=policy)
    if type(batch_index) is not int or batch_index < 0 or batch_index >= len(plan["batches"]):
        raise SourceNeutralMappingError("session_precommit_batch_index_invalid")
    batch = plan["batches"][batch_index]
    marker = f"MAPPING_BATCH_SHA256={batch['batch_sha256']}"
    try:
        prompt_text = grok_precommit.expected_user_prompt.decode("utf-8")
    except (AttributeError, UnicodeError) as exc:
        raise SourceNeutralMappingError("session_precommit_prompt_invalid") from exc
    if marker not in prompt_text.splitlines():
        raise SourceNeutralMappingError("session_precommit_batch_marker_missing")
    payload: dict[str, Any] = {
        "schema_version": PRECOMMIT_SCHEMA_VERSION,
        "session_id": grok_precommit.expected_session_id,
        "request_id": grok_precommit.expected_request_id,
        "plan_sha256": plan["plan_sha256"],
        "batch_id": batch["batch_id"],
        "batch_sha256": batch["batch_sha256"],
        "manifest_sha256": plan["manifest_sha256"],
        "policy_sha256": plan["policy_sha256"],
        "grok_session_precommit_sha256": session_precommit_sha256(grok_precommit),
        "expected_user_prompt_sha256": grok_precommit.expected_user_prompt_sha256,
        "prompt_batch_marker": marker,
        "calls": json.loads(canonical_json(batch["calls"])),
        "precommit_sha256": "",
    }
    payload["precommit_sha256"] = _content_sha256(payload, "precommit_sha256")
    _validate_session_precommit(payload)
    return payload


def _validate_session_precommit(value: Any) -> None:
    precommit = _exact_keys(
        value,
        {
            "schema_version",
            "session_id",
            "request_id",
            "plan_sha256",
            "batch_id",
            "batch_sha256",
            "manifest_sha256",
            "policy_sha256",
            "grok_session_precommit_sha256",
            "expected_user_prompt_sha256",
            "prompt_batch_marker",
            "calls",
            "precommit_sha256",
        },
        "session_precommit_shape_invalid",
    )
    if precommit["schema_version"] != PRECOMMIT_SCHEMA_VERSION:
        raise SourceNeutralMappingError("session_precommit_version_invalid")
    for field in (
        "plan_sha256",
        "batch_sha256",
        "manifest_sha256",
        "policy_sha256",
        "grok_session_precommit_sha256",
        "expected_user_prompt_sha256",
        "precommit_sha256",
    ):
        _sha(precommit[field], f"session_precommit_{field}_invalid")
    if precommit["precommit_sha256"] != _content_sha256(precommit, "precommit_sha256"):
        raise SourceNeutralMappingError("session_precommit_hash_mismatch")
    if precommit["prompt_batch_marker"] != f"MAPPING_BATCH_SHA256={precommit['batch_sha256']}":
        raise SourceNeutralMappingError("session_precommit_batch_marker_invalid")
    calls = precommit["calls"]
    if not isinstance(calls, list) or not calls:
        raise SourceNeutralMappingError("session_precommit_calls_invalid")
    if [row.get("batch_call_ordinal") for row in calls] != list(range(1, len(calls) + 1)):
        raise SourceNeutralMappingError("session_precommit_call_order_invalid")


def render_flat_terminal(blocks: Sequence[Sequence[str]]) -> str:
    lines: list[str] = []
    for ordinal, urls in enumerate(blocks, 1):
        lines.append(f"BEGIN_CALL_{ordinal:04d}_URLS")
        lines.extend(urls)
        lines.append(f"END_CALL_{ordinal:04d}_URLS")
    return "\n".join(lines)


def _parse_flat_terminal(
    terminal_text: Any,
    *,
    calls: Sequence[Mapping[str, Any]],
    x_url_host: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    if not isinstance(terminal_text, str) or not terminal_text or terminal_text.endswith("\n"):
        raise SourceNeutralMappingError("flat_terminal_text_invalid")
    lines = terminal_text.splitlines()
    cursor = 0
    references: list[dict[str, Any]] = []
    saturation: list[dict[str, Any]] = []
    status_owner: dict[str, str] = {}
    seen_candidate_status: set[tuple[str, str]] = set()
    for ordinal, call in enumerate(calls, 1):
        begin = f"BEGIN_CALL_{ordinal:04d}_URLS"
        end = f"END_CALL_{ordinal:04d}_URLS"
        if cursor >= len(lines) or lines[cursor] != begin:
            raise SourceNeutralMappingError("flat_terminal_begin_marker_invalid")
        cursor += 1
        block_urls: list[str] = []
        while cursor < len(lines) and lines[cursor] != end:
            block_urls.append(lines[cursor])
            cursor += 1
        if cursor >= len(lines) or lines[cursor] != end:
            raise SourceNeutralMappingError("flat_terminal_end_marker_invalid")
        cursor += 1
        if len(block_urls) > int(call["arguments"]["limit"]):
            raise SourceNeutralMappingError("flat_terminal_request_limit_exceeded")
        if len(block_urls) != len(set(block_urls)):
            raise SourceNeutralMappingError("flat_terminal_call_url_duplicate")
        for url in block_urls:
            parsed = urlsplit(url)
            parts = parsed.path.split("/")
            if (
                parsed.scheme != "https"
                or parsed.netloc != x_url_host
                or parsed.query
                or parsed.fragment
                or len(parts) != 4
                or parts[0] != ""
                or parts[2] != "status"
                or parts[1].casefold() != call["expected_author_handle"].casefold()
                or _STATUS_ID_RE.fullmatch(parts[3]) is None
            ):
                raise SourceNeutralMappingError("flat_terminal_url_binding_invalid")
            stable_id = parts[3]
            candidate_ref = call["candidate_ref"]
            prior_owner = status_owner.setdefault(stable_id, candidate_ref)
            if prior_owner != candidate_ref:
                raise SourceNeutralMappingError("flat_terminal_cross_candidate_collision")
            identity = (candidate_ref, stable_id)
            if identity not in seen_candidate_status:
                seen_candidate_status.add(identity)
                references.append(
                    {
                        "candidate_ref": candidate_ref,
                        "stable_post_id": stable_id,
                        "url": url,
                        "author_handle": call["expected_author_handle"],
                        "source_status": "model_mediated_url_from_verified_native_x_session",
                        "observed_call_ordinals": [ordinal],
                    }
                )
            else:
                for row in references:
                    if row["candidate_ref"] == candidate_ref and row["stable_post_id"] == stable_id:
                        row["observed_call_ordinals"].append(ordinal)
                        break
        if len(block_urls) == int(call["arguments"]["limit"]):
            saturation.append(
                {
                    "candidate_ref": call["candidate_ref"],
                    "parent_call_ordinal": ordinal,
                    "concept_alias_group_id": call["concept_alias_group_id"],
                    "topic_aliases": list(call["topic_aliases"]),
                    "mode": call["arguments"]["mode"],
                    "time_window": None,
                    "next_child_rule": "topic",
                    "reason": "request_limit_equal_is_lower_bound_only",
                }
            )
    if cursor != len(lines):
        raise SourceNeutralMappingError("flat_terminal_trailing_content")
    return references, saturation


def replay_flat_session(
    *,
    mapping_precommit: Mapping[str, Any],
    terminal_text: str,
    lab_descriptor: Mapping[str, Any],
    raw_session_files: Mapping[str, bytes] | None,
    grok_precommit: GrokOperatorSessionPrecommit | None,
) -> dict[str, Any]:
    """Replay one flat batch; only six-file raw replay can authorize commit."""

    precommit_trusted = False
    try:
        _validate_session_precommit(mapping_precommit)
        precommit_trusted = True
    except (SourceNeutralMappingError, KeyError, TypeError):
        return _rejected_session_receipt(
            mapping_precommit,
            "mapping_precommit_invalid",
            planned_native_x_call_count=None,
        )
    try:
        validate_lab_descriptor(lab_descriptor)
        references, saturation = _parse_flat_terminal(
            terminal_text,
            calls=mapping_precommit["calls"],
            x_url_host=lab_descriptor["x_url_host"],
        )
    except (SourceNeutralMappingError, KeyError, TypeError):
        return _rejected_session_receipt(
            mapping_precommit,
            "flat_projection_invalid",
            planned_native_x_call_count=len(mapping_precommit["calls"]) if precommit_trusted else None,
        )
    if raw_session_files is None or grok_precommit is None:
        return _rejected_session_receipt(
            mapping_precommit,
            "operator_projected_unverified",
            planned_native_x_call_count=len(mapping_precommit["calls"]),
        )
    try:
        if (
            session_precommit_sha256(grok_precommit) != mapping_precommit["grok_session_precommit_sha256"]
            or grok_precommit.expected_user_prompt_sha256 != mapping_precommit["expected_user_prompt_sha256"]
            or mapping_precommit["prompt_batch_marker"]
            not in grok_precommit.expected_user_prompt.decode("utf-8").splitlines()
        ):
            raise SourceNeutralMappingError("grok_precommit_binding_mismatch")
        replay = replay_grok_operator_session(
            raw_session_files,
            session_precommit=grok_precommit,
            allowed_tool_names=frozenset({"x_keyword_search"}),
            expected_terminal_text=terminal_text,
        )
        if replay.session_id != mapping_precommit["session_id"] or replay.request_id != mapping_precommit["request_id"]:
            raise SourceNeutralMappingError("raw_session_identity_mismatch")
        completions = replay.tool_completions
        calls = mapping_precommit["calls"]
        if len(completions) != len(calls):
            raise SourceNeutralMappingError("raw_session_call_count_mismatch")
        for completion, call in zip(completions, calls, strict=True):
            arguments = json.loads(completion.arguments_json, object_pairs_hook=_strict_object)
            if completion.tool_name != call["tool_name"] or arguments != call["arguments"]:
                raise SourceNeutralMappingError("raw_session_call_precommit_mismatch")
    except (
        GrokOperatorSessionReplayError,
        SourceNeutralMappingError,
        UnicodeError,
        json.JSONDecodeError,
    ):
        return _rejected_session_receipt(
            mapping_precommit,
            "raw_six_file_replay_invalid",
            planned_native_x_call_count=len(mapping_precommit["calls"]),
        )
    receipt: dict[str, Any] = {
        "schema_version": RECEIPT_SCHEMA_VERSION,
        "status": "accepted",
        "commit_allowed": True,
        "proof_authority": "existing_raw_grok_six_file_replay",
        "errors": [],
        "precommit_sha256": mapping_precommit["precommit_sha256"],
        "source_artifact_manifest_sha256": replay.transcript_sha256,
        "source_artifact_count": len(replay.source_artifact_sha256s),
        "source_artifact_hashes_sha256": canonical_sha256(list(replay.source_artifact_sha256s)),
        "terminal_sha256": canonical_sha256(terminal_text),
        "planned_native_x_call_count": len(mapping_precommit["calls"]),
        "attested_completed_native_x_call_count": len(replay.tool_completions),
        "x_user_search_call_count": 0,
        "pretool_terminal_count": 0,
        "unique_stable_post_id_count": len(references),
        "saturation_lower_bound_count": len(saturation),
        "references": references,
        "saturation_queue_seeds": saturation,
        "receipt_sha256": "",
    }
    receipt["receipt_sha256"] = _content_sha256(receipt, "receipt_sha256")
    assert_schema_valid(receipt, RECEIPT_SCHEMA_FILE)
    return receipt


def _rejected_session_receipt(
    precommit: Mapping[str, Any],
    error: str,
    *,
    planned_native_x_call_count: int | None,
) -> dict[str, Any]:
    digest = precommit.get("precommit_sha256") if isinstance(precommit, Mapping) else None
    if not isinstance(digest, str) or _SHA_RE.fullmatch(digest) is None:
        digest = "0" * 64
    receipt: dict[str, Any] = {
        "schema_version": RECEIPT_SCHEMA_VERSION,
        "status": "rejected",
        "commit_allowed": False,
        "proof_authority": "operator_projected_unverified",
        "errors": [error],
        "precommit_sha256": digest,
        "source_artifact_manifest_sha256": None,
        "source_artifact_count": 0,
        "source_artifact_hashes_sha256": None,
        "terminal_sha256": None,
        "planned_native_x_call_count": planned_native_x_call_count,
        "attested_completed_native_x_call_count": 0,
        "x_user_search_call_count": 0,
        "pretool_terminal_count": 0,
        "unique_stable_post_id_count": 0,
        "saturation_lower_bound_count": 0,
        "references": [],
        "saturation_queue_seeds": [],
        "receipt_sha256": "",
    }
    receipt["receipt_sha256"] = _content_sha256(receipt, "receipt_sha256")
    assert_schema_valid(receipt, RECEIPT_SCHEMA_FILE)
    return receipt


def build_mapping_session_projection(
    *,
    mapping_precommit: Mapping[str, Any],
    terminal_text: str,
    lab_descriptor: Mapping[str, Any],
    raw_session_files: Mapping[str, bytes],
    grok_precommit: GrokOperatorSessionPrecommit,
) -> MappingSessionProjection:
    """Freeze all source material needed to rederive one session receipt."""

    receipt = replay_flat_session(
        mapping_precommit=mapping_precommit,
        terminal_text=terminal_text,
        lab_descriptor=lab_descriptor,
        raw_session_files=raw_session_files,
        grok_precommit=grok_precommit,
    )
    projection = MappingSessionProjection(
        mapping_precommit_json=_canonical_json_bytes(mapping_precommit),
        terminal_text=terminal_text,
        lab_descriptor_json=_canonical_json_bytes(lab_descriptor),
        grok_precommit=grok_precommit,
        raw_session_artifacts=_freeze_raw_session_files(raw_session_files),
        receipt_sha256=receipt["receipt_sha256"],
    )
    _replay_mapping_session_projection(projection)
    return projection


def _replay_mapping_session_projection(
    projection: Any,
) -> tuple[dict[str, Any], dict[str, Any]]:
    if type(projection) is not MappingSessionProjection:
        raise SourceNeutralMappingError("mapping_session_projection_required")
    mapping_precommit = strict_load_json_bytes(
        projection.mapping_precommit_json,
        error="mapping_session_projection_precommit_invalid",
    )
    lab_descriptor = strict_load_json_bytes(
        projection.lab_descriptor_json,
        error="mapping_session_projection_descriptor_invalid",
    )
    try:
        raw_session_files = thaw_raw_session_artifacts(projection.raw_session_artifacts)
    except GrokOperatorSessionReplayError as exc:
        raise SourceNeutralMappingError("mapping_session_projection_raw_invalid") from exc
    receipt = replay_flat_session(
        mapping_precommit=mapping_precommit,
        terminal_text=projection.terminal_text,
        lab_descriptor=lab_descriptor,
        raw_session_files=raw_session_files,
        grok_precommit=projection.grok_precommit,
    )
    if receipt["receipt_sha256"] != projection.receipt_sha256:
        raise SourceNeutralMappingError("mapping_session_projection_receipt_mismatch")
    return mapping_precommit, receipt


def split_invalid_batch(candidate_refs: Sequence[str]) -> list[list[str]]:
    refs = list(candidate_refs)
    if not refs or len(set(refs)) != len(refs):
        raise SourceNeutralMappingError("invalid_batch_candidate_refs_invalid")
    if len(refs) == 1:
        return []
    split_at = 2 if len(refs) == 3 else (len(refs) + 1) // 2
    return [refs[:split_at], refs[split_at:]]


def expand_saturation_item(item: Mapping[str, Any], policy: Mapping[str, Any]) -> list[dict[str, Any]]:
    validate_policy(policy)
    rule = item.get("next_child_rule")
    aliases = item.get("topic_aliases")
    if not isinstance(aliases, list) or not aliases:
        raise SourceNeutralMappingError("saturation_topic_aliases_invalid")
    base = {
        "candidate_ref": item["candidate_ref"],
        "concept_alias_group_id": item["concept_alias_group_id"],
        "reason": "parent_was_saturation_lower_bound",
    }
    if rule == "topic":
        midpoint = (len(aliases) + 1) // 2
        partitions = [aliases[:midpoint], aliases[midpoint:]]
        return [
            {**base, "topic_aliases": part, "mode": item["mode"], "time_window": None, "next_child_rule": "mode"}
            for part in partitions
            if part
        ]
    if rule == "mode":
        modes = [mode for mode in policy["saturation"]["mode_children"] if mode != item["mode"]]
        return [
            {**base, "topic_aliases": aliases, "mode": mode, "time_window": None, "next_child_rule": "time"}
            for mode in modes[:1]
        ]
    if rule == "time":
        return [
            {
                **base,
                "topic_aliases": aliases,
                "mode": item["mode"],
                "time_window": window,
                "next_child_rule": "closed",
            }
            for window in policy["saturation"]["time_children"]
        ]
    if rule == "closed":
        return []
    raise SourceNeutralMappingError("saturation_child_rule_invalid")


def build_frontier_queues(
    *,
    manifest: Mapping[str, Any],
    plan: Mapping[str, Any],
    session_projections: Sequence[MappingSessionProjection],
    policy: Mapping[str, Any],
) -> dict[str, list[dict[str, Any]]]:
    validate_candidate_manifest(manifest)
    validate_policy(policy)
    validate_wave_plan(plan, manifest=manifest, policy=policy)
    if type(session_projections) not in {list, tuple}:
        raise SourceNeutralMappingError("frontier_session_projections_invalid")
    references: dict[tuple[str, str], Mapping[str, Any]] = {}
    manifest_refs = {row["candidate_ref"] for row in manifest["candidates"]}
    stable_post_owners: dict[str, str] = {}
    saturation: list[dict[str, Any]] = []
    batches_by_id = {batch["batch_id"]: (index, batch) for index, batch in enumerate(plan["batches"])}
    projected_by_batch: dict[str, tuple[dict[str, Any], dict[str, Any]]] = {}
    session_ids: set[str] = set()
    request_ids: set[str] = set()
    receipt_sha256s: set[str] = set()
    for projection in session_projections:
        mapping_precommit, receipt = _replay_mapping_session_projection(projection)
        if (
            strict_load_json_bytes(
                projection.lab_descriptor_json,
                error="frontier_projection_descriptor_invalid",
            )
            != manifest["lab_descriptor"]
        ):
            raise SourceNeutralMappingError("frontier_projection_descriptor_mismatch")
        batch_id = mapping_precommit.get("batch_id")
        if (
            batch_id not in batches_by_id
            or batch_id in projected_by_batch
            or mapping_precommit["session_id"] in session_ids
            or mapping_precommit["request_id"] in request_ids
            or receipt["receipt_sha256"] in receipt_sha256s
        ):
            raise SourceNeutralMappingError("frontier_projection_batch_coverage_invalid")
        session_ids.add(mapping_precommit["session_id"])
        request_ids.add(mapping_precommit["request_id"])
        receipt_sha256s.add(receipt["receipt_sha256"])
        batch_index, _ = batches_by_id[batch_id]
        expected_precommit = build_session_precommit(
            plan=plan,
            manifest=manifest,
            policy=policy,
            batch_index=batch_index,
            grok_precommit=projection.grok_precommit,
        )
        if canonical_json(mapping_precommit) != canonical_json(expected_precommit):
            raise SourceNeutralMappingError("frontier_projection_plan_binding_invalid")
        projected_by_batch[batch_id] = (mapping_precommit, receipt)

    pending: list[dict[str, Any]] = []
    retry_split: list[dict[str, Any]] = []
    fully_attested_candidate_refs: set[str] = set()
    accepted_receipts: list[dict[str, Any]] = []
    for batch in plan["batches"]:
        projected = projected_by_batch.get(batch["batch_id"])
        if projected is None:
            pending.append(
                {
                    "batch_id": batch["batch_id"],
                    "candidate_refs": list(batch["candidate_refs"]),
                    "planned_native_x_call_count": len(batch["calls"]),
                    "reason": "unexecuted_wave_p_batch",
                }
            )
            continue
        _, receipt = projected
        if receipt["status"] != "accepted":
            children = split_invalid_batch(batch["candidate_refs"])
            retry_split.append(
                {
                    "batch_id": batch["batch_id"],
                    "candidate_refs": list(batch["candidate_refs"]),
                    "planned_native_x_call_count": receipt["planned_native_x_call_count"],
                    "split_candidate_refs": children,
                    "retry_action": "split" if children else "retry_singleton",
                    "error_codes": list(receipt["errors"]),
                }
            )
            continue
        if (
            receipt["commit_allowed"] is not True
            or receipt["proof_authority"] != "existing_raw_grok_six_file_replay"
            or receipt["errors"] != []
            or receipt["source_artifact_count"] != 6
            or receipt["planned_native_x_call_count"] != len(batch["calls"])
            or receipt["attested_completed_native_x_call_count"] != len(batch["calls"])
        ):
            raise SourceNeutralMappingError("frontier_projection_accepted_receipt_invalid")
        fully_attested_candidate_refs.update(batch["candidate_refs"])
        accepted_receipts.append(receipt)

    for receipt in accepted_receipts:
        for row in receipt["references"]:
            if row["candidate_ref"] not in manifest_refs:
                raise SourceNeutralMappingError("frontier_reference_candidate_unknown")
            owner = stable_post_owners.setdefault(row["stable_post_id"], row["candidate_ref"])
            if owner != row["candidate_ref"]:
                raise SourceNeutralMappingError("frontier_cross_candidate_stable_post_collision")
            references.setdefault((row["candidate_ref"], row["stable_post_id"]), row)
        if any(row["candidate_ref"] not in manifest_refs for row in receipt["saturation_queue_seeds"]):
            raise SourceNeutralMappingError("frontier_saturation_candidate_unknown")
        saturation.extend(json.loads(canonical_json(receipt["saturation_queue_seeds"])))
    hydration = [
        {
            "candidate_ref": row["candidate_ref"],
            "stable_post_id": row["stable_post_id"],
            "url": row["url"],
            "expected_author_handle": row["author_handle"],
            "x_url_host": manifest["lab_descriptor"]["x_url_host"],
            "queue_reason": "exact_thread_hydration_required",
        }
        for row in references.values()
    ]
    counts: dict[str, int] = {}
    for candidate_ref, _ in references:
        counts[candidate_ref] = counts.get(candidate_ref, 0) + 1
    descriptor = manifest["lab_descriptor"]
    challenger: list[dict[str, Any]] = []
    for candidate in manifest["candidates"]:
        candidate_ref = candidate["candidate_ref"]
        if candidate_ref not in fully_attested_candidate_refs:
            continue
        states = {
            candidate["lab_affiliation_prior"]["state"],
            candidate["pretraining_experience_prior"]["state"],
        }
        sparse = counts.get(candidate_ref, 0) <= policy["queues"]["authored_sparse_max_references"]
        if not sparse and not states & {"ambiguous", "unsupported"}:
            continue
        seeds = [stable_id for ref, stable_id in references if ref == candidate_ref]
        for challenger_type in policy["queues"]["challenger_types"]:
            if challenger_type == "thread" and not seeds:
                continue
            challenger.append(
                {
                    "candidate_ref": candidate_ref,
                    "challenger_type": challenger_type,
                    "official_handles": list(descriptor["official_handles"]),
                    "project_aliases": list(descriptor["project_aliases"]),
                    "seed_stable_post_ids": seeds,
                    "relationship": "official_or_third_party_non_self",
                    "self_evidence_allowed": False,
                }
            )
    return {
        "pending_wave_p_queue": pending,
        "retry_split_queue": retry_split,
        "saturation_queue": saturation,
        "thread_hydration_queue": hydration,
        "luna_input_queue": [],
        "challenger_queue": challenger,
    }


def _validate_hydration_task(task: Any) -> Mapping[str, Any]:
    queued = _exact_keys(
        task,
        {
            "candidate_ref",
            "stable_post_id",
            "url",
            "expected_author_handle",
            "x_url_host",
            "queue_reason",
        },
        "thread_hydration_queue_shape_invalid",
    )
    _identifier(queued["candidate_ref"], "thread_hydration_candidate_ref_invalid")
    if not isinstance(queued["stable_post_id"], str) or _STATUS_ID_RE.fullmatch(queued["stable_post_id"]) is None:
        raise SourceNeutralMappingError("thread_hydration_stable_post_id_invalid")
    _handle(queued["expected_author_handle"], "thread_hydration_author_invalid")
    if queued["queue_reason"] != "exact_thread_hydration_required":
        raise SourceNeutralMappingError("thread_hydration_reason_invalid")
    return queued


def build_exact_post_hydration_projection(
    *,
    task: Mapping[str, Any],
    terminal: Mapping[str, Any],
    raw_session_files: Mapping[str, bytes],
    grok_precommit: GrokOperatorSessionPrecommit,
) -> ExactPostHydrationProjection:
    """Replay one exact x_thread_fetch and retain bytes for Luna handoff."""

    queued = _validate_hydration_task(task)
    try:
        assert_schema_valid(terminal, EXACT_HYDRATION_SCHEMA_FILE)
    except Exception as exc:
        raise SourceNeutralMappingError("exact_hydration_terminal_schema_invalid") from exc
    if terminal.get("schema_version") != EXACT_HYDRATION_SCHEMA_VERSION:
        raise SourceNeutralMappingError("exact_hydration_terminal_version_invalid")
    parsed = urlsplit(terminal["source_url"])
    parts = parsed.path.split("/")
    if (
        terminal["candidate_ref"] != queued["candidate_ref"]
        or terminal["requested_stable_post_id"] != queued["stable_post_id"]
        or terminal["returned_stable_post_id"] != queued["stable_post_id"]
        or terminal["source_url"] != queued["url"]
        or terminal["author_handle"].casefold() != queued["expected_author_handle"].casefold()
        or parsed.scheme != "https"
        or parsed.netloc != queued["x_url_host"]
        or parsed.query
        or parsed.fragment
        or parts != ["", terminal["author_handle"], "status", terminal["returned_stable_post_id"]]
        or terminal["lookup_status"] != "matched"
    ):
        raise SourceNeutralMappingError("exact_hydration_source_binding_invalid")
    try:
        source_text = terminal["full_text"].encode("utf-8")
    except (AttributeError, UnicodeError) as exc:
        raise SourceNeutralMappingError("exact_hydration_full_text_invalid") from exc
    if not source_text or hashlib.sha256(source_text).hexdigest() != terminal["full_text_sha256"]:
        raise SourceNeutralMappingError("exact_hydration_text_bytes_mismatch")
    try:
        replay = replay_grok_operator_session(
            raw_session_files,
            session_precommit=grok_precommit,
            allowed_tool_names=frozenset({"x_thread_fetch"}),
            expected_terminal=terminal,
        )
    except GrokOperatorSessionReplayError as exc:
        raise SourceNeutralMappingError("exact_hydration_raw_replay_invalid") from exc
    if len(replay.tool_completions) != 1:
        raise SourceNeutralMappingError("exact_hydration_call_count_invalid")
    completion = replay.tool_completions[0]
    try:
        arguments = json.loads(completion.arguments_json, object_pairs_hook=_strict_object)
    except json.JSONDecodeError as exc:
        raise SourceNeutralMappingError("exact_hydration_tool_arguments_invalid") from exc
    if completion.tool_name != "x_thread_fetch" or arguments != {"post_id": queued["stable_post_id"]}:
        raise SourceNeutralMappingError("exact_hydration_tool_binding_invalid")
    terminal_json = _canonical_json_bytes(terminal)
    task_sha256 = canonical_sha256(queued)
    projection_body = {
        "task_sha256": task_sha256,
        "terminal_sha256": replay.terminal_sha256,
        "session_precommit_sha256": session_precommit_sha256(grok_precommit),
        "source_artifact_sha256s": list(replay.source_artifact_sha256s),
        "source_text_sha256": hashlib.sha256(source_text).hexdigest(),
    }
    projection = ExactPostHydrationProjection(
        task_sha256=task_sha256,
        terminal_json=terminal_json,
        grok_precommit=grok_precommit,
        raw_session_artifacts=replay.source_artifacts,
        terminal_sha256=replay.terminal_sha256,
        source_text=source_text,
        projection_sha256=canonical_sha256(projection_body),
    )
    return projection


def _replay_exact_hydration_projection(
    projection: Any,
    *,
    task: Mapping[str, Any],
) -> tuple[ExactPostHydrationProjection, dict[str, Any]]:
    if type(projection) is not ExactPostHydrationProjection:
        raise SourceNeutralMappingError("exact_hydration_projection_required")
    terminal = strict_load_json_bytes(
        projection.terminal_json,
        error="exact_hydration_projection_terminal_invalid",
    )
    try:
        raw_session_files = thaw_raw_session_artifacts(projection.raw_session_artifacts)
    except GrokOperatorSessionReplayError as exc:
        raise SourceNeutralMappingError("exact_hydration_projection_raw_invalid") from exc
    recomputed = build_exact_post_hydration_projection(
        task=task,
        terminal=terminal,
        raw_session_files=raw_session_files,
        grok_precommit=projection.grok_precommit,
    )
    if recomputed != projection:
        raise SourceNeutralMappingError("exact_hydration_projection_content_mismatch")
    return recomputed, terminal


def build_luna_state_review_projection(
    *,
    hydration_projection: ExactPostHydrationProjection,
    result: Mapping[str, Any],
) -> LunaStateReviewProjection:
    try:
        assert_schema_valid(result, LUNA_REVIEW_SCHEMA_FILE)
    except Exception as exc:
        raise SourceNeutralMappingError("luna_review_schema_invalid") from exc
    terminal = strict_load_json_bytes(
        hydration_projection.terminal_json,
        error="luna_review_hydration_terminal_invalid",
    )
    if (
        result.get("schema_version") != LUNA_REVIEW_SCHEMA_VERSION
        or result["hydration_projection_sha256"] != hydration_projection.projection_sha256
        or result["candidate_ref"] != terminal["candidate_ref"]
        or result["stable_post_id"] != terminal["returned_stable_post_id"]
        or result["source_text_sha256"] != hashlib.sha256(hydration_projection.source_text).hexdigest()
    ):
        raise SourceNeutralMappingError("luna_review_source_binding_invalid")
    result_json = _canonical_json_bytes(result)
    return LunaStateReviewProjection(
        result_json=result_json,
        result_sha256=hashlib.sha256(result_json).hexdigest(),
    )


def _replay_luna_review_projection(
    projection: Any,
    *,
    hydration_projection: ExactPostHydrationProjection,
) -> dict[str, Any]:
    if type(projection) is not LunaStateReviewProjection:
        raise SourceNeutralMappingError("luna_review_projection_required")
    result = strict_load_json_bytes(projection.result_json, error="luna_review_projection_result_invalid")
    recomputed = build_luna_state_review_projection(
        hydration_projection=hydration_projection,
        result=result,
    )
    if recomputed != projection:
        raise SourceNeutralMappingError("luna_review_projection_content_mismatch")
    return result


def _review_coverage_status(expected: int, completed: int) -> str:
    if expected == 0:
        return "not_applicable"
    if completed == expected:
        return "complete"
    if completed == 0:
        return "not_started"
    return "incomplete"


def _collect_luna_reviews(
    *,
    hydration_by_sha: Mapping[str, ExactPostHydrationProjection],
    luna_review_projections: Sequence[LunaStateReviewProjection],
) -> dict[str, dict[str, Any]]:
    if type(luna_review_projections) not in {list, tuple}:
        raise SourceNeutralMappingError("luna_review_projections_invalid")
    checked: dict[str, dict[str, Any]] = {}
    review_ids: set[str] = set()
    for projection in luna_review_projections:
        if type(projection) is not LunaStateReviewProjection:
            raise SourceNeutralMappingError("luna_review_projection_required")
        raw = strict_load_json_bytes(projection.result_json, error="luna_review_projection_result_invalid")
        hydration_sha = raw.get("hydration_projection_sha256")
        if hydration_sha not in hydration_by_sha or hydration_sha in checked or raw.get("review_id") in review_ids:
            raise SourceNeutralMappingError("luna_review_coverage_invalid")
        if raw.get("authority_status") != "diagnostic_only_unattested":
            raise SourceNeutralMappingError("luna_review_authority_invalid")
        review_ids.add(raw["review_id"])
        checked[hydration_sha] = _replay_luna_review_projection(
            projection,
            hydration_projection=hydration_by_sha[hydration_sha],
        )
    return checked


def _build_luna_axis_reduction(
    *,
    manifest: Mapping[str, Any],
    hydration_by_sha: Mapping[str, ExactPostHydrationProjection],
    checked_luna: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    """Reduce diagnostic Post proposals once per candidate/axis/version.

    Caller-created Luna proposals are preserved for analysis, but the frozen
    manifest prior remains the resolved state and no transition is authorized.
    This prevents a selected Post, repeated Posts, or a contradictory model run
    from upgrading or downgrading a candidate state.
    """

    validate_candidate_manifest(manifest)
    if any(key not in hydration_by_sha for key in checked_luna):
        raise SourceNeutralMappingError("luna_axis_reduction_hydration_unknown")
    expected_by_candidate: dict[str, int] = {row["candidate_ref"]: 0 for row in manifest["candidates"]}
    expected_evidence_by_candidate: dict[str, list[str]] = {row["candidate_ref"]: [] for row in manifest["candidates"]}
    reviewed_by_candidate: dict[str, list[tuple[str, Mapping[str, Any]]]] = {
        row["candidate_ref"]: [] for row in manifest["candidates"]
    }
    for hydration_sha, projection in hydration_by_sha.items():
        terminal = strict_load_json_bytes(
            projection.terminal_json,
            error="luna_axis_reduction_hydration_terminal_invalid",
        )
        candidate_ref = terminal["candidate_ref"]
        if candidate_ref not in expected_by_candidate:
            raise SourceNeutralMappingError("luna_axis_reduction_candidate_unknown")
        expected_by_candidate[candidate_ref] += 1
        expected_evidence_by_candidate[candidate_ref].append(hydration_sha)
        if hydration_sha in checked_luna:
            reviewed_by_candidate[candidate_ref].append((hydration_sha, checked_luna[hydration_sha]))

    rows: list[dict[str, Any]] = []
    axis_fields = (
        ("lab_affiliation", "lab_affiliation_prior", "proposed_lab_affiliation_state"),
        (
            "pretraining_experience",
            "pretraining_experience_prior",
            "proposed_pretraining_experience_state",
        ),
    )
    for candidate in manifest["candidates"]:
        candidate_ref = candidate["candidate_ref"]
        expected = expected_by_candidate[candidate_ref]
        reviewed = sorted(reviewed_by_candidate[candidate_ref], key=lambda item: item[0])
        completed = len(reviewed)
        row_coverage = "not_applicable" if expected == 0 else _review_coverage_status(expected, completed)
        for axis, prior_field, proposal_field in axis_fields:
            proposed_states = {item[1][proposal_field] for item in reviewed}
            proposed_state: str | None
            if row_coverage != "complete" or not proposed_states:
                proposed_state = None
            elif len(proposed_states) == 1:
                proposed_state = next(iter(proposed_states))
            else:
                proposed_state = "ambiguous"
            prior = candidate[prior_field]
            expected_evidence = sorted(expected_evidence_by_candidate[candidate_ref])
            reviewed_evidence = [item[0] for item in reviewed]
            proposal_set = [
                {
                    "hydration_projection_sha256": hydration_sha,
                    "proposed_state": result[proposal_field],
                }
                for hydration_sha, result in reviewed
            ]
            state_version_sha256 = canonical_sha256(
                {
                    "candidate_manifest_sha256": manifest["manifest_sha256"],
                    "candidate_ref": candidate_ref,
                    "axis": axis,
                    "frozen_prior": prior,
                }
            )
            rows.append(
                {
                    "candidate_ref": candidate_ref,
                    "axis": axis,
                    "state_version_sha256": state_version_sha256,
                    "prior_state": prior["state"],
                    "prior_evidence_status": prior["evidence_status"],
                    "expected_evidence_count": expected,
                    "reviewed_evidence_count": completed,
                    "expected_evidence_manifest_sha256": canonical_sha256(expected_evidence),
                    "reviewed_evidence_manifest_sha256": canonical_sha256(reviewed_evidence),
                    "proposal_set_sha256": canonical_sha256(proposal_set),
                    "coverage_status": row_coverage,
                    "diagnostic_proposed_state": proposed_state,
                    "resolved_state": prior["state"],
                    "transition_status": "not_authorized",
                    "transition_id": None,
                }
            )
    unique_row_keys = {(row["candidate_ref"], row["axis"], row["state_version_sha256"]) for row in rows}
    if len(rows) != len(manifest["candidates"]) * 2 or len(unique_row_keys) != len(rows):
        raise SourceNeutralMappingError("luna_axis_reduction_row_cardinality_invalid")
    expected_total = len(hydration_by_sha)
    completed_total = len(checked_luna)
    payload: dict[str, Any] = {
        "schema_version": LUNA_AXIS_REDUCTION_SCHEMA_VERSION,
        "reducer_version": "source_neutral_mapping_luna_axis_reducer.v1",
        "candidate_manifest_sha256": manifest["manifest_sha256"],
        "expected_review_count": expected_total,
        "completed_review_count": completed_total,
        "coverage_status": _review_coverage_status(expected_total, completed_total),
        "transition_authority": "diagnostic_only_unattested",
        "candidate_axis_rows": rows,
        "authorized_transition_count": 0,
        "reduction_sha256": "",
    }
    payload["reduction_sha256"] = _content_sha256(payload, "reduction_sha256")
    assert_schema_valid(payload, LUNA_AXIS_REDUCTION_SCHEMA_FILE)
    return payload


def _validated_hydration_outputs(
    *,
    manifest: Mapping[str, Any],
    plan: Mapping[str, Any],
    session_projections: Sequence[MappingSessionProjection],
    hydration_projections: Sequence[ExactPostHydrationProjection],
    policy: Mapping[str, Any],
) -> tuple[list[dict[str, Any]], dict[str, ExactPostHydrationProjection]]:
    queues = build_frontier_queues(
        manifest=manifest,
        plan=plan,
        session_projections=session_projections,
        policy=policy,
    )
    tasks = {canonical_sha256(row): row for row in queues["thread_hydration_queue"]}
    if type(hydration_projections) not in {list, tuple}:
        raise SourceNeutralMappingError("exact_hydration_projections_invalid")
    output: list[dict[str, Any]] = []
    checked_by_sha: dict[str, ExactPostHydrationProjection] = {}
    session_ids: set[str] = set()
    request_ids: set[str] = set()
    for projection in hydration_projections:
        if type(projection) is not ExactPostHydrationProjection:
            raise SourceNeutralMappingError("exact_hydration_projection_required")
        if projection.task_sha256 not in tasks:
            raise SourceNeutralMappingError("exact_hydration_projection_task_unknown")
        if (
            projection.projection_sha256 in checked_by_sha
            or projection.grok_precommit.expected_session_id in session_ids
            or projection.grok_precommit.expected_request_id in request_ids
        ):
            raise SourceNeutralMappingError("exact_hydration_projection_duplicate")
        checked, terminal = _replay_exact_hydration_projection(
            projection,
            task=tasks[projection.task_sha256],
        )
        checked_by_sha[projection.projection_sha256] = checked
        session_ids.add(projection.grok_precommit.expected_session_id)
        request_ids.add(projection.grok_precommit.expected_request_id)
        output.append(
            {
                "candidate_ref": terminal["candidate_ref"],
                "stable_post_id": terminal["returned_stable_post_id"],
                "source_url": terminal["source_url"],
                "author_handle": terminal["author_handle"],
                "source_text": checked.source_text.decode("utf-8"),
                "source_text_sha256": hashlib.sha256(checked.source_text).hexdigest(),
                "hydration_projection_sha256": checked.projection_sha256,
                "source_binding_status": "exact_source_bound",
                "review_purpose": "lab_and_pretraining_temporal_state_transition",
            }
        )
    mapping_session_ids = [projection.grok_precommit.expected_session_id for projection in session_projections]
    mapping_request_ids = [projection.grok_precommit.expected_request_id for projection in session_projections]
    hydration_session_ids = [projection.grok_precommit.expected_session_id for projection in hydration_projections]
    hydration_request_ids = [projection.grok_precommit.expected_request_id for projection in hydration_projections]
    all_session_ids = mapping_session_ids + hydration_session_ids
    all_request_ids = mapping_request_ids + hydration_request_ids
    if len(all_session_ids) != len(set(all_session_ids)) or len(all_request_ids) != len(set(all_request_ids)):
        raise SourceNeutralMappingError("execution_identity_registry_duplicate")
    return output, checked_by_sha


def build_luna_input_queue(
    *,
    manifest: Mapping[str, Any],
    plan: Mapping[str, Any],
    session_projections: Sequence[MappingSessionProjection],
    hydration_projections: Sequence[ExactPostHydrationProjection],
    policy: Mapping[str, Any],
) -> list[dict[str, Any]]:
    output, _ = _validated_hydration_outputs(
        manifest=manifest,
        plan=plan,
        session_projections=session_projections,
        hydration_projections=hydration_projections,
        policy=policy,
    )
    return output


def build_luna_axis_reduction(
    *,
    manifest: Mapping[str, Any],
    plan: Mapping[str, Any],
    session_projections: Sequence[MappingSessionProjection],
    hydration_projections: Sequence[ExactPostHydrationProjection],
    luna_review_projections: Sequence[LunaStateReviewProjection],
    policy: Mapping[str, Any],
) -> dict[str, Any]:
    """Build the deterministic diagnostic candidate-axis reduction."""

    _, checked_hydrations = _validated_hydration_outputs(
        manifest=manifest,
        plan=plan,
        session_projections=session_projections,
        hydration_projections=hydration_projections,
        policy=policy,
    )
    checked_luna = _collect_luna_reviews(
        hydration_by_sha=checked_hydrations,
        luna_review_projections=luna_review_projections,
    )
    return _build_luna_axis_reduction(
        manifest=manifest,
        hydration_by_sha=checked_hydrations,
        checked_luna=checked_luna,
    )


def _derive_semantic_strategy_payload(plan: Mapping[str, Any]) -> dict[str, Any]:
    """Describe retrieval semantics without scheduling or execution identity."""

    cells: dict[str, dict[str, Any]] = {}
    for batch in plan["batches"]:
        for call in batch["calls"]:
            aliases = list(call["topic_aliases"])
            cell = {
                "tool_name": call["tool_name"],
                "query_template": f"from:{{candidate_handle}} ({' OR '.join(aliases)})",
                "topic_aliases": aliases,
                "mode": call["arguments"]["mode"],
                "request_limit": call["arguments"]["limit"],
                "time_window": None,
            }
            cells[canonical_json(cell)] = cell
    return {
        "strategy_family": "wave_p",
        "query_surface": "candidate_authored",
        "relationship_topology": "self_authored",
        "retrieval_cells": [cells[key] for key in sorted(cells)],
    }


def _derive_executed_wave_facts(
    *,
    campaign_id: str,
    wave_id: str,
    manifest: Mapping[str, Any],
    policy: Mapping[str, Any],
    plan: Mapping[str, Any],
    session_projections: Sequence[MappingSessionProjection],
    hydration_projections: Sequence[ExactPostHydrationProjection],
    luna_review_projections: Sequence[LunaStateReviewProjection],
    predecessor_wave_facts: ExecutedWaveFacts | None,
    replay_seen_object_ids: set[int] | None = None,
    replay_depth: int = 0,
) -> ExecutedWaveFacts:
    _identifier(campaign_id, "executed_wave_campaign_id_invalid")
    _identifier(wave_id, "executed_wave_id_invalid")
    validate_wave_plan(plan, manifest=manifest, policy=policy)
    strategy = _derive_semantic_strategy_payload(plan)
    if type(session_projections) not in {list, tuple}:
        raise SourceNeutralMappingError("executed_wave_session_projections_invalid")
    if type(hydration_projections) not in {list, tuple} or type(luna_review_projections) not in {list, tuple}:
        raise SourceNeutralMappingError("executed_wave_downstream_projections_invalid")

    checked_predecessor: ExecutedWaveFacts | None = None
    if predecessor_wave_facts is not None:
        checked_predecessor = _replay_executed_wave_facts(
            predecessor_wave_facts,
            seen_object_ids=replay_seen_object_ids,
            depth=replay_depth + 1,
        )
        if checked_predecessor.campaign_id != campaign_id:
            raise SourceNeutralMappingError("executed_wave_campaign_mismatch")
        if checked_predecessor.manifest_json != _canonical_json_bytes(manifest):
            raise SourceNeutralMappingError("executed_wave_manifest_lineage_mismatch")
    campaign_ordinal = 0 if checked_predecessor is None else checked_predecessor.campaign_ordinal + 1
    predecessor_digest = None if checked_predecessor is None else checked_predecessor.wave_facts_sha256
    prior_stable_ids = () if checked_predecessor is None else checked_predecessor.cumulative_stable_post_ids
    prior_frontier_sha256 = (
        canonical_sha256(
            {
                "campaign_id": campaign_id,
                "candidate_manifest_sha256": manifest["manifest_sha256"],
                "frontier_ordinal": -1,
                "predecessor_wave_facts_sha256": None,
                "stable_post_ids": [],
            }
        )
        if checked_predecessor is None
        else checked_predecessor.cumulative_frontier_sha256
    )

    queues = build_frontier_queues(
        manifest=manifest,
        plan=plan,
        session_projections=session_projections,
        policy=policy,
    )
    if queues["pending_wave_p_queue"] or queues["retry_split_queue"]:
        raise SourceNeutralMappingError("executed_wave_plan_execution_incomplete")
    hydration_outputs, checked_hydrations = _validated_hydration_outputs(
        manifest=manifest,
        plan=plan,
        session_projections=session_projections,
        hydration_projections=hydration_projections,
        policy=policy,
    )
    if len(hydration_outputs) != len(queues["thread_hydration_queue"]):
        raise SourceNeutralMappingError("executed_wave_hydration_incomplete")
    hydration_by_sha = checked_hydrations
    checked_luna = _collect_luna_reviews(
        hydration_by_sha=hydration_by_sha,
        luna_review_projections=luna_review_projections,
    )
    reduction = _build_luna_axis_reduction(
        manifest=manifest,
        hydration_by_sha=hydration_by_sha,
        checked_luna=checked_luna,
    )

    receipt_by_batch: dict[str, tuple[MappingSessionProjection, dict[str, Any]]] = {}
    for projection in session_projections:
        precommit, receipt = _replay_mapping_session_projection(projection)
        receipt_by_batch[precommit["batch_id"]] = (projection, receipt)
    completed = tuple(
        batch["batch_id"]
        for batch in plan["batches"]
        if batch["batch_id"] in receipt_by_batch and receipt_by_batch[batch["batch_id"]][1]["status"] == "accepted"
    )
    rejected = tuple(
        batch["batch_id"]
        for batch in plan["batches"]
        if batch["batch_id"] in receipt_by_batch and receipt_by_batch[batch["batch_id"]][1]["status"] == "rejected"
    )
    planned = tuple(batch["batch_id"] for batch in plan["batches"])
    expected_refs = tuple(row["candidate_ref"] for row in manifest["candidates"])
    covered_refs = tuple(
        ref for batch in plan["batches"] if batch["batch_id"] in completed for ref in batch["candidate_refs"]
    )
    all_stable_ids = sorted(
        {
            row["stable_post_id"]
            for _, receipt in receipt_by_batch.values()
            if receipt["status"] == "accepted"
            for row in receipt["references"]
        },
        key=int,
    )
    prior = set(prior_stable_ids)
    new_ids = tuple(item for item in all_stable_ids if item not in prior)
    cumulative_stable_post_ids = tuple(sorted(prior | set(all_stable_ids), key=int))
    cumulative_frontier_sha256 = canonical_sha256(
        {
            "campaign_id": campaign_id,
            "campaign_ordinal": campaign_ordinal,
            "predecessor_frontier_sha256": prior_frontier_sha256,
            "added_stable_post_ids": list(new_ids),
            "cumulative_stable_post_ids": list(cumulative_stable_post_ids),
        }
    )

    current_session_ids = [
        projection.grok_precommit.expected_session_id
        for projection in (*tuple(session_projections), *tuple(hydration_projections))
    ]
    current_request_ids = [
        projection.grok_precommit.expected_request_id
        for projection in (*tuple(session_projections), *tuple(hydration_projections))
    ]
    current_execution_digests = [
        *(projection.receipt_sha256 for projection in session_projections),
        *(projection.projection_sha256 for projection in hydration_projections),
    ]
    current_luna_digests = [projection.result_sha256 for projection in luna_review_projections]
    current_review_ids = [result["review_id"] for result in checked_luna.values()]
    historical_session_ids: set[str] = set()
    historical_request_ids: set[str] = set()
    historical_execution_digests: set[str] = set()
    historical_luna_digests: set[str] = set()
    historical_review_ids: set[str] = set()
    cursor = checked_predecessor
    while cursor is not None:
        for projection in (*cursor.session_projections, *cursor.hydration_projections):
            historical_session_ids.add(projection.grok_precommit.expected_session_id)
            historical_request_ids.add(projection.grok_precommit.expected_request_id)
        historical_execution_digests.update(cursor.session_projection_sha256s)
        historical_execution_digests.update(cursor.exact_hydration_projection_sha256s)
        historical_luna_digests.update(cursor.luna_result_sha256s)
        for projection in cursor.luna_review_projections:
            result = strict_load_json_bytes(
                projection.result_json,
                error="executed_wave_historical_luna_result_invalid",
            )
            historical_review_ids.add(result["review_id"])
        cursor = cursor.predecessor_wave_facts
    if (
        len(current_session_ids) != len(set(current_session_ids))
        or len(current_request_ids) != len(set(current_request_ids))
        or len(current_execution_digests) != len(set(current_execution_digests))
        or len(current_luna_digests) != len(set(current_luna_digests))
        or len(current_review_ids) != len(set(current_review_ids))
        or historical_session_ids.intersection(current_session_ids)
        or historical_request_ids.intersection(current_request_ids)
        or historical_execution_digests.intersection(current_execution_digests)
        or historical_luna_digests.intersection(current_luna_digests)
        or historical_review_ids.intersection(current_review_ids)
    ):
        raise SourceNeutralMappingError("campaign_execution_identity_reused_without_cache_projection")

    remaining_queues = json.loads(canonical_json(queues))
    remaining_queues["thread_hydration_queue"] = []
    remaining_queues["luna_input_queue"] = [
        row for row in hydration_outputs if row["hydration_projection_sha256"] not in checked_luna
    ]
    remaining_queues_json = _canonical_json_bytes(remaining_queues)
    reduction_json = _canonical_json_bytes(reduction)
    session_projection_sha256s = tuple(receipt_by_batch[batch_id][0].receipt_sha256 for batch_id in completed)
    exact_hydration_projection_sha256s = tuple(sorted(hydration_by_sha))
    luna_result_sha256s = tuple(sorted(item.result_sha256 for item in luna_review_projections))
    remaining_queue_state_sha256 = hashlib.sha256(remaining_queues_json).hexdigest()
    wave_facts_body = {
        "campaign_id": campaign_id,
        "campaign_ordinal": campaign_ordinal,
        "wave_id": wave_id,
        "predecessor_wave_facts_sha256": predecessor_digest,
        "prior_frontier_sha256": prior_frontier_sha256,
        "cumulative_frontier_sha256": cumulative_frontier_sha256,
        "cumulative_stable_post_ids": list(cumulative_stable_post_ids),
        "manifest_sha256": manifest["manifest_sha256"],
        "policy_sha256": canonical_sha256(policy),
        "plan_sha256": plan["plan_sha256"],
        "strategy_signature_sha256": canonical_sha256(strategy),
        "planned_work_item_ids": list(planned),
        "completed_work_item_ids": list(completed),
        "rejected_work_item_ids": list(rejected),
        "covered_candidate_refs": list(covered_refs),
        "expected_candidate_refs": list(expected_refs),
        "session_projection_sha256s": list(session_projection_sha256s),
        "exact_hydration_projection_sha256s": list(exact_hydration_projection_sha256s),
        "luna_result_sha256s": list(luna_result_sha256s),
        "new_stable_post_ids": list(new_ids),
        "luna_axis_reduction_sha256": reduction["reduction_sha256"],
        "semantic_review_coverage_status": reduction["coverage_status"],
        "semantic_transition_authority": "diagnostic_only_unattested",
        "authorized_semantic_transition_ids": [],
        "remaining_queue_state_sha256": remaining_queue_state_sha256,
    }
    wave_facts_sha256 = canonical_sha256(wave_facts_body)
    return ExecutedWaveFacts(
        campaign_id=campaign_id,
        campaign_ordinal=campaign_ordinal,
        wave_id=wave_id,
        predecessor_wave_facts=checked_predecessor,
        predecessor_wave_facts_sha256=predecessor_digest,
        prior_frontier_sha256=prior_frontier_sha256,
        cumulative_stable_post_ids=cumulative_stable_post_ids,
        cumulative_frontier_sha256=cumulative_frontier_sha256,
        manifest_json=_canonical_json_bytes(manifest),
        policy_json=_canonical_json_bytes(policy),
        plan_json=_canonical_json_bytes(plan),
        strategy_payload_json=_canonical_json_bytes(strategy),
        session_projections=tuple(session_projections),
        hydration_projections=tuple(hydration_projections),
        luna_review_projections=tuple(luna_review_projections),
        strategy_signature_sha256=canonical_sha256(strategy),
        planned_work_item_ids=planned,
        completed_work_item_ids=completed,
        rejected_work_item_ids=rejected,
        covered_candidate_refs=covered_refs,
        expected_candidate_refs=expected_refs,
        session_projection_sha256s=session_projection_sha256s,
        exact_hydration_projection_sha256s=exact_hydration_projection_sha256s,
        luna_result_sha256s=luna_result_sha256s,
        new_stable_post_ids=new_ids,
        luna_axis_reduction_json=reduction_json,
        luna_axis_reduction_sha256=reduction["reduction_sha256"],
        semantic_review_coverage_status=reduction["coverage_status"],
        semantic_transition_authority="diagnostic_only_unattested",
        authorized_semantic_transition_ids=(),
        remaining_queues_json=remaining_queues_json,
        remaining_queue_state_sha256=remaining_queue_state_sha256,
        wave_facts_sha256=wave_facts_sha256,
    )


def build_executed_wave_facts(
    *,
    campaign_id: str,
    wave_id: str,
    manifest: Mapping[str, Any],
    policy: Mapping[str, Any],
    plan: Mapping[str, Any],
    session_projections: Sequence[MappingSessionProjection],
    hydration_projections: Sequence[ExactPostHydrationProjection],
    luna_review_projections: Sequence[LunaStateReviewProjection],
    predecessor_wave_facts: ExecutedWaveFacts | None = None,
) -> ExecutedWaveFacts:
    return _derive_executed_wave_facts(
        campaign_id=campaign_id,
        wave_id=wave_id,
        manifest=manifest,
        policy=policy,
        plan=plan,
        session_projections=session_projections,
        hydration_projections=hydration_projections,
        luna_review_projections=luna_review_projections,
        predecessor_wave_facts=predecessor_wave_facts,
    )


def _replay_executed_wave_facts(
    value: Any,
    *,
    seen_object_ids: set[int] | None = None,
    depth: int = 0,
) -> ExecutedWaveFacts:
    if type(value) is not ExecutedWaveFacts:
        raise SourceNeutralMappingError("executed_wave_facts_required")
    if depth > 256:
        raise SourceNeutralMappingError("executed_wave_lineage_depth_invalid")
    seen = set() if seen_object_ids is None else seen_object_ids
    if id(value) in seen:
        raise SourceNeutralMappingError("executed_wave_lineage_cycle")
    seen.add(id(value))
    manifest = strict_load_json_bytes(value.manifest_json, error="executed_wave_manifest_invalid")
    policy = strict_load_json_bytes(value.policy_json, error="executed_wave_policy_invalid")
    plan = strict_load_json_bytes(value.plan_json, error="executed_wave_plan_invalid")
    try:
        recomputed = _derive_executed_wave_facts(
            campaign_id=value.campaign_id,
            wave_id=value.wave_id,
            manifest=manifest,
            policy=policy,
            plan=plan,
            session_projections=value.session_projections,
            hydration_projections=value.hydration_projections,
            luna_review_projections=value.luna_review_projections,
            predecessor_wave_facts=value.predecessor_wave_facts,
            replay_seen_object_ids=seen,
            replay_depth=depth,
        )
        if recomputed != value:
            raise SourceNeutralMappingError("executed_wave_facts_content_mismatch")
        return recomputed
    finally:
        seen.remove(id(value))


def structural_stop(
    *,
    queues: Mapping[str, Sequence[Any]],
    recent_waves: Sequence[ExecutedWaveFacts],
    policy: Mapping[str, Any],
) -> dict[str, Any]:
    validate_policy(policy)
    queue_shape_valid = (
        isinstance(queues, Mapping)
        and set(queues) == set(QUEUE_KEYS)
        and all(isinstance(queues[key], list) for key in QUEUE_KEYS)
    )
    required = policy["stop"]["consecutive_materially_distinct_zero_waves"]
    trailing = list(recent_waves[-required:]) if type(recent_waves) in {list, tuple} else []
    checked: list[ExecutedWaveFacts] = []
    if len(trailing) == required:
        try:
            checked = [_replay_executed_wave_facts(row) for row in trailing]
        except Exception:  # fail closed to continue_mapping at the public stop boundary
            checked = []
    derived_queues: dict[str, Any] | None = None
    if checked:
        try:
            derived_queues = strict_load_json_bytes(
                checked[-1].remaining_queues_json,
                error="executed_wave_remaining_queues_invalid",
            )
        except SourceNeutralMappingError:
            derived_queues = None
    try:
        remaining_states = [
            strict_load_json_bytes(
                row.remaining_queues_json,
                error="executed_wave_remaining_queues_invalid",
            )
            for row in checked
        ]
    except SourceNeutralMappingError:
        checked = []
        remaining_states = []
    queue_state_matches = False
    if queue_shape_valid and derived_queues is not None:
        try:
            queue_state_matches = canonical_json(queues) == canonical_json(derived_queues)
        except (TypeError, ValueError):
            queue_state_matches = False
    queues_empty = queue_state_matches and all(not derived_queues[key] for key in QUEUE_KEYS)
    mechanically_zero_waves = len(checked) == required and all(
        row.planned_work_item_ids
        and row.completed_work_item_ids == row.planned_work_item_ids
        and not row.rejected_work_item_ids
        and row.covered_candidate_refs == row.expected_candidate_refs
        and not row.new_stable_post_ids
        and not row.authorized_semantic_transition_ids
        and all(not remaining_states[index][key] for key in QUEUE_KEYS)
        for index, row in enumerate(checked)
    )
    lineage_adjacent = mechanically_zero_waves and all(
        current.campaign_id == previous.campaign_id
        and current.campaign_ordinal == previous.campaign_ordinal + 1
        and current.predecessor_wave_facts_sha256 == previous.wave_facts_sha256
        and current.prior_frontier_sha256 == previous.cumulative_frontier_sha256
        and current.predecessor_wave_facts == previous
        for previous, current in zip(checked[:-1], checked[1:], strict=True)
    )
    campaign_continuity = (
        mechanically_zero_waves
        and lineage_adjacent
        and len({row.campaign_id for row in checked}) == 1
        and len({row.manifest_json for row in checked}) == 1
    )
    semantic_transition_authoritative = mechanically_zero_waves and all(
        row.semantic_transition_authority == "receipt_first_authoritative_complete" for row in checked
    )
    session_ids = [
        projection.grok_precommit.expected_session_id
        for row in checked
        for projection in (*row.session_projections, *row.hydration_projections)
    ]
    request_ids = [
        projection.grok_precommit.expected_request_id
        for row in checked
        for projection in (*row.session_projections, *row.hydration_projections)
    ]
    execution_digests = [
        digest
        for row in checked
        for digest in (*row.session_projection_sha256s, *row.exact_hydration_projection_sha256s)
    ]
    cross_wave_execution_distinct = (
        len(session_ids) == len(set(session_ids))
        and len(request_ids) == len(set(request_ids))
        and len(execution_digests) == len(set(execution_digests))
    )
    distinct = (
        mechanically_zero_waves
        and campaign_continuity
        and semantic_transition_authoritative
        and cross_wave_execution_distinct
        and len({row.wave_id for row in checked}) == required
        and len({row.strategy_signature_sha256 for row in checked}) == required
    )
    stopped = queues_empty and bool(distinct)
    return {
        "stop": stopped,
        "queues_empty": queues_empty,
        "trailing_zero_wave_count": len(trailing) if mechanically_zero_waves else 0,
        "materially_distinct": bool(distinct),
        "lineage_adjacent": bool(lineage_adjacent),
        "semantic_transition_authoritative": bool(semantic_transition_authoritative),
        "reason": (
            "structural_convergence"
            if stopped
            else "semantic_transition_authority_unavailable"
            if mechanically_zero_waves and not semantic_transition_authoritative
            else "continue_mapping"
        ),
    }


def _rate(numerator: int, denominator: int) -> float | None:
    return round(numerator / denominator, 6) if denominator else None


def build_candidate_free_aggregate(
    *,
    manifest: Mapping[str, Any],
    policy: Mapping[str, Any],
    plan: Mapping[str, Any],
    session_projections: Sequence[MappingSessionProjection],
    hydration_projections: Sequence[ExactPostHydrationProjection] = (),
    luna_review_projections: Sequence[LunaStateReviewProjection] = (),
) -> dict[str, Any]:
    validate_wave_plan(plan, manifest=manifest, policy=policy)
    build_frontier_queues(
        manifest=manifest,
        plan=plan,
        session_projections=session_projections,
        policy=policy,
    )
    receipt_rows: list[dict[str, Any]] = []
    receipts: list[dict[str, Any]] = []
    for projection in session_projections:
        precommit, receipt = _replay_mapping_session_projection(projection)
        receipt_rows.append(
            {
                "batch_id": precommit["batch_id"],
                "receipt_sha256": receipt["receipt_sha256"],
                "status": receipt["status"],
            }
        )
        receipts.append(receipt)
    receipt_rows.sort(key=lambda row: row["batch_id"])
    planned_native_x_calls = plan["planned_native_x_call_count"]
    attested_completed_native_x_calls = sum(receipt["attested_completed_native_x_call_count"] for receipt in receipts)
    stable_ids = {
        row["stable_post_id"]
        for receipt in receipts
        if receipt["status"] == "accepted"
        for row in receipt["references"]
    }
    unique_stable_post_ids = len(stable_ids)
    hydration_outputs, checked_hydrations = _validated_hydration_outputs(
        manifest=manifest,
        plan=plan,
        session_projections=session_projections,
        hydration_projections=hydration_projections,
        policy=policy,
    )
    exact_source_bound_hydrations = len(hydration_outputs)
    try:
        checked_luna = _collect_luna_reviews(
            hydration_by_sha=checked_hydrations,
            luna_review_projections=luna_review_projections,
        )
    except SourceNeutralMappingError as exc:
        raise SourceNeutralMappingError("aggregate_luna_hydration_binding_invalid") from exc
    reduction = _build_luna_axis_reduction(
        manifest=manifest,
        hydration_by_sha=checked_hydrations,
        checked_luna=checked_luna,
    )
    luna_terminal_reviews = len(checked_luna)
    if (
        attested_completed_native_x_calls > planned_native_x_calls
        or unique_stable_post_ids > attested_completed_native_x_calls * policy["search"]["request_limit"]
        or exact_source_bound_hydrations > unique_stable_post_ids
        or luna_terminal_reviews > exact_source_bound_hydrations
        or (attested_completed_native_x_calls == 0 and unique_stable_post_ids != 0)
        or (unique_stable_post_ids == 0 and (exact_source_bound_hydrations or luna_terminal_reviews))
        or (exact_source_bound_hydrations == 0 and luna_terminal_reviews != 0)
    ):
        raise SourceNeutralMappingError("aggregate_denominator_violation")
    bindings = {
        "policy_sha256": canonical_sha256(policy),
        "candidate_manifest_sha256": manifest["manifest_sha256"],
        "wave_plan_sha256": plan["plan_sha256"],
        "session_receipt_manifest_sha256": canonical_sha256(receipt_rows),
        "hydration_receipt_manifest_sha256": canonical_sha256(
            sorted(projection.projection_sha256 for projection in hydration_projections)
        ),
        "luna_result_manifest_sha256": canonical_sha256(
            sorted(projection.result_sha256 for projection in luna_review_projections)
        ),
        "luna_axis_reduction_sha256": reduction["reduction_sha256"],
    }
    payload: dict[str, Any] = {
        "schema_version": AGGREGATE_SCHEMA_VERSION,
        "privacy_class": "candidate_free_counts_and_hashes_only",
        "claim_status": "diagnostic_only",
        "bindings": bindings,
        "metric_denominators": {
            "execution_compliance": planned_native_x_calls,
            "stable_post_id_retrieval": attested_completed_native_x_calls,
            "exact_hydration": unique_stable_post_ids,
            "luna_diagnostic_review_coverage": exact_source_bound_hydrations,
        },
        "metric_numerators": {
            "execution_compliance": attested_completed_native_x_calls,
            "stable_post_id_retrieval": unique_stable_post_ids,
            "exact_hydration": exact_source_bound_hydrations,
            "luna_diagnostic_review_coverage": luna_terminal_reviews,
        },
        "metric_rates": {
            "execution_compliance": _rate(attested_completed_native_x_calls, planned_native_x_calls),
            "stable_post_id_retrieval": _rate(unique_stable_post_ids, attested_completed_native_x_calls),
            "exact_hydration": _rate(exact_source_bound_hydrations, unique_stable_post_ids),
            "luna_diagnostic_review_coverage": _rate(luna_terminal_reviews, exact_source_bound_hydrations),
        },
        "semantic_review_coverage_status": reduction["coverage_status"],
        "semantic_transition_authority": "diagnostic_only_unattested",
        "authorized_semantic_transition_count": 0,
        "model_call_counts_included": False,
        "aggregate_sha256": "",
    }
    payload["aggregate_sha256"] = _content_sha256(payload, "aggregate_sha256")
    assert_schema_valid(payload, AGGREGATE_SCHEMA_FILE)
    return payload


def validate_private_calibration_binding(
    *,
    calibration: Mapping[str, Any],
    private_receipt_path: Path,
    private_summary_path: Path,
) -> None:
    receipt_bytes = private_receipt_path.read_bytes()
    summary_bytes = private_summary_path.read_bytes()
    bindings = calibration["source_bindings"]
    if hashlib.sha256(receipt_bytes).hexdigest() != bindings["private_receipt_sha256"]:
        raise SourceNeutralMappingError("calibration_private_receipt_hash_mismatch")
    if hashlib.sha256(summary_bytes).hexdigest() != bindings["private_summary_sha256"]:
        raise SourceNeutralMappingError("calibration_private_summary_hash_mismatch")
    receipt = strict_load_json(private_receipt_path)
    _exact_keys(
        receipt,
        {
            "builder_source_sha256",
            "candidate_fields_in_summary",
            "raw_files_modified",
            "schema_version",
            "source_binding_count",
            "source_bindings",
            "summary_sha256",
        },
        "calibration_private_receipt_shape_invalid",
    )
    if (
        receipt["schema_version"] != "x_first.gdm_query_family_calibration_receipt.v1"
        or receipt["candidate_fields_in_summary"] is not False
        or receipt["raw_files_modified"] is not False
        or receipt["summary_sha256"] != bindings["private_summary_sha256"]
        or type(receipt["source_binding_count"]) is not int
        or receipt["source_binding_count"] != len(receipt["source_bindings"])
        or receipt["source_binding_count"] != bindings["private_source_binding_count"]
        or canonical_sha256(receipt["source_bindings"]) != bindings["private_source_binding_manifest_sha256"]
    ):
        raise SourceNeutralMappingError("calibration_private_source_binding_invalid")


def validate_checked_in_assets(
    root: Path | None = None,
    *,
    private_calibration_receipt: Path | None = None,
    private_calibration_summary: Path | None = None,
) -> list[str]:
    base = root or project_root()
    errors: list[str] = []
    try:
        policy = strict_load_json(base / "configs" / "source_neutral_mapping_policy.v1.json")
        validate_policy(policy)
        manifest = strict_load_json(base / "fixtures" / "source_neutral_mapping_manifest_fixture_v1.json")
        validate_candidate_manifest(manifest)
        calibration = strict_load_json(
            base / "docs" / "live-evidence" / "2026-07-17-gdm-query-family-calibration.aggregate.v1.json"
        )
        assert_schema_valid(calibration, CALIBRATION_SCHEMA_FILE)
        if calibration["aggregate_sha256"] != _content_sha256(calibration, "aggregate_sha256"):
            raise SourceNeutralMappingError("calibration_aggregate_hash_mismatch")
        if (private_calibration_receipt is None) != (private_calibration_summary is None):
            raise SourceNeutralMappingError("calibration_private_binding_paths_incomplete")
        if private_calibration_receipt is not None and private_calibration_summary is not None:
            validate_private_calibration_binding(
                calibration=calibration,
                private_receipt_path=private_calibration_receipt,
                private_summary_path=private_calibration_summary,
            )
        registry = strict_load_json(base / "contracts" / "source_neutral_mapping_contract_registry.v1.json")
        _exact_keys(
            registry,
            {
                "schema_version",
                "registry_version",
                "contracts",
                "checked_assets",
                "provider_calls_allowed",
                "product_writes_allowed",
            },
            "mapping_contract_registry_shape_invalid",
        )
        expected_paths = [f"contracts/{filename}" for filename in CONTRACT_SCHEMA_FILES]
        if (
            registry["schema_version"] != "x.source_neutral.mapping.contract_registry.v1"
            or registry["provider_calls_allowed"] is not False
            or registry["product_writes_allowed"] is not False
            or [row.get("path") for row in registry["contracts"]] != expected_paths
            or registry["checked_assets"]
            != [
                "configs/source_neutral_mapping_policy.v1.json",
                "fixtures/source_neutral_mapping_manifest_fixture_v1.json",
                "docs/live-evidence/2026-07-17-gdm-query-family-calibration.aggregate.v1.json",
            ]
        ):
            raise SourceNeutralMappingError("mapping_contract_registry_invalid")
        for path in expected_paths:
            strict_load_json(base / path)
    except Exception as exc:  # closed CLI boundary
        errors.append(f"source_neutral_mapping_assets_invalid:{type(exc).__name__}:{exc}")
    return errors


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate provider-free source-neutral mapping assets")
    parser.add_argument("--private-calibration-receipt", type=Path)
    parser.add_argument("--private-calibration-summary", type=Path)
    args = parser.parse_args()
    errors = validate_checked_in_assets(
        private_calibration_receipt=args.private_calibration_receipt,
        private_calibration_summary=args.private_calibration_summary,
    )
    print(json.dumps({"errors": errors, "status": "valid" if not errors else "invalid"}, sort_keys=True))
    return 0 if not errors else 1


if __name__ == "__main__":
    raise SystemExit(main())
