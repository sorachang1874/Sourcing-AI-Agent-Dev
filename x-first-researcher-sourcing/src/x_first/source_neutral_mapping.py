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
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from x_first.grok_operator_session_replay import (
    GrokOperatorSessionPrecommit,
    GrokOperatorSessionReplayError,
    replay_grok_operator_session,
    session_precommit_sha256,
)
from x_first.recall_pool_schema import assert_schema_valid

POLICY_SCHEMA_VERSION = "x.source_neutral.mapping.policy.v1"
MANIFEST_SCHEMA_VERSION = "x.source_neutral.mapping.candidate_manifest.v1"
PLAN_SCHEMA_VERSION = "x.source_neutral.mapping.wave_plan.v1"
PRECOMMIT_SCHEMA_VERSION = "x.source_neutral.mapping.session_precommit.v1"
TERMINAL_SCHEMA_VERSION = "x.source_neutral.mapping.flat_terminal.v1"
RECEIPT_SCHEMA_VERSION = "x.source_neutral.mapping.session_receipt.v1"
AGGREGATE_SCHEMA_VERSION = "x.source_neutral.mapping.candidate_free_aggregate.v1"
CALIBRATION_SCHEMA_VERSION = "x.source_neutral.mapping.calibration_aggregate.v1"

POLICY_SCHEMA_FILE = "x.source_neutral.mapping.policy.v1.schema.json"
MANIFEST_SCHEMA_FILE = "x.source_neutral.mapping.candidate_manifest.v1.schema.json"
PLAN_SCHEMA_FILE = "x.source_neutral.mapping.wave_plan.v1.schema.json"
RECEIPT_SCHEMA_FILE = "x.source_neutral.mapping.session_receipt.v1.schema.json"
AGGREGATE_SCHEMA_FILE = "x.source_neutral.mapping.candidate_free_aggregate.v1.schema.json"
CALIBRATION_SCHEMA_FILE = "x.source_neutral.mapping.calibration_aggregate.v1.schema.json"

CONTRACT_SCHEMA_FILES = (
    POLICY_SCHEMA_FILE,
    MANIFEST_SCHEMA_FILE,
    PLAN_SCHEMA_FILE,
    RECEIPT_SCHEMA_FILE,
    AGGREGATE_SCHEMA_FILE,
    CALIBRATION_SCHEMA_FILE,
)

_SHA_RE = re.compile(r"[0-9a-f]{64}")
_ID_RE = re.compile(r"[a-z0-9][a-z0-9_.-]{0,127}")
_HANDLE_RE = re.compile(r"[A-Za-z0-9_]{1,15}")
_STATUS_ID_RE = re.compile(r"[0-9]{1,32}")
_QUERY_TERM_RE = re.compile(r'(?:[A-Za-z0-9_-]+|"[A-Za-z0-9 -]+")')
_AXIS_STATES = frozenset({"current", "historical", "ambiguous", "unsupported"})
_PRIOR_EVIDENCE = frozenset(
    {"fixture_asserted", "source_bound", "model_mediated_unverified", "unsupported"}
)
QUEUE_KEYS = (
    "saturation_queue",
    "thread_hydration_queue",
    "luna_input_queue",
    "challenger_queue",
)


class SourceNeutralMappingError(ValueError):
    """Stable fail-closed error for the provider-free mapping controller."""


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
        if not isinstance(values, list) or not values or any(
            not isinstance(item, str) or not item.strip() or len(item) > 160 for item in values
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
        _QUERY_TERM_RE.fullmatch(term) is None
        for group in policy["concept_alias_groups"]
        for term in group["aliases"]
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
        "luna_qualified_state_upgrades",
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
    validate_wave_plan(payload, manifest=manifest, policy=policy)
    return payload


def validate_wave_plan(
    plan: Any,
    *,
    manifest: Mapping[str, Any],
    policy: Mapping[str, Any],
) -> None:
    if not isinstance(plan, Mapping):
        raise SourceNeutralMappingError("wave_plan_not_object")
    assert_schema_valid(plan, PLAN_SCHEMA_FILE)
    if plan.get("schema_version") != PLAN_SCHEMA_VERSION:
        raise SourceNeutralMappingError("wave_plan_version_invalid")
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


def build_session_precommit(
    *,
    plan: Mapping[str, Any],
    batch_index: int,
    grok_precommit: GrokOperatorSessionPrecommit,
) -> dict[str, Any]:
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

    try:
        _validate_session_precommit(mapping_precommit)
        validate_lab_descriptor(lab_descriptor)
        references, saturation = _parse_flat_terminal(
            terminal_text,
            calls=mapping_precommit["calls"],
            x_url_host=lab_descriptor["x_url_host"],
        )
    except (SourceNeutralMappingError, KeyError, TypeError):
        return _rejected_session_receipt(mapping_precommit, "flat_projection_invalid")
    if raw_session_files is None or grok_precommit is None:
        return _rejected_session_receipt(mapping_precommit, "operator_projected_unverified")
    try:
        if (
            session_precommit_sha256(grok_precommit)
            != mapping_precommit["grok_session_precommit_sha256"]
            or grok_precommit.expected_user_prompt_sha256
            != mapping_precommit["expected_user_prompt_sha256"]
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
        return _rejected_session_receipt(mapping_precommit, "raw_six_file_replay_invalid")
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


def _rejected_session_receipt(precommit: Mapping[str, Any], error: str) -> dict[str, Any]:
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
        "planned_native_x_call_count": 0,
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
    accepted_receipts: Sequence[Mapping[str, Any]],
    policy: Mapping[str, Any],
) -> dict[str, list[dict[str, Any]]]:
    validate_candidate_manifest(manifest)
    validate_policy(policy)
    references: dict[tuple[str, str], Mapping[str, Any]] = {}
    manifest_refs = {row["candidate_ref"] for row in manifest["candidates"]}
    stable_post_owners: dict[str, str] = {}
    saturation: list[dict[str, Any]] = []
    for receipt in accepted_receipts:
        try:
            assert_schema_valid(receipt, RECEIPT_SCHEMA_FILE)
        except Exception as exc:
            raise SourceNeutralMappingError("frontier_receipt_schema_invalid") from exc
        if (
            receipt.get("status") != "accepted"
            or receipt.get("commit_allowed") is not True
            or receipt.get("proof_authority") != "existing_raw_grok_six_file_replay"
            or receipt.get("errors") != []
            or receipt.get("source_artifact_count") != 6
            or receipt.get("planned_native_x_call_count")
            != receipt.get("attested_completed_native_x_call_count")
            or receipt.get("receipt_sha256") != _content_sha256(receipt, "receipt_sha256")
            or receipt.get("unique_stable_post_id_count") != len(receipt.get("references", []))
            or receipt.get("saturation_lower_bound_count")
            != len(receipt.get("saturation_queue_seeds", []))
        ):
            raise SourceNeutralMappingError("frontier_receipt_not_accepted")
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
        states = {
            candidate["lab_affiliation_prior"]["state"],
            candidate["pretraining_experience_prior"]["state"],
        }
        sparse = counts.get(candidate_ref, 0) <= policy["queues"]["authored_sparse_max_references"]
        if not sparse and not states & {"ambiguous", "unsupported"}:
            continue
        for challenger_type in policy["queues"]["challenger_types"]:
            challenger.append(
                {
                    "candidate_ref": candidate_ref,
                    "challenger_type": challenger_type,
                    "official_handles": list(descriptor["official_handles"]),
                    "project_aliases": list(descriptor["project_aliases"]),
                    "seed_stable_post_ids": [
                        stable_id for ref, stable_id in references if ref == candidate_ref
                    ],
                    "relationship": "official_or_third_party_non_self",
                    "self_evidence_allowed": False,
                }
            )
    return {
        "saturation_queue": saturation,
        "thread_hydration_queue": hydration,
        "luna_input_queue": [],
        "challenger_queue": challenger,
    }


def build_luna_input_queue(
    *,
    thread_hydration_queue: Sequence[Mapping[str, Any]],
    exact_hydrations: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    expected: dict[tuple[str, str], Mapping[str, Any]] = {}
    for row in thread_hydration_queue:
        queued = _exact_keys(
            row,
            {"candidate_ref", "stable_post_id", "url", "queue_reason"},
            "thread_hydration_queue_shape_invalid",
        )
        identity = (queued["candidate_ref"], queued["stable_post_id"])
        if identity in expected:
            raise SourceNeutralMappingError("thread_hydration_queue_identity_duplicate")
        expected[identity] = queued
    output: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for row in exact_hydrations:
        hydration = _exact_keys(
            row,
            {
                "candidate_ref",
                "stable_post_id",
                "source_url",
                "author_handle",
                "full_text",
                "full_text_sha256",
                "hydration_status",
            },
            "exact_hydration_shape_invalid",
        )
        identity = (hydration["candidate_ref"], hydration["stable_post_id"])
        if identity not in expected or identity in seen or hydration["hydration_status"] != "exact_source_bound":
            raise SourceNeutralMappingError("exact_hydration_binding_invalid")
        queued = expected[identity]
        source_url = hydration["source_url"]
        author_handle = _handle(hydration["author_handle"], "exact_hydration_author_handle_invalid")
        parsed = urlsplit(source_url) if isinstance(source_url, str) else None
        parts = parsed.path.split("/") if parsed is not None else []
        if (
            source_url != queued["url"]
            or parsed is None
            or parsed.scheme != "https"
            or not parsed.netloc
            or parsed.query
            or parsed.fragment
            or len(parts) != 4
            or parts[0] != ""
            or parts[2] != "status"
            or parts[1].casefold() != author_handle.casefold()
            or parts[3] != hydration["stable_post_id"]
        ):
            raise SourceNeutralMappingError("exact_hydration_source_url_binding_invalid")
        if not isinstance(hydration["full_text"], str) or not hydration["full_text"]:
            raise SourceNeutralMappingError("exact_hydration_full_text_invalid")
        if canonical_sha256(hydration["full_text"]) != hydration["full_text_sha256"]:
            raise SourceNeutralMappingError("exact_hydration_text_hash_mismatch")
        seen.add(identity)
        output.append(
            {
                "candidate_ref": hydration["candidate_ref"],
                "stable_post_id": hydration["stable_post_id"],
                "source_url": hydration["source_url"],
                "author_handle": hydration["author_handle"],
                "source_text": hydration["full_text"],
                "source_text_sha256": hydration["full_text_sha256"],
                "source_binding_status": "exact_source_bound",
                "review_purpose": "lab_and_pretraining_temporal_state_transition",
            }
        )
    return output


def structural_stop(
    *,
    queues: Mapping[str, Sequence[Any]],
    recent_waves: Sequence[Mapping[str, Any]],
    policy: Mapping[str, Any],
) -> dict[str, Any]:
    validate_policy(policy)
    queue_shape_valid = (
        isinstance(queues, Mapping)
        and set(queues) == set(QUEUE_KEYS)
        and all(isinstance(queues[key], list) for key in QUEUE_KEYS)
    )
    queues_empty = queue_shape_valid and all(not queues[key] for key in QUEUE_KEYS)
    required = policy["stop"]["consecutive_materially_distinct_zero_waves"]
    trailing = list(recent_waves[-required:])
    wave_keys = {
        "wave_id",
        "strategy_signature_sha256",
        "new_stable_post_id_count",
        "luna_qualified_state_upgrade_count",
    }
    valid_waves = len(trailing) == required and all(
        isinstance(row, Mapping)
        and set(row) == wave_keys
        and isinstance(row["wave_id"], str)
        and _ID_RE.fullmatch(row["wave_id"]) is not None
        and isinstance(row["strategy_signature_sha256"], str)
        and _SHA_RE.fullmatch(row["strategy_signature_sha256"]) is not None
        and type(row["new_stable_post_id_count"]) is int
        and row["new_stable_post_id_count"] == 0
        and type(row["luna_qualified_state_upgrade_count"]) is int
        and row["luna_qualified_state_upgrade_count"] == 0
        for row in trailing
    )
    distinct = valid_waves and len({row["strategy_signature_sha256"] for row in trailing}) == required
    stopped = queues_empty and bool(distinct)
    return {
        "stop": stopped,
        "queues_empty": queues_empty,
        "trailing_zero_wave_count": len(trailing) if valid_waves else 0,
        "materially_distinct": bool(distinct),
        "reason": "structural_convergence" if stopped else "continue_mapping",
    }


def _rate(numerator: int, denominator: int) -> float | None:
    return round(numerator / denominator, 6) if denominator else None


def build_candidate_free_aggregate(
    *,
    bindings: Mapping[str, str],
    planned_native_x_calls: int,
    attested_completed_native_x_calls: int,
    unique_stable_post_ids: int,
    exact_source_bound_hydrations: int,
    luna_terminal_reviews: int,
    luna_qualified_state_upgrades: int,
) -> dict[str, Any]:
    expected_bindings = {
        "policy_sha256",
        "candidate_manifest_sha256",
        "wave_plan_sha256",
        "session_receipt_manifest_sha256",
        "hydration_receipt_manifest_sha256",
        "luna_result_manifest_sha256",
    }
    if set(bindings) != expected_bindings:
        raise SourceNeutralMappingError("aggregate_binding_registry_invalid")
    for value in bindings.values():
        _sha(value, "aggregate_binding_hash_invalid")
    counts = (
        planned_native_x_calls,
        attested_completed_native_x_calls,
        unique_stable_post_ids,
        exact_source_bound_hydrations,
        luna_terminal_reviews,
        luna_qualified_state_upgrades,
    )
    if any(type(value) is not int or value < 0 for value in counts):
        raise SourceNeutralMappingError("aggregate_count_invalid")
    if (
        attested_completed_native_x_calls > planned_native_x_calls
        or exact_source_bound_hydrations > unique_stable_post_ids
        or luna_qualified_state_upgrades > luna_terminal_reviews
    ):
        raise SourceNeutralMappingError("aggregate_denominator_violation")
    payload: dict[str, Any] = {
        "schema_version": AGGREGATE_SCHEMA_VERSION,
        "privacy_class": "candidate_free_counts_and_hashes_only",
        "claim_status": "diagnostic_only",
        "bindings": dict(bindings),
        "metric_denominators": {
            "execution_compliance": planned_native_x_calls,
            "stable_post_id_retrieval": attested_completed_native_x_calls,
            "exact_hydration": unique_stable_post_ids,
            "luna_qualified_state_upgrades": luna_terminal_reviews,
        },
        "metric_numerators": {
            "execution_compliance": attested_completed_native_x_calls,
            "stable_post_id_retrieval": unique_stable_post_ids,
            "exact_hydration": exact_source_bound_hydrations,
            "luna_qualified_state_upgrades": luna_qualified_state_upgrades,
        },
        "metric_rates": {
            "execution_compliance": _rate(attested_completed_native_x_calls, planned_native_x_calls),
            "stable_post_id_retrieval": _rate(unique_stable_post_ids, attested_completed_native_x_calls),
            "exact_hydration": _rate(exact_source_bound_hydrations, unique_stable_post_ids),
            "luna_qualified_state_upgrades": _rate(luna_qualified_state_upgrades, luna_terminal_reviews),
        },
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
        or canonical_sha256(receipt["source_bindings"])
        != bindings["private_source_binding_manifest_sha256"]
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
