"""Luna candidate-bundle batch runner + Grok collection runner (offline-tested lane).

This module implements the approved design in
``docs/LUNA_LIVE_BATCH_RUNNER_DESIGN.md``: one Grok collection call (or more)
per candidate produces a per-candidate bundle (account resolution + reported X
Bio + candidate-authored Posts/Replies with retrieval receipts), and exactly
one Luna ``gpt-5.6-luna`` Responses call per candidate judges the FULL bundle
(LinkedIn seed professional facts + Bio + every collected Post/Reply) into the
two diagnostic axis states plus per-axis evidence citations.

Boundaries pinned by the design:

- Luna output stays ``diagnostic_only_unattested`` with
  ``model_claim_scope: state_proposal_only``; it never authorizes state
  transitions, ranking, eligibility, outreach, or canonical writes.
- ``judged_bundle_sha256`` is the canonical sha256 of the SORTED per-item
  content digests of the judged bundle manifest.  That definition is exactly
  the digest the unchanged ``luna_axis_reduction.v1`` reducer reproduces as
  ``reviewed_evidence_manifest_sha256`` when the deterministic adapter folds
  one judged evidence row per bundle item, so adapter output stays
  reducer-compatible without modifying the reducer.
- This lane ships no live provider path: every provider-costing call is gated
  behind an approval receipt and all transports are injected protocols.  Tests
  use offline fakes only; live execution is a later, separately gated lane.

Canonical-JSON discipline mirrors ``profile_bio_semantic_v2``: sorted keys,
compact separators, duplicate-key-rejecting strict decode.
"""

from __future__ import annotations

import hashlib
import json
import re
import subprocess
import threading
import time
import uuid
from collections.abc import Callable, Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol, runtime_checkable

from x_first import luna_live_canary as legacy
from x_first.profile_bio_semantic_v2 import canonical_json, canonical_sha256, load_json
from x_first.recall_pool_schema import assert_schema_valid, load_contract_schema
from x_first.source_neutral_mapping import (
    SourceNeutralMappingError,
    _build_luna_axis_reduction,
    freeze_candidate_manifest,
)

REVIEW_SCHEMA_VERSION = "x.source_neutral.mapping.luna_candidate_review.v1"
REVIEW_SCHEMA_FILE = "x.source_neutral.mapping.luna_candidate_review.v1.schema.json"
PROMPT_SCHEMA_VERSION = "x.source_neutral.mapping.luna_candidate_review.prompt.v1"
PROMPT_VERSION = "luna-candidate-review-prompt-v1"
PROMPT_PATH = "configs/luna_candidate_review_prompt.v1.json"
CANONICAL_PROMPT_SHA256 = "54bbac418f26a1141eb0266d960810ae06f8b1d873fcfbbf1f4199dfb3f305b0"

APPROVAL_RECEIPT_SCHEMA_VERSION = "x.source_neutral.mapping.luna_batch_runner.approval.v1"
EXECUTION_RECEIPT_SCHEMA_VERSION = "x.source_neutral.mapping.luna_batch_runner.execution_receipt.v1"
GROK_OPERATOR_RECEIPT_SCHEMA_VERSION = "x.source_neutral.mapping.luna_batch_runner.grok_operator_receipt.v1"
LUNA_BATCH_RESULT_SCHEMA_VERSION = "x.source_neutral.mapping.luna_batch_runner.result.v1"
GROK_COLLECTION_RESULT_SCHEMA_VERSION = "x.source_neutral.mapping.luna_batch_runner.grok_collection_result.v1"
PIPELINE_RESULT_SCHEMA_VERSION = "x.source_neutral.mapping.luna_batch_runner.pipeline_result.v1"

MODEL_ID = legacy.MODEL_ID
BASE_URL = legacy.BASE_URL
RESPONSES_URL = legacy.RESPONSES_URL
KEY_ENVIRONMENT_VARIABLE = legacy.KEY_ENVIRONMENT_VARIABLE
TOTAL_TIMEOUT_MS = legacy.TOTAL_TIMEOUT_MS
PROVIDER_ID = "chshapi_openai_compatible_relay"

DEFAULT_WORKER_COUNT = 16
MAX_WORKER_COUNT = 256
DEFAULT_REASONING_EFFORT = "high"
DEFAULT_MAX_OUTPUT_TOKENS = 4096
MAX_GROK_OUTPUT_BYTES = 4_000_000

_AXES = ("lab_affiliation", "pretraining_experience")
_AXIS_STATE_FIELD = {
    "lab_affiliation": "proposed_lab_affiliation_state",
    "pretraining_experience": "proposed_pretraining_experience_state",
}
_AXIS_CITATION_FIELD = {
    "lab_affiliation": "lab_affiliation_evidence_citations",
    "pretraining_experience": "pretraining_experience_evidence_citations",
}
_AXIS_STATES = frozenset({"current", "historical", "ambiguous", "unsupported"})
_SOURCE_STATUSES = frozenset(
    {"source_bound", "model_mediated_unverified", "human_supplied_unverified", "fixture_synthetic"}
)
_SEED_SOURCE_KINDS = frozenset({"x_account", "linkedin_profile", "professional_profile", "name_only"})
_FACT_TYPES = frozenset({"affiliation", "role", "education", "project", "location", "other"})
_FACT_TEMPORAL_STATES = frozenset({"current", "historical", "ambiguous", "not_applicable"})
_BUNDLE_ITEM_KINDS = frozenset({"post", "reply"})
_RESOLUTION_CONFIDENCES = frozenset({"high", "medium", "low", "not_found"})

_ID_RE = re.compile(r"[a-z0-9][a-z0-9_.-]{0,127}")
_SHA256_RE = re.compile(r"[0-9a-f]{64}")
_HANDLE_RE = re.compile(r"[A-Za-z0-9_]{1,15}")
_POST_ID_RE = re.compile(r"[1-9][0-9]{0,31}")
_EVIDENCE_REF_RE = re.compile(r"[A-Za-z0-9_.:/-]{1,512}")
_CANONICAL_TIME_RE = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z")
_SEED_KEYS = {
    "seed_ref",
    "source_kind",
    "external_record_ref",
    "source_record_sha256",
    "source_status",
    "source_profile_url",
    "name_text",
    "x_handle_proposals",
    "professional_facts",
}
_FACT_KEYS = {"fact_type", "value", "temporal_state", "evidence_ref"}
_BUNDLE_KEYS = {"candidate_ref", "seed_ref", "account_resolution", "x_bio", "items"}
_ACCOUNT_RESOLUTION_KEYS = {"handle", "resolution_confidence", "evidence_receipts"}
_BIO_KEYS = {"text"}
_BUNDLE_ITEM_KEYS = {
    "stable_post_id",
    "kind",
    "source_url",
    "author_handle",
    "authored_at",
    "text",
    "retrieval_receipt",
}
_RECEIPT_REF_KEYS = {"receipt_kind", "receipt_ref"}
_JUDGED_OUTPUT_KEYS = {
    "proposed_lab_affiliation_state",
    "proposed_pretraining_experience_state",
    "lab_affiliation_evidence_citations",
    "pretraining_experience_evidence_citations",
}
_APPROVAL_KEYS = {"schema_version", "approval_id", "approved_at", "candidate_refs_sha256"}
_PROMPT_KEYS = {"schema_version", "prompt_version", "model_id", "developer_instructions"}


class LunaBatchRunnerError(ValueError):
    """Raised when a Luna batch-runner contract, binding, or gate check fails."""


def project_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _is_int(value: Any) -> bool:
    return type(value) is int


def _timestamp(value: datetime) -> str:
    return value.astimezone(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _strict_json_loads(text: str) -> Any:
    def reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        value: dict[str, Any] = {}
        for key, item in pairs:
            if key in value:
                raise ValueError("duplicate_json_key")
            value[key] = item
        return value

    def reject_non_finite(token: str) -> Any:
        raise ValueError(f"non_finite_json_number:{token}")

    return json.loads(
        text,
        object_pairs_hook=reject_duplicate_keys,
        parse_constant=reject_non_finite,
    )


def _strict_decoder() -> json.JSONDecoder:
    def reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        value: dict[str, Any] = {}
        for key, item in pairs:
            if key in value:
                raise ValueError("duplicate_json_key")
            value[key] = item
        return value

    def reject_non_finite(token: str) -> Any:
        raise ValueError(f"non_finite_json_number:{token}")

    return json.JSONDecoder(object_pairs_hook=reject_duplicate_keys, parse_constant=reject_non_finite)


def _terminal_json_object(text: str) -> Any:
    """Extract one terminal JSON object from model narration (bounded repair).

    Agentic CLI models legitimately narrate before the final payload. Try each
    `{` from the end of the text backwards and accept the first candidate that
    decodes — under the duplicate-key/non-finite-rejecting strict decoder — and
    consumes the rest of the message except trailing whitespace. The candidate
    bundle validator remains the integrity gate; unbalanced or absent payloads
    still fail closed.
    """

    decoder = _strict_decoder()
    for start in range(len(text) - 1, -1, -1):
        if text[start] != "{":
            continue
        try:
            parsed, end = decoder.raw_decode(text, start)
        except (ValueError, RecursionError):
            continue
        if text[end:].strip():
            continue
        if isinstance(parsed, dict):
            return parsed
    raise ValueError("no_terminal_json_object")


def _scan_json(value: Any, *, max_depth: int = 64, max_nodes: int = 4096) -> list[str]:
    errors: list[str] = []
    stack: list[tuple[Any, int]] = [(value, 0)]
    nodes = 0
    while stack:
        current, depth = stack.pop()
        nodes += 1
        if nodes > max_nodes:
            errors.append("json_node_budget_exceeded")
            break
        if depth > max_depth:
            errors.append("json_depth_budget_exceeded")
            break
        if isinstance(current, dict):
            stack.extend((key, depth + 1) for key in current)
            stack.extend((item, depth + 1) for item in current.values())
        elif isinstance(current, list):
            stack.extend((item, depth + 1) for item in current)
    return errors


def _bounded_text(value: Any, *, minimum: int, maximum: int) -> bool:
    return isinstance(value, str) and minimum <= len(value) <= maximum


# ---------------------------------------------------------------------------
# Prompt asset (versioned, canonical-SHA pinned; mirrors validate_prompt).
# ---------------------------------------------------------------------------


def validate_prompt(prompt: Any) -> list[str]:
    errors: list[str] = []
    traversal = _scan_json(prompt)
    if traversal:
        return [f"$.validation: {error}" for error in traversal]
    if not isinstance(prompt, dict) or set(prompt) != _PROMPT_KEYS:
        return ["$: prompt keys must be exactly the closed prompt asset keys"]
    if prompt["schema_version"] != PROMPT_SCHEMA_VERSION:
        errors.append("$.schema_version: unsupported")
    if prompt["prompt_version"] != PROMPT_VERSION:
        errors.append("$.prompt_version: unsupported")
    if prompt["model_id"] != MODEL_ID:
        errors.append("$.model_id: must equal gpt-5.6-luna")
    if not _bounded_text(prompt["developer_instructions"], minimum=100, maximum=4000):
        errors.append("$.developer_instructions: must be a bounded non-empty string")
    try:
        digest = canonical_sha256(prompt)
    except (TypeError, ValueError, RecursionError):
        errors.append("$: must be canonical JSON")
    else:
        if digest != CANONICAL_PROMPT_SHA256:
            errors.append("$: must exactly match the pinned candidate-review prompt")
    return errors


def load_prompt() -> dict[str, Any]:
    prompt = load_json(project_root() / PROMPT_PATH)
    errors = validate_prompt(prompt)
    if errors:
        raise LunaBatchRunnerError(f"luna_candidate_review_prompt_invalid:{errors[0]}")
    return prompt


# ---------------------------------------------------------------------------
# Seed + bundle data model.
# ---------------------------------------------------------------------------


def _validate_fact(fact: Any) -> None:
    if not isinstance(fact, dict) or set(fact) != _FACT_KEYS:
        raise LunaBatchRunnerError("seed_fact_shape_invalid")
    if fact["fact_type"] not in _FACT_TYPES:
        raise LunaBatchRunnerError("seed_fact_type_invalid")
    if not _bounded_text(fact["value"], minimum=1, maximum=1000):
        raise LunaBatchRunnerError("seed_fact_value_invalid")
    if fact["temporal_state"] not in _FACT_TEMPORAL_STATES:
        raise LunaBatchRunnerError("seed_fact_temporal_state_invalid")
    if not isinstance(fact["evidence_ref"], str) or _EVIDENCE_REF_RE.fullmatch(fact["evidence_ref"]) is None:
        raise LunaBatchRunnerError("seed_fact_evidence_ref_invalid")


def validate_seed_input(seed: Any) -> dict[str, Any]:
    """Validate the portable-campaign ``seed_inputs`` subset this runner judges.

    The owning contract stays ``x.portable.research_campaign.request.v1``
    ``$defs.seed_input``; this is the structural mirror of the fields the
    identity-anchored prompts and judged-bundle manifest actually consume.
    """

    if not isinstance(seed, dict) or not set(seed) <= _SEED_KEYS:
        raise LunaBatchRunnerError("seed_input_shape_invalid")
    for key in ("seed_ref", "source_kind", "source_status", "name_text", "professional_facts"):
        if key not in seed:
            raise LunaBatchRunnerError("seed_input_shape_invalid")
    if not isinstance(seed["seed_ref"], str) or _ID_RE.fullmatch(seed["seed_ref"]) is None:
        raise LunaBatchRunnerError("seed_ref_invalid")
    if seed["source_kind"] not in _SEED_SOURCE_KINDS:
        raise LunaBatchRunnerError("seed_source_kind_invalid")
    if seed["source_status"] not in _SOURCE_STATUSES:
        raise LunaBatchRunnerError("seed_source_status_invalid")
    if not _bounded_text(seed["name_text"], minimum=1, maximum=200):
        raise LunaBatchRunnerError("seed_name_text_invalid")
    facts = seed["professional_facts"]
    if not isinstance(facts, list):
        raise LunaBatchRunnerError("seed_professional_facts_invalid")
    evidence_refs: set[str] = set()
    for fact in facts:
        _validate_fact(fact)
        if fact["evidence_ref"] in evidence_refs:
            raise LunaBatchRunnerError("seed_fact_evidence_ref_duplicate")
        evidence_refs.add(fact["evidence_ref"])
    return json.loads(canonical_json(seed))


def _validate_receipt_ref(receipt: Any, *, error: str) -> None:
    if not isinstance(receipt, dict) or set(receipt) != _RECEIPT_REF_KEYS:
        raise LunaBatchRunnerError(error)
    if not _bounded_text(receipt["receipt_kind"], minimum=1, maximum=64):
        raise LunaBatchRunnerError(error)
    if not _bounded_text(receipt["receipt_ref"], minimum=1, maximum=512):
        raise LunaBatchRunnerError(error)


def validate_candidate_bundle(bundle: Any, *, seed: Mapping[str, Any]) -> dict[str, Any]:
    """Validate one Grok-collected per-candidate bundle against the §3.0 shape."""

    if not isinstance(bundle, dict) or set(bundle) != _BUNDLE_KEYS:
        raise LunaBatchRunnerError("candidate_bundle_shape_invalid")
    if bundle["candidate_ref"] != seed["seed_ref"] or bundle["seed_ref"] != seed["seed_ref"]:
        raise LunaBatchRunnerError("candidate_bundle_seed_mismatch")
    resolution = bundle["account_resolution"]
    if not isinstance(resolution, dict) or set(resolution) != _ACCOUNT_RESOLUTION_KEYS:
        raise LunaBatchRunnerError("account_resolution_shape_invalid")
    if not isinstance(resolution["handle"], str):
        raise LunaBatchRunnerError("account_resolution_handle_invalid")
    if resolution["resolution_confidence"] not in _RESOLUTION_CONFIDENCES:
        raise LunaBatchRunnerError("account_resolution_confidence_invalid")
    if not isinstance(resolution["evidence_receipts"], list):
        raise LunaBatchRunnerError("account_resolution_receipts_invalid")
    for receipt in resolution["evidence_receipts"]:
        _validate_receipt_ref(receipt, error="account_resolution_receipt_invalid")
    if resolution["resolution_confidence"] == "not_found":
        # Explicit negative resolution: the collector proved no plausible account;
        # the handle stays empty and the negative-search receipts carry the proof.
        if resolution["handle"] != "" or not resolution["evidence_receipts"]:
            raise LunaBatchRunnerError("account_resolution_not_found_invalid")
    elif _HANDLE_RE.fullmatch(resolution["handle"]) is None:
        raise LunaBatchRunnerError("account_resolution_handle_invalid")
    bio = bundle["x_bio"]
    if not isinstance(bio, dict) or set(bio) != _BIO_KEYS:
        raise LunaBatchRunnerError("x_bio_shape_invalid")
    if not _bounded_text(bio["text"], minimum=0, maximum=4000):
        raise LunaBatchRunnerError("x_bio_text_invalid")
    items = bundle["items"]
    if not isinstance(items, list):
        raise LunaBatchRunnerError("bundle_items_invalid")
    post_ids: set[str] = set()
    for item in items:
        if not isinstance(item, dict) or set(item) != _BUNDLE_ITEM_KEYS:
            raise LunaBatchRunnerError("bundle_item_shape_invalid")
        if not isinstance(item["stable_post_id"], str) or _POST_ID_RE.fullmatch(item["stable_post_id"]) is None:
            raise LunaBatchRunnerError("bundle_item_post_id_invalid")
        if item["stable_post_id"] in post_ids:
            raise LunaBatchRunnerError("bundle_item_post_id_duplicate")
        post_ids.add(item["stable_post_id"])
        if item["kind"] not in _BUNDLE_ITEM_KINDS:
            raise LunaBatchRunnerError("bundle_item_kind_invalid")
        source_url = item["source_url"]
        if not _bounded_text(source_url, minimum=1, maximum=512) or not source_url.startswith("https://"):
            raise LunaBatchRunnerError("bundle_item_source_url_invalid")
        if not isinstance(item["author_handle"], str) or _HANDLE_RE.fullmatch(item["author_handle"]) is None:
            raise LunaBatchRunnerError("bundle_item_author_handle_invalid")
        if not isinstance(item["authored_at"], str) or len(item["authored_at"]) > 40:
            raise LunaBatchRunnerError("bundle_item_authored_at_invalid")
        if not _bounded_text(item["text"], minimum=1, maximum=8000):
            raise LunaBatchRunnerError("bundle_item_text_invalid")
        _validate_receipt_ref(item["retrieval_receipt"], error="bundle_item_retrieval_receipt_invalid")
    return json.loads(canonical_json(bundle))


# ---------------------------------------------------------------------------
# Judged-bundle manifest + digest.
# ---------------------------------------------------------------------------


def build_judged_bundle_manifest(
    bundle: Mapping[str, Any],
    *,
    seed: Mapping[str, Any],
) -> tuple[list[dict[str, Any]], str]:
    """Derive the ordered judged-item manifest and the reducer-compatible digest.

    Per-item ``sha256`` is the canonical sha256 of that item's full judged
    content (ref included).  ``judged_bundle_sha256`` is the canonical sha256
    of the SORTED per-item digests, matching the digest the unchanged
    ``luna_axis_reduction.v1`` reducer recomputes as
    ``reviewed_evidence_manifest_sha256``.
    """

    items: list[dict[str, Any]] = []
    item_refs: set[str] = set()

    def append(item_ref: str, source_kind: str, source_status: str, content: Mapping[str, Any]) -> None:
        if item_ref in item_refs:
            raise LunaBatchRunnerError("judged_item_ref_duplicate")
        item_refs.add(item_ref)
        items.append(
            {
                "item_ref": item_ref,
                "source_kind": source_kind,
                "source_status": source_status,
                "sha256": canonical_sha256(content),
            }
        )

    for fact in seed["professional_facts"]:
        item_ref = f"seed_fact:{fact['evidence_ref']}"
        append(
            item_ref,
            "seed_fact",
            seed["source_status"],
            {
                "item_ref": item_ref,
                "fact_type": fact["fact_type"],
                "value": fact["value"],
                "temporal_state": fact["temporal_state"],
                "evidence_ref": fact["evidence_ref"],
            },
        )
    append(
        "x_bio",
        "x_bio",
        "model_mediated_unverified",
        {"item_ref": "x_bio", "text": bundle["x_bio"]["text"]},
    )
    for post in bundle["items"]:
        item_ref = f"post:{post['stable_post_id']}"
        append(
            item_ref,
            "x_post" if post["kind"] == "post" else "x_reply",
            "model_mediated_unverified",
            {
                "item_ref": item_ref,
                "kind": post["kind"],
                "stable_post_id": post["stable_post_id"],
                "text": post["text"],
            },
        )
    judged_bundle_sha256 = canonical_sha256(sorted(item["sha256"] for item in items))
    return items, judged_bundle_sha256


# ---------------------------------------------------------------------------
# Worker pool (aggressive, ordinal-restored, failure-isolating).
# ---------------------------------------------------------------------------


def _check_worker_count(worker_count: Any) -> int:
    if not _is_int(worker_count) or not 1 <= worker_count <= MAX_WORKER_COUNT:
        raise LunaBatchRunnerError("worker_count_invalid")
    return worker_count


def _seed_refs(seeds: Sequence[Mapping[str, Any]]) -> list[str]:
    """Extract the batch-level candidate identity set (light check only).

    Full seed validation happens per candidate inside the pool so a malformed
    seed degrades to a failed row instead of aborting the batch.
    """

    if not isinstance(seeds, Sequence) or isinstance(seeds, (str, bytes)) or not seeds:
        raise LunaBatchRunnerError("seed_queue_empty")
    refs: list[str] = []
    for seed in seeds:
        if not isinstance(seed, Mapping) or not isinstance(seed.get("seed_ref"), str):
            raise LunaBatchRunnerError("seed_input_shape_invalid")
        refs.append(seed["seed_ref"])
    if len(refs) != len(set(refs)):
        raise LunaBatchRunnerError("seed_ref_duplicate")
    return refs


def _run_indexed_pool(
    ordinals: list[int],
    fn: Callable[[int], dict[str, Any]],
    *,
    worker_count: int,
) -> list[dict[str, Any]]:
    """Run ``fn`` per ordinal concurrently and restore seed-ordinal order.

    ``fn`` must isolate its own failures into the returned per-candidate row;
    an exception escaping one worker never aborts sibling rows.
    """

    results: list[dict[str, Any] | None] = [None] * len(ordinals)
    with ThreadPoolExecutor(max_workers=worker_count) as pool:
        futures = {pool.submit(fn, ordinal): ordinal for ordinal in ordinals}
        for future in as_completed(futures):
            ordinal = futures[future]
            results[ordinal] = future.result()
    return [row for row in results if row is not None]


# ---------------------------------------------------------------------------
# Grok collection runner.
# ---------------------------------------------------------------------------


def _identity_context(seed: Mapping[str, Any]) -> dict[str, Any]:
    facts = seed["professional_facts"]
    return {
        "name_text": seed["name_text"],
        "current_labs": [
            fact["value"]
            for fact in facts
            if fact["fact_type"] == "affiliation" and fact["temporal_state"] == "current"
        ],
        "past_labs": [
            fact["value"]
            for fact in facts
            if fact["fact_type"] == "affiliation" and fact["temporal_state"] == "historical"
        ],
    }


def build_grok_identity_prompt(
    seed: Mapping[str, Any],
    *,
    target_direction: str = "pre-training",
) -> str:
    """Build the identity-anchored per-candidate collection prompt (§3.0/§3.1).

    The prompt MUST anchor identity on the seed's name + current lab + past
    labs so the resolved account is the same person, not a namesake.
    """

    context = _identity_context(seed)
    facts_lines = [
        f"- {fact['fact_type']} ({fact['temporal_state']}): {fact['value']}"
        for fact in seed["professional_facts"]
    ]
    facts_block = "\n".join(facts_lines) if facts_lines else "- (no professional facts supplied)"
    current_labs = ", ".join(context["current_labs"]) or "(none supplied)"
    past_labs = ", ".join(context["past_labs"]) or "(none supplied)"
    return (
        "Resolve the public X account of exactly this person and collect their "
        "candidate-authored public Posts/Replies relevant to the target direction "
        f"({target_direction}).\n"
        f"Identity anchor (the account MUST plausibly be this same person, not a namesake):\n"
        f"- candidate_ref: {seed['seed_ref']}\n"
        f"- name: {context['name_text']}\n"
        f"- current lab(s): {current_labs}\n"
        f"- past lab(s): {past_labs}\n"
        f"LinkedIn professional facts (identity context):\n{facts_block}\n"
        "Return strict JSON with keys candidate_ref, seed_ref, account_resolution "
        "(handle, resolution_confidence, evidence_receipts), x_bio, and items[]. "
        "When no plausible account exists after a real search, return "
        "resolution_confidence `not_found` with an empty handle and "
        "evidence_receipts documenting the negative searches you ran; never invent "
        "or guess a handle. "
        "x_bio MUST be an object with exactly one key: text (the exact reported bio "
        "text, or an empty string when none). Each items[] entry MUST have exactly "
        "the keys stable_post_id (the numeric X post id as a string), kind (post or "
        "reply), source_url (https URL of the post), author_handle, authored_at "
        "(ISO-8601), text, and retrieval_receipt. candidate_ref and "
        f"seed_ref MUST both be exactly `{seed['seed_ref']}` (copy it verbatim). "
        "Every receipt — each "
        "entry of evidence_receipts and each item's retrieval_receipt — MUST be an "
        "object with exactly two string keys: receipt_kind (the retrieval method, at "
        "most 64 characters) and receipt_ref (the query text, URL, or id it was "
        "retrieved with, at most 512 characters). Use only public X "
        "data; do not infer ethnicity, nationality, or another protected identity; "
        "do not rank, decide eligibility, or authorize outreach. Your entire response "
        "must be exactly one strict JSON object — no prose, narration, or markdown "
        "fences."
    )


def build_grok_argv(
    prompt: str,
    *,
    grok_binary: str = "grok",
    extra_args: Sequence[str] = (),
) -> list[str]:
    """Headless CLI invocation shape (the only mode that works in agent shells)."""

    if not _bounded_text(grok_binary, minimum=1, maximum=160):
        raise LunaBatchRunnerError("grok_binary_invalid")
    return [grok_binary, "-p", prompt, "--output-format", "json", *list(extra_args)]


@runtime_checkable
class GrokTransport(Protocol):
    """One headless Grok CLI call; returns the parsed headless envelope."""

    def run(
        self,
        *,
        argv: Sequence[str],
        prompt: str,
        session_id: str,
        timeout_ms: int,
    ) -> Mapping[str, Any]: ...


class SubprocessGrokTransport:
    """Headless `grok -p ... --output-format json` subprocess transport.

    Never constructed by tests in this lane; the live collection wave is a
    later, separately gated lane.
    """

    def __init__(self, *, max_output_bytes: int = MAX_GROK_OUTPUT_BYTES) -> None:
        self._max_output_bytes = max_output_bytes

    def run(
        self,
        *,
        argv: Sequence[str],
        prompt: str,
        session_id: str,
        timeout_ms: int,
    ) -> Mapping[str, Any]:
        del prompt, session_id
        completed = subprocess.run(
            list(argv),
            capture_output=True,
            timeout=max(1, timeout_ms) / 1000,
            check=False,
        )
        if completed.returncode != 0:
            raise LunaBatchRunnerError("grok_cli_exit_nonzero")
        if len(completed.stdout) > self._max_output_bytes:
            raise LunaBatchRunnerError("grok_cli_output_budget_exceeded")
        try:
            envelope = _strict_json_loads(completed.stdout.decode("utf-8"))
        except (UnicodeError, ValueError, RecursionError) as exc:
            raise LunaBatchRunnerError("grok_cli_envelope_invalid") from exc
        if not isinstance(envelope, dict):
            raise LunaBatchRunnerError("grok_cli_envelope_invalid")
        return envelope


class OfflineFakeGrokTransport:
    """Scripted offline Grok transport for tests; performs no subprocess or network I/O."""

    def __init__(
        self,
        *,
        bundles: Mapping[str, Mapping[str, Any]],
        cost_usd: float = 0.0,
        latency_s: Mapping[str, float] | None = None,
        failures: Mapping[str, str] | None = None,
    ) -> None:
        self._bundles = {key: json.loads(canonical_json(value)) for key, value in bundles.items()}
        self._cost_usd = cost_usd
        self._latency_s = dict(latency_s or {})
        self._failures = dict(failures or {})
        self.calls: list[dict[str, Any]] = []
        self._lock = threading.Lock()

    def run(
        self,
        *,
        argv: Sequence[str],
        prompt: str,
        session_id: str,
        timeout_ms: int,
    ) -> Mapping[str, Any]:
        with self._lock:
            self.calls.append(
                {"argv": list(argv), "prompt": prompt, "session_id": session_id, "timeout_ms": timeout_ms}
            )
        matches = [ref for ref in self._bundles if f"candidate_ref: {ref}" in prompt]
        if len(matches) != 1:
            raise LunaBatchRunnerError("grok_fake_candidate_unresolved")
        candidate_ref = matches[0]
        if candidate_ref in self._failures:
            raise LunaBatchRunnerError(self._failures[candidate_ref])
        latency = self._latency_s.get(candidate_ref, 0.0)
        if latency > 0:
            time.sleep(latency)
        return {
            "sessionId": session_id,
            "stopReason": "EndTurn",
            "text": canonical_json(self._bundles[candidate_ref]),
            "usage": {"input_tokens": 1, "output_tokens": 1, "total_tokens": 2},
            "total_cost_usd": self._cost_usd,
        }


def _bundle_from_envelope(envelope: Any, *, session_id: str) -> dict[str, Any]:
    if not isinstance(envelope, dict):
        raise LunaBatchRunnerError("grok_envelope_invalid")
    if envelope.get("sessionId") != session_id:
        raise LunaBatchRunnerError("grok_envelope_session_mismatch")
    if envelope.get("stopReason") != "EndTurn":
        raise LunaBatchRunnerError("grok_envelope_not_terminal")
    structured = envelope.get("structuredOutput")
    if structured is not None:
        if not isinstance(structured, dict):
            raise LunaBatchRunnerError("grok_envelope_structured_output_invalid")
        return json.loads(canonical_json(structured))
    text = envelope.get("text")
    if not isinstance(text, str) or not text.strip():
        raise LunaBatchRunnerError("grok_envelope_text_invalid")
    try:
        parsed = _strict_json_loads(text)
    except (ValueError, RecursionError):
        try:
            parsed = _terminal_json_object(text)
        except (ValueError, RecursionError) as exc:
            raise LunaBatchRunnerError("grok_envelope_text_invalid") from exc
    if not isinstance(parsed, dict):
        raise LunaBatchRunnerError("grok_envelope_text_invalid")
    return parsed


def collect_candidate_bundle(
    seed: Mapping[str, Any],
    *,
    transport: GrokTransport,
    session_id: str | None = None,
    timeout_ms: int = TOTAL_TIMEOUT_MS,
    grok_binary: str = "grok",
    target_direction: str = "pre-training",
    wall_clock: Callable[[], datetime] | None = None,
    monotonic: Callable[[], float] | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Run one identity-anchored Grok collection call and validate the bundle."""

    checked_seed = validate_seed_input(seed)
    now = wall_clock or (lambda: datetime.now(UTC))
    clock = monotonic or time.monotonic
    session = session_id or f"grok-collect-{uuid.uuid4().hex}"
    prompt = build_grok_identity_prompt(checked_seed, target_direction=target_direction)
    argv = build_grok_argv(prompt, grok_binary=grok_binary)
    started_at = _timestamp(now())
    started = clock()
    envelope = transport.run(argv=argv, prompt=prompt, session_id=session, timeout_ms=timeout_ms)
    # Real CLI provenance: a live headless CLI mints its own session id; when the
    # envelope carries one, adopt it as the receipt session instead of the
    # caller-generated placeholder (prompt/argv hashes already bind the call).
    envelope_session = envelope.get("sessionId") if isinstance(envelope, dict) else None
    if isinstance(envelope_session, str) and envelope_session.strip():
        session = envelope_session
    bundle = _bundle_from_envelope(envelope, session_id=session)
    # Caller-owned echo fields: the runner issued exactly one collection call for
    # this seed, so the returned bundle necessarily belongs to it. Normalize the
    # model's transcription of the caller-owned refs instead of trusting it.
    bundle["candidate_ref"] = checked_seed["seed_ref"]
    bundle["seed_ref"] = checked_seed["seed_ref"]
    checked_bundle = validate_candidate_bundle(bundle, seed=checked_seed)
    elapsed_ms = max(0, int((clock() - started) * 1000))
    cost = envelope.get("total_cost_usd")
    receipt = {
        "schema_version": GROK_OPERATOR_RECEIPT_SCHEMA_VERSION,
        "candidate_ref": checked_seed["seed_ref"],
        "session_id": session,
        "prompt_sha256": hashlib.sha256(prompt.encode("utf-8")).hexdigest(),
        "argv_sha256": canonical_sha256(list(argv)),
        "timeout_ms": timeout_ms,
        "started_at": started_at,
        "completed_at": _timestamp(now()),
        "elapsed_ms": elapsed_ms,
        "cost_usd": cost if type(cost) in {int, float} else None,
        "outcome": "completed",
        "error_code": None,
    }
    return checked_bundle, receipt


def run_grok_collection(
    *,
    seeds: Sequence[Mapping[str, Any]],
    transport: GrokTransport,
    worker_count: int = DEFAULT_WORKER_COUNT,
    timeout_ms: int = TOTAL_TIMEOUT_MS,
    grok_binary: str = "grok",
    target_direction: str = "pre-training",
    wall_clock: Callable[[], datetime] | None = None,
    monotonic: Callable[[], float] | None = None,
) -> dict[str, Any]:
    """Collect per-candidate bundles over the seed queue (§3.1).

    Aggressive pool (default 16 workers, no provider-side cap assumed);
    results are re-ordered by seed ordinal and one candidate's failure
    produces a failed receipt, never a batch abort.
    """

    workers = _check_worker_count(worker_count)
    seed_refs = _seed_refs(seeds)

    def work(ordinal: int) -> dict[str, Any]:
        row: dict[str, Any] = {"candidate_ref": seed_refs[ordinal], "seed_ordinal": ordinal}
        try:
            seed = validate_seed_input(seeds[ordinal])
            bundle, receipt = collect_candidate_bundle(
                seed,
                transport=transport,
                timeout_ms=timeout_ms,
                grok_binary=grok_binary,
                target_direction=target_direction,
                wall_clock=wall_clock,
                monotonic=monotonic,
            )
        except Exception as exc:  # noqa: BLE001 - failure isolation: one candidate never aborts the batch
            code = exc.args[0] if exc.args and isinstance(exc.args[0], str) else "grok_collection_failed"
            row.update(
                {
                    "status": "failed",
                    "error_code": code,
                    "bundle": None,
                    "operator_receipt": {
                        "schema_version": GROK_OPERATOR_RECEIPT_SCHEMA_VERSION,
                        "candidate_ref": seed_refs[ordinal],
                        "session_id": None,
                        "prompt_sha256": None,
                        "argv_sha256": None,
                        "timeout_ms": timeout_ms,
                        "started_at": None,
                        "completed_at": None,
                        "elapsed_ms": None,
                        "cost_usd": None,
                        "outcome": "failed",
                        "error_code": code,
                    },
                }
            )
        else:
            row.update({"status": "completed", "error_code": None, "bundle": bundle, "operator_receipt": receipt})
        return row

    results = _run_indexed_pool(list(range(len(seed_refs))), work, worker_count=workers)
    completed = sum(1 for row in results if row["status"] == "completed")
    return {
        "schema_version": GROK_COLLECTION_RESULT_SCHEMA_VERSION,
        "worker_count": workers,
        "candidate_count": len(results),
        "completed_count": completed,
        "failed_count": len(results) - completed,
        "results": results,
    }


# ---------------------------------------------------------------------------
# Luna candidate review: request building, output validation, binding.
# ---------------------------------------------------------------------------


def _resolve_local_refs(node: Any, *, root: Mapping[str, Any]) -> Any:
    if isinstance(node, dict):
        reference = node.get("$ref")
        if isinstance(reference, str) and reference.startswith("#/$defs/"):
            target = root["$defs"][reference.removeprefix("#/$defs/")]
            return _resolve_local_refs(target, root=root)
        return {key: _resolve_local_refs(value, root=root) for key, value in node.items()}
    if isinstance(node, list):
        return [_resolve_local_refs(item, root=root) for item in node]
    return node


def judged_output_schema() -> dict[str, Any]:
    """Derive the closed strict-output schema from the review contract (§3.3).

    The review contract stays the single source of truth; the Responses strict
    schema is its judged-field subset with local ``$ref``s inlined.
    """

    contract = load_contract_schema(REVIEW_SCHEMA_FILE)
    properties = {
        key: _resolve_local_refs(contract["properties"][key], root=contract) for key in sorted(_JUDGED_OUTPUT_KEYS)
    }
    return {
        "type": "object",
        "additionalProperties": False,
        "required": sorted(_JUDGED_OUTPUT_KEYS),
        "properties": properties,
    }


def build_luna_source_payload(
    seed: Mapping[str, Any],
    bundle: Mapping[str, Any],
) -> dict[str, Any]:
    """Assemble the judged input: identity context + manifest + FULL bundle."""

    manifest_items, judged_bundle_sha256 = build_judged_bundle_manifest(bundle, seed=seed)
    return {
        "task": "luna_candidate_review",
        "candidate_ref": seed["seed_ref"],
        "identity_context": _identity_context(seed),
        "judged_bundle_manifest": manifest_items,
        "judged_bundle_sha256": judged_bundle_sha256,
        "bundle": {
            "seed_facts": list(seed["professional_facts"]),
            "account_resolution": dict(bundle["account_resolution"]),
            "x_bio": dict(bundle["x_bio"]),
            "items": list(bundle["items"]),
        },
    }


def build_luna_responses_payload(
    seed: Mapping[str, Any],
    bundle: Mapping[str, Any],
    *,
    prompt: Mapping[str, Any],
    reasoning_effort: str = DEFAULT_REASONING_EFFORT,
    max_output_tokens: int = DEFAULT_MAX_OUTPUT_TOKENS,
) -> dict[str, Any]:
    """Build the chshapi-relay Responses payload (canary transport shape)."""

    checked_seed = validate_seed_input(seed)
    checked_bundle = validate_candidate_bundle(bundle, seed=checked_seed)
    source_payload = build_luna_source_payload(checked_seed, checked_bundle)
    return {
        "model": MODEL_ID,
        "reasoning": {"effort": reasoning_effort},
        "instructions": prompt["developer_instructions"],
        "input": [
            {
                "role": "user",
                "content": [{"type": "input_text", "text": canonical_json(source_payload)}],
            }
        ],
        "text": {
            "format": {
                "type": "json_schema",
                "name": "x_source_neutral_mapping_luna_candidate_review_judged_v1",
                "strict": True,
                "schema": judged_output_schema(),
            }
        },
        "tools": [],
        "max_output_tokens": max_output_tokens,
        "truncation": "disabled",
        "store": False,
        "metadata": {
            "candidate_ref": checked_seed["seed_ref"],
            "judged_bundle_sha256": source_payload["judged_bundle_sha256"],
            "prompt_sha256": CANONICAL_PROMPT_SHA256,
        },
    }


def validate_judged_model_output(output: Any, *, manifest_items: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    """Validate the judged fields; every citation must name a judged bundle item."""

    if not isinstance(output, dict) or set(output) != _JUDGED_OUTPUT_KEYS:
        raise LunaBatchRunnerError("luna_output_shape_invalid")
    for axis in _AXES:
        if output[_AXIS_STATE_FIELD[axis]] not in _AXIS_STATES:
            raise LunaBatchRunnerError("luna_output_state_invalid")
    judged_refs = {item["item_ref"] for item in manifest_items}
    for axis in _AXES:
        citations = output[_AXIS_CITATION_FIELD[axis]]
        if not isinstance(citations, list) or any(not isinstance(citation, str) for citation in citations):
            raise LunaBatchRunnerError("luna_output_citations_invalid")
        if len(citations) != len(set(citations)):
            raise LunaBatchRunnerError("luna_output_citation_duplicate")
        for citation in citations:
            if citation not in judged_refs:
                raise LunaBatchRunnerError("luna_output_citation_not_judged")
    return json.loads(canonical_json(output))


def extract_judged_output(response_body: Any) -> tuple[dict[str, Any], str | None]:
    """Extract the judged fields from a Responses body; exact returned-model check."""

    if not isinstance(response_body, dict) or response_body.get("status") != "completed":
        raise LunaBatchRunnerError("luna_response_invalid")
    returned_model = response_body.get("model")
    if returned_model != MODEL_ID:
        raise LunaBatchRunnerError("luna_response_model_mismatch")
    output_items = response_body.get("output")
    if not isinstance(output_items, list):
        raise LunaBatchRunnerError("luna_response_invalid")
    messages = [item for item in output_items if isinstance(item, dict) and item.get("type") == "message"]
    if len(messages) != 1:
        raise LunaBatchRunnerError("luna_response_invalid")
    content = messages[0].get("content")
    if not isinstance(content, list) or len(content) != 1 or not isinstance(content[0], dict):
        raise LunaBatchRunnerError("luna_response_invalid")
    output_text = content[0]
    if output_text.get("type") != "output_text" or not isinstance(output_text.get("text"), str):
        raise LunaBatchRunnerError("luna_response_invalid")
    try:
        parsed = _strict_json_loads(output_text["text"])
    except (ValueError, RecursionError) as exc:
        raise LunaBatchRunnerError("luna_output_json_invalid") from exc
    if not isinstance(parsed, dict):
        raise LunaBatchRunnerError("luna_output_json_invalid")
    return parsed, returned_model if isinstance(returned_model, str) else None


def build_candidate_review(
    seed: Mapping[str, Any],
    bundle: Mapping[str, Any],
    model_output: Mapping[str, Any],
) -> dict[str, Any]:
    """Bind validated judged fields to the recomputed manifest + digest.

    The manifest and ``judged_bundle_sha256`` are always recomputed from the
    supplied bundle, never taken from model output; citations are closed
    against the judged item refs.
    """

    checked_seed = validate_seed_input(seed)
    checked_bundle = validate_candidate_bundle(bundle, seed=checked_seed)
    manifest_items, judged_bundle_sha256 = build_judged_bundle_manifest(checked_bundle, seed=checked_seed)
    checked_output = validate_judged_model_output(model_output, manifest_items=manifest_items)
    review = {
        "schema_version": REVIEW_SCHEMA_VERSION,
        "candidate_ref": checked_seed["seed_ref"],
        "judged_bundle_manifest": manifest_items,
        "judged_bundle_sha256": judged_bundle_sha256,
        "proposed_lab_affiliation_state": checked_output["proposed_lab_affiliation_state"],
        "proposed_pretraining_experience_state": checked_output["proposed_pretraining_experience_state"],
        "lab_affiliation_evidence_citations": list(checked_output["lab_affiliation_evidence_citations"]),
        "pretraining_experience_evidence_citations": list(
            checked_output["pretraining_experience_evidence_citations"]
        ),
        "authority_status": "diagnostic_only_unattested",
        "model_claim_scope": "state_proposal_only",
    }
    try:
        assert_schema_valid(review, REVIEW_SCHEMA_FILE)
    except Exception as exc:
        raise LunaBatchRunnerError("luna_candidate_review_schema_invalid") from exc
    return review


def validate_candidate_review_binding(
    review: Mapping[str, Any],
    *,
    seed: Mapping[str, Any],
    bundle: Mapping[str, Any],
) -> None:
    """Re-derive the manifest + digest from the bundle and require exact binding."""

    try:
        assert_schema_valid(review, REVIEW_SCHEMA_FILE)
    except Exception as exc:
        raise LunaBatchRunnerError("luna_candidate_review_schema_invalid") from exc
    checked_seed = validate_seed_input(seed)
    checked_bundle = validate_candidate_bundle(bundle, seed=checked_seed)
    if review["candidate_ref"] != checked_seed["seed_ref"]:
        raise LunaBatchRunnerError("luna_review_candidate_mismatch")
    manifest_items, judged_bundle_sha256 = build_judged_bundle_manifest(checked_bundle, seed=checked_seed)
    if review["judged_bundle_sha256"] != judged_bundle_sha256:
        raise LunaBatchRunnerError("luna_review_bundle_binding_invalid")
    if json.loads(canonical_json(review["judged_bundle_manifest"])) != json.loads(canonical_json(manifest_items)):
        raise LunaBatchRunnerError("luna_review_manifest_binding_invalid")
    judged_refs = {item["item_ref"] for item in manifest_items}
    for axis in _AXES:
        if any(citation not in judged_refs for citation in review[_AXIS_CITATION_FIELD[axis]]):
            raise LunaBatchRunnerError("luna_review_citation_not_judged")


# ---------------------------------------------------------------------------
# Approval gate + execution receipts + Luna transport.
# ---------------------------------------------------------------------------


def validate_approval_receipt(approval: Any, *, candidate_refs: Sequence[str]) -> dict[str, Any]:
    """Validate the approval receipt that gates every provider-costing call."""

    if not isinstance(approval, dict) or set(approval) != _APPROVAL_KEYS:
        raise PermissionError("luna_approval_receipt_invalid")
    if approval["schema_version"] != APPROVAL_RECEIPT_SCHEMA_VERSION:
        raise PermissionError("luna_approval_receipt_invalid")
    if not isinstance(approval["approval_id"], str) or _ID_RE.fullmatch(approval["approval_id"]) is None:
        raise PermissionError("luna_approval_receipt_invalid")
    if (
        not isinstance(approval["approved_at"], str)
        or _CANONICAL_TIME_RE.fullmatch(approval["approved_at"]) is None
    ):
        raise PermissionError("luna_approval_receipt_invalid")
    if (
        not isinstance(approval["candidate_refs_sha256"], str)
        or _SHA256_RE.fullmatch(approval["candidate_refs_sha256"]) is None
        or approval["candidate_refs_sha256"] != canonical_sha256(list(candidate_refs))
    ):
        raise PermissionError("luna_approval_candidate_binding_invalid")
    return json.loads(canonical_json(approval))


@runtime_checkable
class LunaTransport(Protocol):
    """One Luna Responses call; returns the parsed response body."""

    def complete(self, *, payload: Mapping[str, Any], timeout_ms: int) -> Mapping[str, Any]: ...


def _fake_responses_body(candidate_ref: str, judged_output: Mapping[str, Any], *, model_id: str) -> dict[str, Any]:
    return {
        "id": f"resp_fake_{candidate_ref}",
        "object": "response",
        "status": "completed",
        "model": model_id,
        "output": [
            {
                "id": f"msg_fake_{candidate_ref}",
                "type": "message",
                "status": "completed",
                "role": "assistant",
                "content": [
                    {"type": "output_text", "annotations": [], "text": canonical_json(judged_output)}
                ],
            }
        ],
        "usage": {"input_tokens": 1, "output_tokens": 1, "total_tokens": 2},
    }


class OfflineFakeLunaTransport:
    """Scripted offline Luna transport for tests; performs no network I/O."""

    def __init__(
        self,
        *,
        outputs: Mapping[str, Mapping[str, Any]] | None = None,
        responder: Callable[[Mapping[str, Any]], Mapping[str, Any]] | None = None,
        model_id: str = MODEL_ID,
        latency_s: Mapping[str, float] | None = None,
        failures: Mapping[str, str] | None = None,
    ) -> None:
        self._outputs = {key: json.loads(canonical_json(value)) for key, value in (outputs or {}).items()}
        self._responder = responder
        self._model_id = model_id
        self._latency_s = dict(latency_s or {})
        self._failures = dict(failures or {})
        self.calls: list[dict[str, Any]] = []
        self._lock = threading.Lock()

    def complete(self, *, payload: Mapping[str, Any], timeout_ms: int) -> Mapping[str, Any]:
        candidate_ref = payload["metadata"]["candidate_ref"]
        with self._lock:
            self.calls.append({"payload": json.loads(canonical_json(payload)), "timeout_ms": timeout_ms})
        if candidate_ref in self._failures:
            raise LunaBatchRunnerError(self._failures[candidate_ref])
        latency = self._latency_s.get(candidate_ref, 0.0)
        if latency > 0:
            time.sleep(latency)
        if self._responder is not None:
            judged = self._responder(payload)
        elif candidate_ref in self._outputs:
            judged = self._outputs[candidate_ref]
        else:
            raise LunaBatchRunnerError("luna_fake_candidate_unresolved")
        return _fake_responses_body(candidate_ref, judged, model_id=self._model_id)


def _failed_execution_receipt(
    *,
    candidate_ref: str,
    request_payload_sha256: str | None,
    started_at: str | None,
    completed_at: str | None,
    elapsed_ms: int | None,
    error_code: str,
) -> dict[str, Any]:
    return {
        "schema_version": EXECUTION_RECEIPT_SCHEMA_VERSION,
        "candidate_ref": candidate_ref,
        "provider": PROVIDER_ID,
        "endpoint": RESPONSES_URL,
        "requested_model": MODEL_ID,
        "returned_model": None,
        "exact_model_match": False,
        "request_payload_sha256": request_payload_sha256,
        "prompt_sha256": CANONICAL_PROMPT_SHA256,
        "started_at": started_at,
        "completed_at": completed_at,
        "elapsed_ms": elapsed_ms,
        "outcome": "failed",
        "error_code": error_code,
    }


def review_one_candidate(
    seed: Mapping[str, Any],
    bundle: Mapping[str, Any],
    *,
    transport: LunaTransport,
    prompt: Mapping[str, Any],
    timeout_ms: int = TOTAL_TIMEOUT_MS,
    reasoning_effort: str = DEFAULT_REASONING_EFFORT,
    wall_clock: Callable[[], datetime] | None = None,
    monotonic: Callable[[], float] | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Run the single Luna judgment call for one candidate bundle (§3.2)."""

    now = wall_clock or (lambda: datetime.now(UTC))
    clock = monotonic or time.monotonic
    payload = build_luna_responses_payload(
        seed,
        bundle,
        prompt=prompt,
        reasoning_effort=reasoning_effort,
    )
    encoded_payload = canonical_json(payload).encode("utf-8")
    payload_sha256 = hashlib.sha256(encoded_payload).hexdigest()
    started_at = _timestamp(now())
    started = clock()
    response_body = transport.complete(payload=payload, timeout_ms=timeout_ms)
    judged_raw, returned_model = extract_judged_output(response_body)
    review = build_candidate_review(seed, bundle, judged_raw)
    if review["judged_bundle_sha256"] != payload["metadata"]["judged_bundle_sha256"]:
        raise LunaBatchRunnerError("luna_bundle_mutated_in_flight")
    elapsed_ms = max(0, int((clock() - started) * 1000))
    receipt = {
        "schema_version": EXECUTION_RECEIPT_SCHEMA_VERSION,
        "candidate_ref": review["candidate_ref"],
        "provider": PROVIDER_ID,
        "endpoint": RESPONSES_URL,
        "requested_model": MODEL_ID,
        "returned_model": returned_model,
        "exact_model_match": returned_model == MODEL_ID,
        "request_payload_sha256": payload_sha256,
        "prompt_sha256": CANONICAL_PROMPT_SHA256,
        "started_at": started_at,
        "completed_at": _timestamp(now()),
        "elapsed_ms": elapsed_ms,
        "outcome": "completed",
        "error_code": None,
    }
    return review, receipt


def run_luna_batch(
    *,
    seeds: Sequence[Mapping[str, Any]],
    bundles: Mapping[str, Mapping[str, Any]],
    transport: LunaTransport,
    approval: Mapping[str, Any] | None,
    worker_count: int = DEFAULT_WORKER_COUNT,
    timeout_ms: int = TOTAL_TIMEOUT_MS,
    reasoning_effort: str = DEFAULT_REASONING_EFFORT,
    wall_clock: Callable[[], datetime] | None = None,
    monotonic: Callable[[], float] | None = None,
) -> dict[str, Any]:
    """Judge every candidate bundle with one Luna call each (§3.2).

    The approval receipt is validated BEFORE any provider-costing call: no
    receipt, no adjudication.  Per-candidate failures stay isolated in failed
    rows; results are re-ordered by seed ordinal.
    """

    workers = _check_worker_count(worker_count)
    candidate_refs = _seed_refs(seeds)
    if approval is None:
        raise PermissionError("luna_approval_receipt_required")
    checked_approval = validate_approval_receipt(approval, candidate_refs=candidate_refs)
    prompt = load_prompt()

    def work(ordinal: int) -> dict[str, Any]:
        row: dict[str, Any] = {"candidate_ref": candidate_refs[ordinal], "seed_ordinal": ordinal}
        try:
            seed = validate_seed_input(seeds[ordinal])
            bundle = bundles[seed["seed_ref"]]
            review, receipt = review_one_candidate(
                seed,
                bundle,
                transport=transport,
                prompt=prompt,
                timeout_ms=timeout_ms,
                reasoning_effort=reasoning_effort,
                wall_clock=wall_clock,
                monotonic=monotonic,
            )
        except Exception as exc:  # noqa: BLE001 - failure isolation: one candidate never aborts the batch
            code = exc.args[0] if exc.args and isinstance(exc.args[0], str) else "luna_review_failed"
            row.update(
                {
                    "status": "failed",
                    "error_code": code,
                    "review": None,
                    "execution_receipt": _failed_execution_receipt(
                        candidate_ref=candidate_refs[ordinal],
                        request_payload_sha256=None,
                        started_at=None,
                        completed_at=None,
                        elapsed_ms=None,
                        error_code=code,
                    ),
                }
            )
        else:
            row.update({"status": "completed", "error_code": None, "review": review, "execution_receipt": receipt})
        return row

    results = _run_indexed_pool(list(range(len(candidate_refs))), work, worker_count=workers)
    completed = sum(1 for row in results if row["status"] == "completed")
    return {
        "schema_version": LUNA_BATCH_RESULT_SCHEMA_VERSION,
        "approval_receipt": checked_approval,
        "prompt_version": PROMPT_VERSION,
        "prompt_sha256": CANONICAL_PROMPT_SHA256,
        "worker_count": workers,
        "candidate_count": len(results),
        "completed_count": completed,
        "failed_count": len(results) - completed,
        "results": results,
    }


# ---------------------------------------------------------------------------
# Event-level streaming pipeline: per candidate, collect -> judge fused.
# ---------------------------------------------------------------------------


def run_streaming_pipeline(
    *,
    seeds: Sequence[Mapping[str, Any]],
    grok_transport: GrokTransport,
    luna_transport: LunaTransport,
    approval: Mapping[str, Any] | None,
    worker_count: int = DEFAULT_WORKER_COUNT,
    grok_timeout_ms: int = 300_000,
    luna_timeout_ms: int = 180_000,
    target_direction: str = "pre-training",
    reasoning_effort: str = DEFAULT_REASONING_EFFORT,
    grok_binary: str = "grok",
    wall_clock: Callable[[], datetime] | None = None,
    monotonic: Callable[[], float] | None = None,
) -> dict[str, Any]:
    """Fuse Grok collection and Luna judgment per candidate (operator directive 2026-07-20).

    No global stage barrier: a candidate's Luna call starts the moment its own
    bundle validates, so at 1000+ candidate scale a slow collection never idles
    the judgment capacity of the others.  The approval receipt is validated
    BEFORE any transport call (no receipt, no calls at all).  Failures stay
    per-candidate: a failed collection marks judgment ``not_attempted``; a
    failed judgment never discards the collected bundle.  Rows are re-ordered
    by seed ordinal.
    """

    workers = _check_worker_count(worker_count)
    candidate_refs = _seed_refs(seeds)
    if approval is None:
        raise PermissionError("luna_approval_receipt_required")
    checked_approval = validate_approval_receipt(approval, candidate_refs=candidate_refs)
    prompt = load_prompt()

    def work(ordinal: int) -> dict[str, Any]:
        row: dict[str, Any] = {"candidate_ref": candidate_refs[ordinal], "seed_ordinal": ordinal}
        try:
            seed = validate_seed_input(seeds[ordinal])
            bundle, grok_receipt = collect_candidate_bundle(
                seed,
                transport=grok_transport,
                timeout_ms=grok_timeout_ms,
                grok_binary=grok_binary,
                target_direction=target_direction,
                wall_clock=wall_clock,
                monotonic=monotonic,
            )
        except Exception as exc:  # noqa: BLE001 - failure isolation
            code = exc.args[0] if exc.args and isinstance(exc.args[0], str) else "grok_collection_failed"
            row["grok"] = {
                "status": "failed",
                "error_code": code,
                "bundle": None,
                "operator_receipt": {
                    "schema_version": GROK_OPERATOR_RECEIPT_SCHEMA_VERSION,
                    "candidate_ref": candidate_refs[ordinal],
                    "session_id": None,
                    "prompt_sha256": None,
                    "argv_sha256": None,
                    "timeout_ms": grok_timeout_ms,
                    "started_at": None,
                    "completed_at": None,
                    "elapsed_ms": None,
                    "cost_usd": None,
                    "outcome": "failed",
                    "error_code": code,
                },
            }
            row["luna"] = {
                "status": "not_attempted",
                "error_code": None,
                "review": None,
                "execution_receipt": None,
            }
            return row
        row["grok"] = {"status": "completed", "error_code": None, "bundle": bundle, "operator_receipt": grok_receipt}
        try:
            review, luna_receipt = review_one_candidate(
                seed,
                bundle,
                transport=luna_transport,
                prompt=prompt,
                timeout_ms=luna_timeout_ms,
                reasoning_effort=reasoning_effort,
                wall_clock=wall_clock,
                monotonic=monotonic,
            )
        except Exception as exc:  # noqa: BLE001 - failure isolation: judgment loss never discards the bundle
            code = exc.args[0] if exc.args and isinstance(exc.args[0], str) else "luna_review_failed"
            row["luna"] = {
                "status": "failed",
                "error_code": code,
                "review": None,
                "execution_receipt": _failed_execution_receipt(
                    candidate_ref=candidate_refs[ordinal],
                    request_payload_sha256=None,
                    started_at=None,
                    completed_at=None,
                    elapsed_ms=None,
                    error_code=code,
                ),
            }
            return row
        row["luna"] = {"status": "completed", "error_code": None, "review": review, "execution_receipt": luna_receipt}
        return row

    results = _run_indexed_pool(list(range(len(candidate_refs))), work, worker_count=workers)
    grok_completed = sum(1 for row in results if row["grok"]["status"] == "completed")
    luna_completed = sum(1 for row in results if row["luna"]["status"] == "completed")
    return {
        "schema_version": PIPELINE_RESULT_SCHEMA_VERSION,
        "approval_receipt": checked_approval,
        "prompt_version": PROMPT_VERSION,
        "prompt_sha256": CANONICAL_PROMPT_SHA256,
        "worker_count": workers,
        "candidate_count": len(results),
        "grok_completed_count": grok_completed,
        "luna_completed_count": luna_completed,
        "results": results,
    }


# ---------------------------------------------------------------------------
# Deterministic adapter: candidate reviews -> unchanged axis reducer.
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class _JudgedItemEvidence:
    """Per judged-item evidence row.

    The unchanged ``_build_luna_axis_reduction`` reducer reads only
    ``terminal_json`` (for ``candidate_ref``) from each hydration entry, so the
    adapter supplies exactly that real, candidate-owned field instead of a
    fabricated Grok session projection.
    """

    terminal_json: bytes


def candidate_reviews_to_axis_reduction(
    *,
    reviews: Sequence[Mapping[str, Any]],
    seeds: Sequence[Mapping[str, Any]],
    bundles: Mapping[str, Mapping[str, Any]],
    lab_descriptor: Mapping[str, Any],
    manifest_id: str = "luna_batch_candidate_manifest_v1",
    priors: Mapping[str, Mapping[str, Mapping[str, str]]] | None = None,
) -> dict[str, Any]:
    """Fold candidate reviews into unchanged ``luna_axis_reduction.v1`` rows.

    One judged evidence row per bundle item feeds the unmodified reducer, so
    per axis ``reviewed_evidence_count`` equals the bundle item count,
    ``reviewed_evidence_manifest_sha256`` equals ``judged_bundle_sha256``, and
    coverage is ``complete`` for every judged candidate.  Priors stay the
    frozen resolved state (default ``unsupported``/``unsupported``); Luna
    proposals remain ``diagnostic_only_unattested`` and authorize nothing.
    """

    seed_list = [validate_seed_input(seed) for seed in seeds]
    if not seed_list:
        raise LunaBatchRunnerError("seed_queue_empty")
    seed_by_ref = {seed["seed_ref"]: seed for seed in seed_list}
    if len(seed_by_ref) != len(seed_list):
        raise LunaBatchRunnerError("seed_ref_duplicate")
    default_prior = {"state": "unsupported", "evidence_status": "unsupported"}
    candidates: list[dict[str, Any]] = []
    for seed in seed_list:
        bundle = validate_candidate_bundle(bundles[seed["seed_ref"]], seed=seed)
        handle = bundle["account_resolution"]["handle"]
        candidate_priors = (priors or {}).get(seed["seed_ref"], {})
        candidates.append(
            {
                "candidate_ref": seed["seed_ref"],
                "platform_user_id": None,
                "current_handle": handle,
                "profile_url": f"https://{lab_descriptor['x_url_host']}/{handle}",
                "lab_affiliation_prior": dict(candidate_priors.get("lab_affiliation", default_prior)),
                "pretraining_experience_prior": dict(
                    candidate_priors.get("pretraining_experience", default_prior)
                ),
            }
        )
    try:
        manifest = freeze_candidate_manifest(
            manifest_id=manifest_id,
            lab_descriptor=lab_descriptor,
            candidates=candidates,
        )
    except SourceNeutralMappingError as exc:
        raise LunaBatchRunnerError(f"luna_axis_manifest_invalid:{exc.args[0]}") from exc

    hydration_by_sha: dict[str, _JudgedItemEvidence] = {}
    checked_luna: dict[str, dict[str, Any]] = {}
    for review in reviews:
        candidate_ref = review["candidate_ref"] if isinstance(review, Mapping) else None
        if candidate_ref not in seed_by_ref:
            raise LunaBatchRunnerError("luna_review_candidate_unknown")
        bundle = validate_candidate_bundle(bundles[candidate_ref], seed=seed_by_ref[candidate_ref])
        validate_candidate_review_binding(review, seed=seed_by_ref[candidate_ref], bundle=bundle)
        for item in review["judged_bundle_manifest"]:
            item_sha = item["sha256"]
            if item_sha in hydration_by_sha:
                raise LunaBatchRunnerError("judged_item_digest_collision")
            hydration_by_sha[item_sha] = _JudgedItemEvidence(
                terminal_json=canonical_json({"candidate_ref": candidate_ref}).encode("utf-8")
            )
            checked_luna[item_sha] = {
                "proposed_lab_affiliation_state": review["proposed_lab_affiliation_state"],
                "proposed_pretraining_experience_state": review["proposed_pretraining_experience_state"],
            }
    try:
        return _build_luna_axis_reduction(
            manifest=manifest,
            hydration_by_sha=hydration_by_sha,
            checked_luna=checked_luna,
        )
    except SourceNeutralMappingError as exc:
        raise LunaBatchRunnerError(f"luna_axis_reduction_failed:{exc.args[0]}") from exc
