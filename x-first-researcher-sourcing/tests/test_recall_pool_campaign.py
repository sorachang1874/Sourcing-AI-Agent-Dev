from __future__ import annotations

import copy
import hashlib
import json
import os
import stat
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from typing import Any
from unittest import mock
from uuid import NAMESPACE_DNS, uuid5

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from scripts.merge_recall_pool_campaign import (  # noqa: E402
    _load_sources,
    _publish_private_no_replace,
    _regular_file_size,
)
from x_first.recall_pool_campaign import (  # noqa: E402
    CONTRACT_SCHEMA_FILES,
    CampaignValidationError,
    WaveInput,
    canonical_json,
    canonical_sha256,
    family_attribution_plan_sha256,
    merge_campaign,
    replay_and_validate_campaign_result,
    strategy_definition_sha256,
    user_visible_chat_context_sha256,
    validate_campaign_result,
)
from x_first.recall_pool_schema import (  # noqa: E402
    MiniDraft202012Error,
    assert_schema_valid,
)


def _json_bytes(value: Any) -> bytes:
    return (canonical_json(value) + "\n").encode()


def _sha(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _policy() -> dict[str, Any]:
    return json.loads((ROOT / "configs/recall_pool_campaign_policy.v1.json").read_text())


def _evidence(
    author_handle: str,
    *,
    kind: str = "post",
    relationship: str = "self",
    post_id: str = "123456",
    excerpt: str = "Synthetic pretraining evidence.",
    supports: list[Any] | None = None,
) -> dict[str, Any]:
    return {
        "kind": kind,
        "relationship": relationship,
        "author_handle": author_handle,
        "post_id": post_id,
        "url": f"https://x.com/{author_handle}/status/{post_id}",
        "published_at": "2026-07-14T00:00:00Z",
        "excerpt": excerpt,
        "supports": ["pretraining_experience_state"] if supports is None else supports,
    }


def _candidate(
    handle: str,
    *,
    platform_user_id: str | None = "100",
    bio_excerpt: str | None = "Synthetic lab researcher Bio.",
    lab_state: str = "current",
    pretrain_state: str = "current",
    evidence: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    return {
        "handle": handle,
        "profile_url": f"https://x.com/{handle}",
        "platform_user_id": platform_user_id,
        "bio_excerpt": bio_excerpt,
        "target_lab_affiliation_state": lab_state,
        "pretraining_experience_state": pretrain_state,
        "confidence": "medium",
        "evidence": [_evidence(handle)] if evidence is None else evidence,
        "caveats": ["Synthetic offline test row."],
    }


def _v2_evidence(
    subject_handle: str,
    *,
    post_id: str,
    asserted_value: str,
    thread_relation: str | None = "self_post",
    excerpt: str = "Synthetic authored pretraining evidence.",
) -> dict[str, Any]:
    return {
        "kind": "post",
        "relationship": "self",
        "subject_handle": subject_handle,
        "author_handle": subject_handle,
        "post_id": post_id,
        "url": f"https://x.com/{subject_handle}/status/{post_id}",
        "published_at": "2026-07-14T00:00:00Z",
        "excerpt": excerpt,
        "thread_relation": thread_relation,
        "supports": [
            {
                "dimension": "pretraining_experience_state",
                "asserted_value": asserted_value,
            }
        ],
    }


def _v2_candidate(
    handle: str,
    *,
    evidence: list[dict[str, Any]],
    pretrain_state: str = "historical",
) -> dict[str, Any]:
    candidate = _candidate(
        handle,
        pretrain_state=pretrain_state,
        evidence=evidence,
    )
    candidate["overlap_status"] = "novel"
    return candidate


def _wave_payload(
    candidates: list[dict[str, Any]],
    *,
    model_candidate_rows: int | None = None,
    model_evidence_items: int | None = None,
    model_tool_calls: int = 1,
    model_queries: list[str] | None = None,
    generic_web_used: bool = False,
    tools_reported: list[str] | None = None,
    model_tool_counts: dict[str, int] | None = None,
) -> dict[str, Any]:
    queries = ["model reported query"] if model_queries is None else model_queries
    evidence_items = sum(len(row["evidence"]) for row in candidates)
    return {
        "status": "X_SEARCH_PARTIAL",
        "status_reason": "Synthetic offline wave.",
        "native_x_tool_provenance": {
            "generic_web_used": generic_web_used,
            "tool_calls_reported": model_tool_calls,
            "tools_reported": ["x_user_search"] if tools_reported is None else tools_reported,
            "queries": queries,
        },
        "counts": {
            "observations_inspected_reported": len(candidates),
            "candidates_retained": len(candidates) if model_candidate_rows is None else model_candidate_rows,
        },
        "candidates": candidates,
        "excluded_examples": [],
        "limitations": [],
        "local_reconciliation": {
            "candidate_records_validated": len(candidates),
            "evidence_items_validated": evidence_items if model_evidence_items is None else model_evidence_items,
            "post_urls_structurally_validated": evidence_items,
            "provider_post_bodies_replayable": False,
            "tool_calls_completed": model_tool_calls,
            "tool_counts": {"x_user_search": model_tool_calls} if model_tool_counts is None else model_tool_counts,
        },
    }


def _uuid(label: str) -> str:
    return str(uuid5(NAMESPACE_DNS, label))


def _call(wave_id: str, index: int, *, user_search_count: int = 10) -> dict[str, Any]:
    arguments = {
        "query": f"synthetic query {wave_id} {index}",
        "count": str(user_search_count),
    }
    call = {
        "tool_call_id": f"tool-{wave_id}-{index}",
        "provider_call_id": f"provider-{wave_id}-{index}",
        "tool_name": "x_user_search",
        "arguments": arguments,
    }
    call["call_identity_sha256"] = canonical_sha256(call)
    call["planned_call_identity_sha256"] = canonical_sha256(
        {
            "sequence_ordinal": index,
            "tool_name": call["tool_name"],
            "arguments": call["arguments"],
        }
    )
    return call


def _native_x_call(
    wave_id: str,
    index: int,
    *,
    tool_name: str,
    arguments: dict[str, str],
) -> dict[str, Any]:
    call = {
        "tool_call_id": f"tool-{wave_id}-{index}",
        "provider_call_id": f"provider-{wave_id}-{index}",
        "tool_name": tool_name,
        "arguments": arguments,
    }
    call["call_identity_sha256"] = canonical_sha256(call)
    call["planned_call_identity_sha256"] = canonical_sha256(
        {
            "sequence_ordinal": index,
            "tool_name": tool_name,
            "arguments": arguments,
        }
    )
    return call


def _user_search_call_shape(count: int = 10) -> dict[str, Any]:
    return {
        "tool_name": "x_user_search",
        "limit": None,
        "count": count,
        "mode": None,
        "min_score_threshold": None,
        "thread_argument_shape": None,
    }


def _strategy_definition(
    families: list[str],
    *,
    allowed_user_search_counts: tuple[int, ...] = (10,),
) -> dict[str, Any]:
    return {
        "schema_version": "x.recall_pool.campaign.strategy_definition.v1",
        "query_families": [
            {
                "family_id": family,
                "family_version": f"{family}.v1",
                "allowed_call_shapes": [_user_search_call_shape(count) for count in allowed_user_search_counts],
            }
            for family in families
        ],
    }


def _legacy_strategy_digest(strategy_id: str, families: list[str]) -> str:
    return canonical_sha256(
        {
            "comparability_rule_version": "exact_strategy_definition_and_family_call_mix.v1",
            "query_family_ids": families,
            "strategy_id": strategy_id,
        }
    )


def _bundle(
    wave_id: str,
    candidates: list[dict[str, Any]],
    *,
    call_count: int = 2,
    exact_attribution: bool = False,
    strategy_id: str = "broad_recall.v1",
    query_family_ids: list[str] | None = None,
    prior_exclusion_status: str = "not_applicable",
    bind_user_context: bool = True,
    legacy_strategy: bool = False,
    user_search_count: int = 10,
    allowed_user_search_counts: tuple[int, ...] = (10,),
    model_candidate_rows: int | None = None,
    model_evidence_items: int | None = None,
    model_tool_calls: int = 1,
    model_queries: list[str] | None = None,
    generic_web_used: bool = False,
    calls_override: list[dict[str, Any]] | None = None,
    wave_result_v2: bool = False,
) -> dict[str, Any]:
    families = ["broad"] if query_family_ids is None else query_family_ids
    if call_count < len(families):
        raise AssertionError("synthetic call count must cover every family")
    session_id = _uuid(f"session-{wave_id}")
    request_id = _uuid(f"request-{wave_id}")
    model_id = "grok-4.5"
    prompt = f"Synthetic tracked prompt for {wave_id}.\n".encode()
    system_prompt = f"Synthetic system prompt. Prior exclusion status: {prior_exclusion_status}.\n".encode()
    calls = (
        [_call(wave_id, index, user_search_count=user_search_count) for index in range(call_count)]
        if calls_override is None
        else calls_override
    )
    tools_reported = sorted({call["tool_name"] for call in calls})
    reported_tool_counts = {
        tool: sum(call["tool_name"] == tool for call in calls) for tool in tools_reported
    }
    reported_model_tool_calls = len(calls) if calls_override is not None else model_tool_calls
    result_bytes = _json_bytes(
        _wave_payload(
            candidates,
            model_candidate_rows=model_candidate_rows,
            model_evidence_items=model_evidence_items,
            model_tool_calls=reported_model_tool_calls,
            model_queries=model_queries,
            generic_web_used=generic_web_used,
            tools_reported=tools_reported,
            model_tool_counts=reported_tool_counts,
        )
    )
    updates = [
        {
            "params": {
                "sessionId": session_id,
                "update": {
                    "sessionUpdate": "user_message_chunk",
                    "content": "synthetic",
                    "_meta": {"modelId": model_id, "promptIndex": 0},
                },
            }
        }
    ]
    for call in calls:
        updates.append(
            {
                "params": {
                    "sessionId": session_id,
                    "_meta": {"promptId": request_id},
                    "update": {
                        "sessionUpdate": "tool_call",
                        "toolCallId": call["tool_call_id"],
                        "status": "in_progress",
                    },
                }
            }
        )
        updates.append(
            {
                "params": {
                    "sessionId": session_id,
                    "_meta": {"promptId": request_id},
                    "update": {
                        "sessionUpdate": "tool_call_update",
                        "toolCallId": call["tool_call_id"],
                        "status": "completed",
                        "rawOutput": {
                            "call_id": call["provider_call_id"],
                            "id": call["tool_call_id"],
                            "input": canonical_json(call["arguments"]),
                            "name": call["tool_name"],
                        },
                    },
                }
            }
        )
    assistant_output = "Synthetic non-JSON preface.\n" + result_bytes[:-1].decode("utf-8")
    split_at = len(assistant_output) // 2
    for index, text in enumerate(
        (assistant_output[:split_at], assistant_output[split_at:]),
        start=1,
    ):
        updates.append(
            {
                "params": {
                    "sessionId": session_id,
                    "_meta": {
                        "promptId": request_id,
                        "updateType": "AgentMessageChunk",
                        "chunkId": index * 100,
                        "eventId": f"{session_id}-{100 + index}",
                        "agentTimestampMs": 1_000 + index,
                        "streamStartMs": 900,
                        "turnStartMs": 800,
                        "totalTokens": 123,
                    },
                    "update": {
                        "sessionUpdate": "agent_message_chunk",
                        "content": {"type": "text", "text": text},
                    },
                }
            }
        )
    chat_rows = [
        {"type": "system", "content": system_prompt.decode()},
        {
            "type": "user",
            "content": [
                {
                    "type": "text",
                    "text": f"<user_query>\n{prompt[:-1].decode()}\n</user_query>",
                }
            ],
        },
    ]
    definition = _strategy_definition(
        families,
        allowed_user_search_counts=allowed_user_search_counts,
    )
    prompt_context_payload: dict[str, Any] = {
        "working_directory": str(ROOT),
        "prompt_mode": "synthetic",
    }
    if exact_attribution:
        mapping = {family: [] for family in families}
        for index, call in enumerate(calls):
            mapping[families[index % len(families)]].append(call["planned_call_identity_sha256"])
        definition_digest = strategy_definition_sha256(strategy_id, definition)
        attribution_plan_digest = family_attribution_plan_sha256(definition_digest, mapping)
        prompt_context_payload["recall_pool_family_attribution_plan_sha256"] = attribution_plan_digest
        attribution = {
            "status": "precommitted_runner_bound",
            "family_planned_call_sha256s": mapping,
            "attribution_plan_sha256": attribution_plan_digest,
        }
    else:
        attribution = {"status": "unavailable", "family_call_sha256s": None}
    prompt_context = _json_bytes(prompt_context_payload)
    raw_files = {
        "summary.json": _json_bytes(
            {
                "info": {"id": session_id, "cwd": str(ROOT)},
                "request_id": request_id,
                "current_model_id": model_id,
                "num_messages": len(updates),
            }
        ),
        "updates.jsonl": b"".join(_json_bytes(row) for row in updates),
        "events.jsonl": b"".join(
            _json_bytes(row)
            for row in [
                {"type": "turn_started", "session_id": session_id, "model_id": model_id},
                {"type": "turn_ended", "outcome": "completed"},
            ]
        ),
        "chat_history.jsonl": b"".join(_json_bytes(row) for row in chat_rows),
        "system_prompt.txt": system_prompt,
        "prompt_context.json": prompt_context,
    }
    if legacy_strategy:
        strategy = {
            "strategy_id": strategy_id,
            "strategy_definition_sha256": _legacy_strategy_digest(strategy_id, families),
            "query_family_ids": families,
            "execution_attribution": {"status": "unavailable", "family_call_sha256s": None},
        }
    else:
        strategy = {
            "strategy_id": strategy_id,
            "strategy_definition_sha256": strategy_definition_sha256(strategy_id, definition),
            "strategy_definition": definition,
            "query_family_ids": families,
            "execution_attribution": attribution,
        }
    execution_context = {
        "system_prompt_sha256": _sha(system_prompt),
        "prompt_context_sha256": _sha(prompt_context),
        "prior_exclusion_binding_status": prior_exclusion_status,
    }
    if bind_user_context:
        execution_context["user_visible_chat_context"] = {
            "binding_status": "precommitted_exact",
            "sha256": user_visible_chat_context_sha256(chat_rows),
        }
    upstream = {
        "schema_version": (
            "x.recall_pool.campaign.wave_request.v2"
            if wave_result_v2
            else "x.recall_pool.campaign.wave_request.v1"
        ),
        "wave_id": wave_id,
        "wave_result_schema_version": (
            "x.grok_cli.recall_wave.result_adapter.v2"
            if wave_result_v2
            else "x.grok_cli.recall_wave.result_adapter.v1"
        ),
        "target": {"lab_id": "synthetic_lab", "research_focus_id": "pretraining"},
        "prompt_sha256": _sha(prompt),
        "expected_session_id": session_id,
        "expected_request_id": request_id,
        "expected_model_id": model_id,
        "strategy": strategy,
        "execution_context": execution_context,
    }
    upstream_bytes = _json_bytes(upstream)
    return {
        "wave": WaveInput(wave_id, result_bytes, upstream_bytes, prompt, raw_files),
        "binding": {
            "wave_id": wave_id,
            "source_path": f"{wave_id}.json",
            "source_sha256": _sha(result_bytes),
            "upstream_request": {"path": f"{wave_id}.request.json", "sha256": _sha(upstream_bytes)},
            "prompt": {"path": f"{wave_id}.prompt.md", "sha256": _sha(prompt)},
            "raw_session_directory": f"{wave_id}.session",
        },
    }


def _merge(bundles: list[dict[str, Any]], policy: dict[str, Any] | None = None) -> dict[str, Any]:
    request = {
        "schema_version": "x.recall_pool.campaign.request.v1",
        "campaign_id": "synthetic_campaign",
        "target": {"lab_id": "synthetic_lab", "research_focus_id": "pretraining"},
        "waves": [bundle["binding"] for bundle in bundles],
    }
    return merge_campaign(request, _policy() if policy is None else policy, [bundle["wave"] for bundle in bundles])


def _materialize_bundle_sources(
    root: Path,
    bundle: dict[str, Any],
) -> tuple[dict[str, Any], Path, Path]:
    os.chmod(root, 0o700)
    binding = bundle["binding"]
    source = root / binding["source_path"]
    source.write_bytes(bundle["wave"].result_bytes)
    os.chmod(source, 0o600)
    upstream = root / binding["upstream_request"]["path"]
    upstream.write_bytes(bundle["wave"].upstream_request_bytes)
    os.chmod(upstream, 0o600)
    prompt = root / binding["prompt"]["path"]
    prompt.write_bytes(bundle["wave"].prompt_bytes)
    os.chmod(prompt, 0o600)
    session = root / binding["raw_session_directory"]
    session.mkdir(mode=0o700)
    for name, raw in bundle["wave"].raw_session_files.items():
        path = session / name
        path.write_bytes(raw)
        os.chmod(path, 0o600)
    request = {
        "schema_version": "x.recall_pool.campaign.request.v1",
        "campaign_id": "synthetic_campaign",
        "target": {"lab_id": "synthetic_lab", "research_focus_id": "pretraining"},
        "waves": [binding],
    }
    manifest = root / "manifest.json"
    manifest.write_bytes(_json_bytes(request))
    os.chmod(manifest, 0o600)
    return request, manifest, source


def _mutate_system_chat(bundle: dict[str, Any]) -> None:
    rows = [json.loads(line) for line in bundle["wave"].raw_session_files["chat_history.jsonl"].decode().splitlines()]
    systems = [row for row in rows if row.get("type") == "system"]
    systems[0]["content"] += "mutated"
    bundle["wave"].raw_session_files["chat_history.jsonl"] = b"".join(_json_bytes(row) for row in rows)


def _mutate_assistant_prompt_binding(bundle: dict[str, Any]) -> None:
    rows = [json.loads(line) for line in bundle["wave"].raw_session_files["updates.jsonl"].decode().splitlines()]
    for row in rows:
        update = row.get("params", {}).get("update", {})
        if update.get("sessionUpdate") == "agent_message_chunk":
            row["params"]["_meta"]["promptId"] = _uuid("wrong-assistant-prompt")
            break
    bundle["wave"].raw_session_files["updates.jsonl"] = b"".join(_json_bytes(row) for row in rows)


class RecallPoolCampaignTests(unittest.TestCase):
    def test_casefold_merge_preserves_two_axis_history_and_unverified_state(self) -> None:
        first = _candidate("Alpha", platform_user_id="101", evidence=[])
        second = _candidate(
            "alpha",
            platform_user_id="101",
            lab_state="historical",
            evidence=[_evidence("alpha", supports=["target_lab_affiliation_state"])],
        )
        result = _merge([_bundle("wave_1", [first]), _bundle("wave_2", [second])])
        self.assertEqual(result["metrics"]["raw_candidate_rows"], 2)
        self.assertEqual(result["metrics"]["total_unique_handles"], 1)
        candidate = result["candidates"][0]
        self.assertEqual(candidate["handle_variants"], ["Alpha", "alpha"])
        lab = candidate["state_summary"]["target_lab_affiliation_state"]
        self.assertEqual(lab["model_reported_resolution"], "conflict")
        self.assertIsNone(lab["evidence_supported_resolution"])
        self.assertEqual(lab["evidence_support_status"], "model_mediated_unverified")
        pretrain = candidate["state_summary"]["pretraining_experience_state"]
        self.assertIsNone(pretrain["evidence_supported_resolution"])
        self.assertEqual(pretrain["evidence_support_status"], "model_mediated_unverified")

    def test_model_output_fields_never_become_source_supported_without_payload(self) -> None:
        evidence = _evidence(
            "Alpha",
            supports=[
                {
                    "dimension": "pretraining_experience_state",
                    "asserted_value": "historical",
                }
            ],
        )
        result = _merge([_bundle("wave_1", [_candidate("Alpha", evidence=[evidence])])])
        self.assertFalse(result["provenance"]["raw_tool_source_payload_available"])
        self.assertEqual(
            set(result["provenance"].values()) - {False},
            {"model_mediated_unverified"},
        )
        candidate = result["candidates"][0]
        self.assertEqual(
            set(candidate["field_source_status"].values()),
            {"model_mediated_unverified"},
        )
        state = candidate["state_summary"]["pretraining_experience_state"]
        self.assertEqual(state["evidence_supported_values"], [])
        self.assertIsNone(state["evidence_supported_resolution"])
        self.assertEqual(state["evidence_support_status"], "model_mediated_unverified")
        normalized_evidence = candidate["evidence"][0]
        self.assertEqual(normalized_evidence["source_status"], "model_mediated_unverified")
        self.assertEqual(
            normalized_evidence["support_claims"],
            [
                {
                    "dimension": "pretraining_experience_state",
                    "asserted_value": "historical",
                    "source_status": "model_mediated_unverified",
                }
            ],
        )
        self.assertEqual(
            result["identity_index"]["stable_id_reverse_index"][0]["platform_user_id_status"],
            "model_mediated_unverified",
        )

    def test_all_user_visible_chat_context_is_precommitted_or_downgraded(self) -> None:
        for mutation in ("extra_row", "extra_text"):
            with self.subTest(mutation=mutation):
                bundle = _bundle("wave_1", [_candidate("Alpha")])
                rows = [
                    json.loads(line)
                    for line in bundle["wave"].raw_session_files["chat_history.jsonl"].decode().splitlines()
                ]
                if mutation == "extra_row":
                    rows.append(
                        {
                            "type": "user",
                            "content": [{"type": "text", "text": "unbound visible context"}],
                        }
                    )
                else:
                    user_row = next(row for row in rows if row.get("type") == "user")
                    user_row["content"].append({"type": "text", "text": "unbound visible context"})
                bundle["wave"].raw_session_files["chat_history.jsonl"] = b"".join(_json_bytes(row) for row in rows)
                with self.assertRaisesRegex(
                    CampaignValidationError,
                    "raw_session_user_chat_context_hash_mismatch",
                ):
                    _merge([bundle])

        legacy = _merge(
            [
                _bundle(
                    "wave_1",
                    [_candidate("Alpha")],
                    exact_attribution=True,
                    bind_user_context=False,
                    legacy_strategy=True,
                )
            ]
        )
        receipt = legacy["wave_yields"][0]["mechanically_observed"]["receipt"]
        self.assertEqual(receipt["user_visible_chat_context_binding_status"], "legacy_unbound")
        self.assertEqual(receipt["request_context_replay_status"], "operator_asserted")
        self.assertEqual(receipt["strategy_definition_binding_status"], "legacy_ids_only_unbound")
        self.assertEqual(receipt["family_attribution_binding_status"], "legacy_unbound")
        self.assertEqual(legacy["stop_advisory"]["evaluation_status"], "insufficient_proof")
        self.assertEqual(legacy["stop_advisory"]["recommendation"], "continue_expansion")

    def test_terminal_assistant_chunk_must_follow_every_native_x_tool_event(self) -> None:
        bundle = _bundle("wave_1", [_candidate("Alpha")])
        rows = [json.loads(line) for line in bundle["wave"].raw_session_files["updates.jsonl"].decode().splitlines()]
        completion_index = next(
            index
            for index, row in enumerate(rows)
            if row.get("params", {}).get("update", {}).get("sessionUpdate") == "tool_call_update"
        )
        rows.append(rows.pop(completion_index))
        bundle["wave"].raw_session_files["updates.jsonl"] = b"".join(_json_bytes(row) for row in rows)
        with self.assertRaisesRegex(
            CampaignValidationError,
            "raw_session_terminal_assistant_causality_invalid",
        ):
            _merge([bundle])

    def test_terminal_json_cannot_start_before_tools_and_finish_after_them(self) -> None:
        bundle = _bundle("wave_1", [_candidate("Alpha")])
        rows = [json.loads(line) for line in bundle["wave"].raw_session_files["updates.jsonl"].decode().splitlines()]
        first_assistant_index = next(
            index
            for index, row in enumerate(rows)
            if row.get("params", {}).get("update", {}).get("sessionUpdate") == "agent_message_chunk"
        )
        first_assistant = rows.pop(first_assistant_index)
        self.assertIn("{", first_assistant["params"]["update"]["content"]["text"])
        first_tool_index = next(
            index
            for index, row in enumerate(rows)
            if row.get("params", {}).get("update", {}).get("sessionUpdate") == "tool_call"
        )
        rows.insert(first_tool_index, first_assistant)
        bundle["wave"].raw_session_files["updates.jsonl"] = b"".join(_json_bytes(row) for row in rows)
        with self.assertRaisesRegex(
            CampaignValidationError,
            "raw_session_terminal_assistant_causality_invalid",
        ):
            _merge([bundle])

    def test_raw_replay_owns_tool_counts_and_campaign_query_uniqueness(self) -> None:
        first = _bundle("wave_1", [_candidate("Alpha")], call_count=3, model_tool_calls=99, model_queries=["same"])
        second = _bundle("wave_2", [_candidate("Beta")], call_count=2, model_tool_calls=1, model_queries=["same"])
        result = _merge([first, second])
        self.assertEqual(result["metrics"]["replayed_completed_native_x_calls"], 5)
        self.assertEqual(result["metrics"]["model_reported_tool_calls"], 100)
        self.assertEqual(result["wave_yields"][0]["discrepancies"]["replayed_minus_model_tool_calls"], -96)
        self.assertEqual(result["metrics"]["model_reported_query_count"], 2)
        self.assertEqual(result["metrics"]["model_reported_global_unique_query_count"], 1)
        self.assertEqual(result["metrics"]["replayed_global_unique_query_count"], 5)

    def test_raw_sources_and_terminal_fail_closed(self) -> None:
        mutators = {
            "summary_identity": lambda bundle: bundle["wave"].raw_session_files.__setitem__(
                "summary.json",
                _json_bytes(
                    {
                        "info": {"id": _uuid("wrong")},
                        "request_id": "wrong",
                        "current_model_id": "grok-4.5",
                    }
                ),
            ),
            "prompt": lambda bundle: setattr(bundle["wave"], "prompt_bytes", b"changed\n"),
            "system_prompt": lambda bundle: bundle["wave"].raw_session_files.__setitem__(
                "system_prompt.txt",
                b"changed\n",
            ),
            "system_chat": _mutate_system_chat,
            "assistant_prompt_binding": _mutate_assistant_prompt_binding,
            "terminal": lambda bundle: bundle["wave"].raw_session_files.__setitem__(
                "events.jsonl",
                _json_bytes(
                    {
                        "type": "turn_started",
                        "session_id": _uuid("session-wave_1"),
                        "model_id": "grok-4.5",
                    }
                )
                + _json_bytes({"type": "turn_ended", "outcome": "failed"}),
            ),
            "missing_complete": lambda bundle: bundle["wave"].raw_session_files.__setitem__(
                "updates.jsonl", bundle["wave"].raw_session_files["updates.jsonl"].splitlines(keepends=True)[0]
            ),
        }
        for name, mutate in mutators.items():
            with self.subTest(name=name):
                bundle = _bundle("wave_1", [_candidate("Alpha")], call_count=1)
                mutable_raw = dict(bundle["wave"].raw_session_files)
                bundle["wave"] = WaveInput(
                    bundle["wave"].wave_id,
                    bundle["wave"].result_bytes,
                    bundle["wave"].upstream_request_bytes,
                    bundle["wave"].prompt_bytes,
                    mutable_raw,
                )
                if name == "prompt":
                    bundle["wave"] = WaveInput(
                        bundle["wave"].wave_id,
                        bundle["wave"].result_bytes,
                        bundle["wave"].upstream_request_bytes,
                        b"changed\n",
                        mutable_raw,
                    )
                else:
                    mutate(bundle)
                with self.assertRaises(CampaignValidationError):
                    _merge([bundle])

    def test_forged_result_and_manifest_hash_cannot_replace_raw_assistant_output(self) -> None:
        bundle = _bundle("wave_1", [_candidate("Alpha")])
        forged = json.loads(bundle["wave"].result_bytes)
        forged["status_reason"] = "Coherently rehashed but not emitted by the assistant."
        forged_raw = _json_bytes(forged)
        bundle["wave"] = WaveInput(
            bundle["wave"].wave_id,
            forged_raw,
            bundle["wave"].upstream_request_bytes,
            bundle["wave"].prompt_bytes,
            bundle["wave"].raw_session_files,
        )
        bundle["binding"]["source_sha256"] = _sha(forged_raw)
        with self.assertRaisesRegex(
            CampaignValidationError,
            "assistant_output_result_binding_mismatch",
        ):
            _merge([bundle])

    def test_receipt_binds_assistant_output_and_strategy_definition(self) -> None:
        result = _merge([_bundle("wave_1", [_candidate("Alpha")], exact_attribution=True)])
        receipt = result["wave_yields"][0]["mechanically_observed"]["receipt"]
        self.assertEqual(receipt["assistant_output_binding_status"], "terminal_json_canonical_match")
        self.assertEqual(
            receipt["assistant_terminal_json_sha256"],
            canonical_sha256(json.loads(_bundle("wave_1", [_candidate("Alpha")])["wave"].result_bytes)),
        )
        self.assertEqual(
            receipt["strategy_definition_sha256"],
            strategy_definition_sha256("broad_recall.v1", _strategy_definition(["broad"])),
        )
        self.assertEqual(receipt["strategy_definition_binding_status"], "versioned_complete")
        self.assertEqual(receipt["family_attribution_binding_status"], "precommitted_runner_bound")
        self.assertEqual(
            receipt["query_family_call_profiles"],
            [
                {
                    "family_id": "broad",
                    "completed_call_count": 2,
                    "call_shapes": [{**_user_search_call_shape(), "completed_call_count": 2}],
                }
            ],
        )
        self.assertGreater(
            receipt["assistant_terminal_json_start_update_index"],
            receipt["last_native_x_update_index"],
        )
        self.assertLess(
            receipt["assistant_terminal_json_start_byte_offset"],
            receipt["assistant_terminal_json_end_byte_offset_exclusive"],
        )
        self.assertLessEqual(
            receipt["assistant_terminal_json_start_chunk_index"],
            receipt["assistant_terminal_json_end_chunk_index"],
        )
        self.assertLessEqual(
            receipt["assistant_terminal_json_end_update_index"],
            receipt["final_assistant_update_index"],
        )
        tampered = copy.deepcopy(result)
        tampered_observed = tampered["wave_yields"][0]["mechanically_observed"]
        tampered_receipt = tampered_observed["receipt"]
        tampered_receipt["assistant_terminal_json_start_update_index"] = tampered_receipt["last_native_x_update_index"]
        tampered_observed["receipt_sha256"] = canonical_sha256(tampered_receipt)
        with self.assertRaisesRegex(
            CampaignValidationError,
            "campaign_result_receipt_causality_or_source_invalid",
        ):
            validate_campaign_result(tampered)

    def test_evidence_url_author_mismatch_is_quarantined_not_promoted(self) -> None:
        evidence = _evidence("Other")
        evidence["author_handle"] = "Alpha"
        result = _merge([_bundle("wave_1", [_candidate("Alpha", evidence=[evidence])])])
        self.assertEqual(result["metrics"]["rejected_evidence_association_rows"], 1)
        self.assertEqual(result["metrics"]["valid_candidate_evidence_association_rows"], 0)
        self.assertEqual(result["candidates"][0]["evidence"], [])
        self.assertEqual(
            result["candidates"][0]["state_summary"]["pretraining_experience_state"]["evidence_support_status"],
            "model_mediated_unverified",
        )

    def test_persisted_evidence_url_and_post_id_are_revalidated_after_rehash(self) -> None:
        result = _merge([_bundle("wave_1", [_candidate("Alpha")])])

        author_mismatch = copy.deepcopy(result)
        author_evidence = author_mismatch["candidates"][0]["evidence"][0]
        author_evidence["author_handle"] = "Other"
        author_evidence["evidence_record_sha256"] = canonical_sha256(
            {
                key: author_evidence[key]
                for key in (
                    "subject_handle",
                    "subject_binding_status",
                    "kind",
                    "relationship",
                    "author_handle",
                    "post_id",
                    "url",
                    "published_at",
                    "excerpt",
                    "support_claims",
                    "source_status",
                )
            }
        )
        with self.assertRaisesRegex(
            CampaignValidationError,
            "campaign_result_evidence_url_binding_invalid",
        ):
            validate_campaign_result(author_mismatch)

        post_id_mismatch = copy.deepcopy(result)
        post_evidence = post_id_mismatch["candidates"][0]["evidence"][0]
        post_evidence["post_id"] = "999999"
        post_evidence["evidence_record_sha256"] = canonical_sha256(
            {
                key: post_evidence[key]
                for key in (
                    "subject_handle",
                    "subject_binding_status",
                    "kind",
                    "relationship",
                    "author_handle",
                    "post_id",
                    "url",
                    "published_at",
                    "excerpt",
                    "support_claims",
                    "source_status",
                )
            }
        )
        with self.assertRaisesRegex(
            CampaignValidationError,
            "campaign_result_evidence_post_binding_invalid",
        ):
            validate_campaign_result(post_id_mismatch)

    def test_thread_evidence_is_preserved_and_schema_valid(self) -> None:
        result = _merge(
            [
                _bundle(
                    "wave_1",
                    [_candidate("Alpha", evidence=[_evidence("Alpha", kind="thread")])],
                )
            ]
        )
        self.assertEqual(result["candidates"][0]["evidence"][0]["kind"], "thread")
        assert_schema_valid(result, "x.recall_pool.campaign.result.v1.schema.json")

    def test_same_stable_id_across_handles_is_preserved_as_reversible_proposal(self) -> None:
        result = _merge(
            [
                _bundle("wave_1", [_candidate("Alpha", platform_user_id="777")]),
                _bundle("wave_2", [_candidate("Beta", platform_user_id="777")]),
            ]
        )
        self.assertEqual(result["metrics"]["total_unique_handles"], 2)
        row = result["identity_index"]["stable_id_reverse_index"][0]
        self.assertEqual(row["status"], "multi_handle_conflict")
        self.assertEqual(row["handle_keys"], ["alpha", "beta"])
        self.assertTrue(row["handle_history_proposal"]["reversible"])
        self.assertFalse(row["handle_history_proposal"]["auto_merge_authorized"])

    def test_evidence_records_are_distinct_from_candidate_associations(self) -> None:
        shared = _evidence("Source", relationship="third_party")
        result = _merge(
            [
                _bundle(
                    "wave_1",
                    [
                        _candidate("Alpha", evidence=[shared, copy.deepcopy(shared)]),
                        _candidate("Beta", evidence=[copy.deepcopy(shared)]),
                    ],
                )
            ]
        )
        metrics = result["metrics"]
        self.assertEqual(metrics["raw_candidate_evidence_association_rows"], 3)
        self.assertEqual(metrics["valid_candidate_evidence_association_rows"], 3)
        self.assertEqual(metrics["unique_evidence_records"], 1)
        self.assertEqual(metrics["unique_candidate_evidence_associations"], 2)
        self.assertEqual(metrics["duplicate_candidate_evidence_association_rows"], 1)

    def test_self_evidence_cannot_cross_candidate_subjects(self) -> None:
        result = _merge(
            [
                _bundle(
                    "wave_1",
                    [_candidate("Alpha", evidence=[_evidence("Source")])],
                )
            ]
        )
        self.assertEqual(result["metrics"]["raw_candidate_evidence_association_rows"], 1)
        self.assertEqual(result["metrics"]["valid_candidate_evidence_association_rows"], 0)
        self.assertEqual(result["metrics"]["rejected_evidence_association_rows"], 1)
        self.assertEqual(result["candidates"][0]["evidence"], [])

    def test_generic_web_and_impossible_evidence_dates_fail_closed(self) -> None:
        with self.assertRaisesRegex(CampaignValidationError, "wave_model_provenance_invalid"):
            _merge([_bundle("wave_1", [_candidate("Alpha")], generic_web_used=True)])

        impossible_date = _evidence("Alpha")
        impossible_date["published_at"] = "2026-02-31T00:00:00Z"
        with self.assertRaisesRegex(CampaignValidationError, "evidence_published_at_invalid"):
            _merge(
                [
                    _bundle(
                        "wave_1",
                        [_candidate("Alpha", evidence=[impossible_date])],
                    )
                ]
            )

    def test_state_observation_preserves_confidence_and_caveats(self) -> None:
        result = _merge([_bundle("wave_1", [_candidate("Alpha")])])
        observation = result["candidates"][0]["state_observations"][0]
        self.assertEqual(observation["confidence"], "medium")
        self.assertEqual(observation["caveats"], ["Synthetic offline test row."])

    def test_stop_is_call_productivity_based_and_fail_closed_on_context(self) -> None:
        unavailable = _merge(
            [
                _bundle("wave_1", [_candidate("Alpha")], exact_attribution=False),
                _bundle(
                    "wave_2",
                    [_candidate("Beta")],
                    exact_attribution=True,
                    prior_exclusion_status="system_prompt_hash_bound_operator_asserted",
                ),
                _bundle(
                    "wave_3",
                    [],
                    exact_attribution=True,
                    prior_exclusion_status="system_prompt_hash_bound_operator_asserted",
                ),
            ]
        )
        self.assertEqual(unavailable["stop_advisory"]["evaluation_status"], "insufficient_proof")
        self.assertEqual(unavailable["stop_advisory"]["recommendation"], "continue_expansion")
        self.assertFalse(unavailable["stop_advisory"]["candidate_count_triggered"])

        mixed_call_shapes = _merge(
            [
                _bundle("wave_1", [_candidate("Alpha")], call_count=10, exact_attribution=True),
                _bundle("wave_2", [_candidate("Beta")], call_count=9, exact_attribution=True),
                _bundle("wave_3", [], call_count=10, exact_attribution=True),
            ]
        )
        self.assertEqual(
            mixed_call_shapes["stop_advisory"]["evaluation_status"],
            "insufficient_proof",
        )
        self.assertIn(
            "family_call_mix_not_comparable",
            {row["reason"] for row in mixed_call_shapes["stop_advisory"]["excluded_wave_reasons"]},
        )

        fifty_one_fifty = _merge(
            [
                _bundle("wave_1", [_candidate("Alpha")], call_count=50, exact_attribution=True),
                _bundle("wave_2", [_candidate("Beta")], call_count=1, exact_attribution=True),
                _bundle("wave_3", [], call_count=50, exact_attribution=True),
            ]
        )
        self.assertEqual(
            fifty_one_fifty["stop_advisory"]["evaluation_status"],
            "insufficient_proof",
        )
        self.assertEqual(
            fifty_one_fifty["stop_advisory"]["recommendation"],
            "continue_expansion",
        )

        shape_mismatch = _merge(
            [
                _bundle(
                    "wave_1",
                    [_candidate("Alpha")],
                    exact_attribution=True,
                    user_search_count=10,
                    allowed_user_search_counts=(10, 20),
                ),
                _bundle(
                    "wave_2",
                    [_candidate("Beta")],
                    exact_attribution=True,
                    user_search_count=20,
                    allowed_user_search_counts=(10, 20),
                ),
                _bundle(
                    "wave_3",
                    [],
                    exact_attribution=True,
                    user_search_count=10,
                    allowed_user_search_counts=(10, 20),
                ),
            ]
        )
        self.assertEqual(shape_mismatch["stop_advisory"]["evaluation_status"], "insufficient_proof")
        self.assertIn(
            "family_call_profile_not_comparable",
            {row["reason"] for row in shape_mismatch["stop_advisory"]["excluded_wave_reasons"]},
        )

        bundles = [
            _bundle(
                "wave_1",
                [_candidate(f"a{index:02d}", evidence=[]) for index in range(10)],
                call_count=10,
                exact_attribution=True,
            ),
            _bundle(
                "wave_2",
                [_candidate(f"b{index:02d}", evidence=[]) for index in range(2)],
                call_count=10,
                exact_attribution=True,
            ),
            _bundle("wave_3", [], call_count=10, exact_attribution=True),
        ]
        plateau = _merge(bundles)
        self.assertEqual(plateau["stop_advisory"]["evaluation_status"], "evaluated")
        self.assertEqual(plateau["stop_advisory"]["reason"], "configured_call_productivity_plateau")
        self.assertEqual(
            [
                point["new_unique_handles_per_replayed_completed_native_x_call"]
                for point in plateau["marginal_yield"]["wave_points"]
            ],
            [1.0, 0.2, 0.0],
        )

    def test_upstream_target_prompt_schema_and_attribution_cannot_be_relabeled(self) -> None:
        mutations = [
            ("target", lambda upstream: upstream["target"].update({"lab_id": "other"})),
            ("wave", lambda upstream: upstream.update({"wave_id": "other"})),
            ("schema", lambda upstream: upstream.update({"wave_result_schema_version": "other"})),
            ("prompt", lambda upstream: upstream.update({"prompt_sha256": "0" * 64})),
            (
                "strategy_digest",
                lambda upstream: upstream["strategy"].update({"strategy_definition_sha256": "0" * 64}),
            ),
        ]
        for name, mutate in mutations:
            with self.subTest(name=name):
                bundle = _bundle("wave_1", [_candidate("Alpha")])
                upstream = json.loads(bundle["wave"].upstream_request_bytes)
                mutate(upstream)
                upstream_raw = _json_bytes(upstream)
                bundle["wave"] = WaveInput(
                    "wave_1",
                    bundle["wave"].result_bytes,
                    upstream_raw,
                    bundle["wave"].prompt_bytes,
                    bundle["wave"].raw_session_files,
                )
                bundle["binding"]["upstream_request"]["sha256"] = _sha(upstream_raw)
                with self.assertRaises(CampaignValidationError):
                    _merge([bundle])

    def test_persisted_result_requires_full_source_replay(self) -> None:
        bundle = _bundle("wave_1", [_candidate("Alpha")], exact_attribution=True)
        request = {
            "schema_version": "x.recall_pool.campaign.request.v1",
            "campaign_id": "synthetic_campaign",
            "target": {"lab_id": "synthetic_lab", "research_focus_id": "pretraining"},
            "waves": [bundle["binding"]],
        }
        result = merge_campaign(request, _policy(), [bundle["wave"]])
        replay_and_validate_campaign_result(result, request, _policy(), [bundle["wave"]])
        tampered = copy.deepcopy(result)
        tampered["metrics"]["model_reported_observations"] += 1
        with self.assertRaises(CampaignValidationError):
            replay_and_validate_campaign_result(tampered, request, _policy(), [bundle["wave"]])

    def test_large_pool_has_no_business_candidate_cap(self) -> None:
        candidates = [
            _candidate(f"u{index:05d}", platform_user_id=None, bio_excerpt=None, evidence=[]) for index in range(1500)
        ]
        result = _merge([_bundle("wave_1", candidates, call_count=1)])
        self.assertEqual(result["metrics"]["total_unique_handles"], 1500)
        self.assertIsNone(result["authority"]["business_candidate_limit"])
        self.assertFalse(result["stop_advisory"]["candidate_count_triggered"])

    def test_memory_ceiling_is_operational_not_business_semantics(self) -> None:
        policy = _policy()
        self.assertLessEqual(policy["kill_ceilings"]["max_total_input_bytes"], 256 * 1024 * 1024)
        self.assertLessEqual(policy["kill_ceilings"]["max_candidate_rows_in_memory"], 50_000)
        self.assertLessEqual(policy["kill_ceilings"]["max_evidence_rows_in_memory"], 250_000)
        policy["kill_ceilings"]["max_candidate_rows_in_memory"] = 1
        with self.assertRaisesRegex(CampaignValidationError, "max_candidate_rows_kill_ceiling_exceeded"):
            _merge([_bundle("wave_1", [_candidate("Alpha"), _candidate("Beta")])], policy)
        self.assertIsNone(policy["authority"]["business_candidate_limit"])

    def test_aggregate_byte_ceiling_is_preflighted_before_any_content_read(self) -> None:
        bundle = _bundle("wave_1", [_candidate("Alpha")])
        policy = _policy()
        policy["kill_ceilings"]["max_total_input_bytes"] = 1
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            request, manifest, _source = _materialize_bundle_sources(root, bundle)
            with mock.patch("scripts.merge_recall_pool_campaign._read_regular_file") as read_file:
                with self.assertRaisesRegex(
                    CampaignValidationError,
                    "max_total_input_bytes_kill_ceiling_exceeded",
                ):
                    _load_sources(manifest, request, policy)
                read_file.assert_not_called()

    def test_preflight_size_is_the_maximum_allowed_during_content_read(self) -> None:
        bundle = _bundle("wave_1", [_candidate("Alpha")])
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            request, manifest, source = _materialize_bundle_sources(root, bundle)
            preflight_calls = 0
            mutate_after_call = 3 + len(bundle["wave"].raw_session_files)

            def preflight_then_grow(*args: Any, **kwargs: Any) -> int:
                nonlocal preflight_calls
                size = _regular_file_size(*args, **kwargs)
                preflight_calls += 1
                if preflight_calls == mutate_after_call:
                    with source.open("ab") as stream:
                        stream.write(b"x")
                return size

            with mock.patch(
                "scripts.merge_recall_pool_campaign._regular_file_size",
                side_effect=preflight_then_grow,
            ):
                with self.assertRaisesRegex(ValueError, "input_file_invalid"):
                    _load_sources(manifest, request, _policy())

    def test_all_generated_contracts_execute_and_mutations_fail(self) -> None:
        bundle = _bundle("wave_1", [_candidate("Alpha")], exact_attribution=True)
        result = _merge([bundle])
        for filename in CONTRACT_SCHEMA_FILES:
            schema = json.loads((ROOT / "contracts" / filename).read_text())
            self.assertEqual(schema["$schema"], "https://json-schema.org/draft/2020-12/schema")
            self.assertIs(schema["additionalProperties"], False)
        assert_schema_valid(result, "x.recall_pool.campaign.result.v1.schema.json")
        mutated = copy.deepcopy(result)
        mutated["authority"]["product_write_authorized"] = True
        with self.assertRaises(MiniDraft202012Error):
            assert_schema_valid(mutated, "x.recall_pool.campaign.result.v1.schema.json")
        mutated = copy.deepcopy(result)
        mutated["candidates"][0]["state_summary"]["pretraining_experience_state"]["evidence_supported_resolution"] = (
            "historical"
        )
        with self.assertRaises(CampaignValidationError):
            validate_campaign_result(mutated)
        mutated = copy.deepcopy(result)
        mutated["wave_yields"][0]["model_reported"]["tools_reported"] = []
        with self.assertRaisesRegex(
            CampaignValidationError,
            "campaign_result_model_native_tool_set_conflict",
        ):
            validate_campaign_result(mutated)

    def test_v2_typed_post_reply_evidence_and_mechanical_surface_coverage(self) -> None:
        calls = [
            _native_x_call(
                "wave_v2",
                0,
                tool_name="x_keyword_search",
                arguments={"query": "from:Alpha -filter:replies pretraining", "limit": "25", "mode": "Latest"},
            ),
            _native_x_call(
                "wave_v2",
                1,
                tool_name="x_keyword_search",
                arguments={"query": "from:alpha filter:replies tokenizer", "limit": "25", "mode": "Latest"},
            ),
            _native_x_call(
                "wave_v2",
                2,
                tool_name="x_semantic_search",
                arguments={"query": "OpenAI pretraining researchers", "limit": "25"},
            ),
            _native_x_call(
                "wave_v2",
                3,
                tool_name="x_keyword_search",
                arguments={"query": "from:NotACandidate pretraining", "limit": "25", "mode": "Top"},
            ),
            _native_x_call(
                "wave_v2",
                4,
                tool_name="x_semantic_search",
                arguments={"query": "from:Alpha filter:replies tokenizer", "limit": "25"},
            ),
        ]
        candidate = _v2_candidate(
            "Alpha",
            evidence=[
                _v2_evidence("Alpha", post_id="1001", asserted_value="historical"),
                _v2_evidence(
                    "Alpha",
                    post_id="1002",
                    asserted_value="current",
                    thread_relation="reply",
                    excerpt="I work on both pretraining and inference-time algorithms.",
                ),
            ],
        )
        result = _merge(
            [
                _bundle(
                    "wave_v2",
                    [candidate],
                    calls_override=calls,
                    wave_result_v2=True,
                )
            ]
        )

        self.assertEqual(result["schema_version"], "x.recall_pool.campaign.result.v2")
        merged = result["candidates"][0]
        pretrain = merged["state_summary"]["pretraining_experience_state"]
        self.assertEqual(pretrain["model_evidence_proposed_values"], ["current", "historical"])
        self.assertEqual(pretrain["model_evidence_proposed_resolution"], "conflict")
        self.assertEqual(pretrain["model_evidence_proposal_status"], "model_mediated_unverified")
        self.assertEqual(pretrain["evidence_supported_values"], [])
        self.assertIsNone(pretrain["evidence_supported_resolution"])
        self.assertTrue(merged["model_evidence_proposed_state_conflict"])
        self.assertEqual(merged["surface_coverage"], "both")
        self.assertEqual(len(merged["surface_attempts"]), 2)
        self.assertEqual(
            {row["thread_relation"] for row in merged["evidence"]},
            {"self_post", "reply"},
        )
        self.assertTrue(
            all(
                row["observed_wave_result_schema_versions"]
                == ["x.grok_cli.recall_wave.result_adapter.v2"]
                for row in merged["evidence"]
            )
        )
        receipt = result["wave_yields"][0]["mechanically_observed"]["receipt"]
        self.assertEqual(len(receipt["candidate_surface_attempts"]), 2)
        self.assertEqual(receipt["candidate_surface_coverage"][0]["handle_key"], "alpha")
        self.assertTrue(receipt["candidate_surface_coverage"][0]["authored_post"]["attempted"])
        self.assertTrue(receipt["candidate_surface_coverage"][0]["authored_reply"]["attempted"])
        validate_campaign_result(result)

        mutated = copy.deepcopy(result)
        mutated["candidates"][0]["surface_coverage"] = "authored_post_only"
        with self.assertRaisesRegex(CampaignValidationError, "candidate_surface_merge"):
            validate_campaign_result(mutated)
        mutated = copy.deepcopy(result)
        mutated["candidates"][0]["state_summary"]["pretraining_experience_state"][
            "model_evidence_proposed_resolution"
        ] = "historical"
        with self.assertRaisesRegex(CampaignValidationError, "state_summary"):
            validate_campaign_result(mutated)

    def test_v2_campaign_preserves_legacy_v1_evidence_without_inventing_topology_or_temporality(self) -> None:
        legacy = _bundle("wave_legacy", [_candidate("Legacy")])
        typed = _bundle(
            "wave_typed",
            [
                _v2_candidate(
                    "Typed",
                    evidence=[
                        _v2_evidence(
                            "Typed",
                            post_id="4001",
                            asserted_value="historical",
                        )
                    ],
                )
            ],
            wave_result_v2=True,
        )
        result = _merge([legacy, typed])

        self.assertEqual(result["schema_version"], "x.recall_pool.campaign.result.v2")
        by_handle = {candidate["handle_key"]: candidate for candidate in result["candidates"]}
        legacy_evidence = by_handle["legacy"]["evidence"][0]
        self.assertIsNone(legacy_evidence["thread_relation"])
        self.assertEqual(
            legacy_evidence["observed_wave_result_schema_versions"],
            ["x.grok_cli.recall_wave.result_adapter.v1"],
        )
        self.assertEqual(legacy_evidence["support_claims"][0]["asserted_value"], None)
        legacy_proposal = by_handle["legacy"]["state_summary"]["pretraining_experience_state"]
        self.assertEqual(legacy_proposal["model_evidence_proposed_values"], [])
        self.assertIsNone(legacy_proposal["model_evidence_proposed_resolution"])
        validate_campaign_result(result)

        relabeled = copy.deepcopy(result)
        relabeled["candidates"][0]["evidence"][0]["observed_wave_result_schema_versions"] = [
            "x.grok_cli.recall_wave.result_adapter.v2"
        ]
        with self.assertRaisesRegex(CampaignValidationError, "adapter_versions"):
            validate_campaign_result(relabeled)

    def test_v2_rejects_untyped_support_and_missing_nonbio_thread_relation(self) -> None:
        untyped = _v2_evidence("Alpha", post_id="2001", asserted_value="historical")
        untyped["supports"] = ["pretraining_experience_state"]
        with self.assertRaisesRegex(CampaignValidationError, "evidence_supports_invalid"):
            _merge([_bundle("wave_untyped", [_v2_candidate("Alpha", evidence=[untyped])], wave_result_v2=True)])

        missing_relation = _v2_evidence(
            "Alpha",
            post_id="2002",
            asserted_value="historical",
            thread_relation=None,
        )
        with self.assertRaisesRegex(CampaignValidationError, "evidence_post_shape_invalid"):
            _merge(
                [
                    _bundle(
                        "wave_relation",
                        [_v2_candidate("Alpha", evidence=[missing_relation])],
                        wave_result_v2=True,
                    )
                ]
            )

    def test_wave_request_and_result_adapter_versions_are_exact_pairs(self) -> None:
        for wave_result_v2, wrong_result_version in (
            (False, "x.grok_cli.recall_wave.result_adapter.v2"),
            (True, "x.grok_cli.recall_wave.result_adapter.v1"),
        ):
            with self.subTest(wave_result_v2=wave_result_v2):
                wave_id = "wave_pair_v2" if wave_result_v2 else "wave_pair_v1"
                candidate = (
                    _v2_candidate(
                        "Alpha",
                        evidence=[_v2_evidence("Alpha", post_id="3001", asserted_value="historical")],
                    )
                    if wave_result_v2
                    else _candidate("Alpha")
                )
                bundle = _bundle(
                    wave_id,
                    [candidate],
                    wave_result_v2=wave_result_v2,
                )
                upstream = json.loads(bundle["wave"].upstream_request_bytes)
                upstream["wave_result_schema_version"] = wrong_result_version
                upstream_bytes = _json_bytes(upstream)
                bundle["wave"] = WaveInput(
                    bundle["wave"].wave_id,
                    bundle["wave"].result_bytes,
                    upstream_bytes,
                    bundle["wave"].prompt_bytes,
                    bundle["wave"].raw_session_files,
                )
                bundle["binding"]["upstream_request"]["sha256"] = _sha(upstream_bytes)
                with self.assertRaisesRegex(CampaignValidationError, "wave_request_version_pair_invalid"):
                    _merge([bundle])

    def test_cli_atomic_publish_orphan_cleanup_no_replace_and_replay(self) -> None:
        bundle = _bundle("wave_1", [_candidate("Alpha")], exact_attribution=True)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            os.chmod(root, 0o700)
            binding = bundle["binding"]
            source = root / binding["source_path"]
            source.write_bytes(bundle["wave"].result_bytes)
            os.chmod(source, 0o600)
            upstream = root / binding["upstream_request"]["path"]
            upstream.write_bytes(bundle["wave"].upstream_request_bytes)
            os.chmod(upstream, 0o600)
            prompt = root / binding["prompt"]["path"]
            prompt.write_bytes(bundle["wave"].prompt_bytes)
            os.chmod(prompt, 0o600)
            session = root / binding["raw_session_directory"]
            session.mkdir(mode=0o700)
            for name, raw in bundle["wave"].raw_session_files.items():
                path = session / name
                path.write_bytes(raw)
                os.chmod(path, 0o600)
            request = {
                "schema_version": "x.recall_pool.campaign.request.v1",
                "campaign_id": "synthetic_campaign",
                "target": {"lab_id": "synthetic_lab", "research_focus_id": "pretraining"},
                "waves": [binding],
            }
            manifest = root / "manifest.json"
            manifest.write_bytes(_json_bytes(request))
            os.chmod(manifest, 0o600)
            output = root / "result.json"
            orphan = root / f".result.json.tmp.{'0' * 32}"
            orphan.write_bytes(b"stale")
            os.chmod(orphan, 0o600)
            command = [
                sys.executable,
                str(ROOT / "scripts/merge_recall_pool_campaign.py"),
                "--manifest",
                str(manifest),
                "--output",
                str(output),
            ]
            completed = subprocess.run(command, cwd=ROOT, capture_output=True, text=True, check=False)
            self.assertEqual(completed.returncode, 0, completed.stdout + completed.stderr)
            self.assertFalse(orphan.exists())
            self.assertEqual(stat.S_IMODE(output.stat().st_mode), 0o600)
            before = _sha(output.read_bytes())
            second = subprocess.run(command, cwd=ROOT, capture_output=True, text=True, check=False)
            self.assertEqual(second.returncode, 1)
            self.assertEqual(_sha(output.read_bytes()), before)
            self.assertEqual(list(root.glob(".result.json.tmp.*")), [])
            validate = command[:-2] + ["--validate-existing", str(output)]
            validated = subprocess.run(validate, cwd=ROOT, capture_output=True, text=True, check=False)
            self.assertEqual(validated.returncode, 0, validated.stdout + validated.stderr)
            self.assertEqual(json.loads(validated.stdout)["status"], "validated")

            cross_destination_orphan = root / f".a.tmp.{'1' * 32}"
            cross_destination_orphan.write_bytes(b"must remain")
            os.chmod(cross_destination_orphan, 0o600)
            _publish_private_no_replace(root / "[a]", b"literal destination")
            self.assertTrue(cross_destination_orphan.exists())

    def test_cli_rejects_non_private_pii_sources_and_every_public_prompt(self) -> None:
        bundle = _bundle("wave_1", [_candidate("Alpha")], exact_attribution=True)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            os.chmod(root, 0o700)
            binding = bundle["binding"]
            source = root / binding["source_path"]
            source.write_bytes(bundle["wave"].result_bytes)
            upstream = root / binding["upstream_request"]["path"]
            upstream.write_bytes(bundle["wave"].upstream_request_bytes)
            prompt = root / binding["prompt"]["path"]
            prompt.write_bytes(bundle["wave"].prompt_bytes)
            for private_file in (source, upstream):
                os.chmod(private_file, 0o600)
            os.chmod(prompt, 0o600)
            session = root / binding["raw_session_directory"]
            session.mkdir(mode=0o700)
            for name, raw in bundle["wave"].raw_session_files.items():
                path = session / name
                path.write_bytes(raw)
                os.chmod(path, 0o600)
            request = {
                "schema_version": "x.recall_pool.campaign.request.v1",
                "campaign_id": "synthetic_campaign",
                "target": {"lab_id": "synthetic_lab", "research_focus_id": "pretraining"},
                "waves": [binding],
            }
            manifest = root / "manifest.json"
            manifest.write_bytes(_json_bytes(request))
            os.chmod(manifest, 0o600)

            def run(output_name: str) -> subprocess.CompletedProcess[str]:
                return subprocess.run(
                    [
                        sys.executable,
                        str(ROOT / "scripts/merge_recall_pool_campaign.py"),
                        "--manifest",
                        str(manifest),
                        "--output",
                        str(root / output_name),
                    ],
                    cwd=ROOT,
                    capture_output=True,
                    text=True,
                    check=False,
                )

            os.chmod(source, 0o644)
            self.assertEqual(run("mode-failed.json").returncode, 1)
            os.chmod(source, 0o600)
            raw_summary = session / "summary.json"
            os.chmod(raw_summary, 0o644)
            self.assertEqual(run("raw-mode-failed.json").returncode, 1)
            os.chmod(raw_summary, 0o600)
            os.chmod(session, 0o755)
            self.assertEqual(run("raw-dir-mode-failed.json").returncode, 1)
            os.chmod(session, 0o700)
            original_prompt = prompt.read_bytes()
            public_cases = {
                "generic": original_prompt,
                "bare-handle": original_prompt + b"ExampleHandle\n",
                "bio": original_prompt + b"Bio: synthetic researcher profile text\n",
            }
            for label, public_prompt in public_cases.items():
                with self.subTest(public_prompt=label):
                    prompt.write_bytes(public_prompt)
                    os.chmod(prompt, 0o644)
                    binding["prompt"]["sha256"] = _sha(public_prompt)
                    manifest.write_bytes(_json_bytes(request))
                    os.chmod(manifest, 0o600)
                    self.assertEqual(run(f"prompt-{label}-failed.json").returncode, 1)

            policy = _policy()
            policy["public_prompt_sha256_allowlist"] = [_sha(original_prompt)]
            with self.assertRaisesRegex(
                CampaignValidationError,
                "campaign_public_prompt_allowlist_invalid",
            ):
                _merge([_bundle("wave_2", [_candidate("Beta")])], policy)


if __name__ == "__main__":
    unittest.main()
