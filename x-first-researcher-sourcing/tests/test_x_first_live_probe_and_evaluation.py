from __future__ import annotations

import copy
import hashlib
import json
import stat
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

import x_first.live_probe as live_probe  # noqa: E402
from x_first.live_probe import (  # noqa: E402
    GLOBAL_APPROVAL_OWNER_ID,
    LIVE_RESULT_SCHEMA_VERSION,
    PINNED_GROK_BINARY_SHA256,
    BoundedCommandResult,
    ProviderUsageReceipt,
    RawXPostReceipt,
    ToolCallReceipt,
    ToolProof,
    _run_bounded_command,
    build_live_request,
    build_live_result,
    extract_tool_proof,
    purge_expired_live_artifacts,
    run_live_probe,
    validate_artifact_pair,
    validate_live_request,
    validate_live_result,
)

SESSION_ID = "01234567-89ab-4cde-8fab-0123456789ab"
POST_ID = "1900000000000000001"
USER_ID = "4398626122"
POST_URL = f"https://x.com/OpenAI/status/{POST_ID}"
RUN_ID = "xprobe_run_0123456789abcdef0123456789abcdef"
STARTED_AT = "2026-07-14T09:00:00.000Z"
COMPLETED_AT = "2026-07-14T09:00:01.000Z"


def _inner_response(*, stable_user_id: str | None = USER_ID) -> dict[str, object]:
    return {
        "reported_verdict": "x_native_candidate",
        "access_mode": "x_search",
        "target": {
            "lab_id": "openai",
            "author_handle": "OpenAI",
            "platform_user_id": stable_user_id,
        },
        "observations": [
            {
                "platform_object_id": POST_ID,
                "platform_user_id": stable_user_id,
                "author_handle": "OpenAI",
                "canonical_url": POST_URL,
                "authored_at": "2026-07-13T08:00:00Z",
                "excerpt": "Technical model-training update from the official lab account.",
                "full_body_stored": False,
            }
        ],
        "errors": [],
    }


def _terminal_usage(*, model_turns: int = 2) -> dict[str, object]:
    per_model = {
        "inputTokens": 100,
        "outputTokens": 50,
        "totalTokens": 150,
        "cachedReadTokens": 20,
        "reasoningTokens": 10,
        "modelCalls": 2,
        "apiDurationMs": 250,
    }
    return {**per_model, "modelUsage": {"grok-4.5": per_model}, "numTurns": model_turns}


def _usage_receipt(*, model_turns: int = 2) -> ProviderUsageReceipt:
    return ProviderUsageReceipt(
        input_tokens=100,
        output_tokens=50,
        total_tokens=150,
        cached_read_tokens=20,
        reasoning_tokens=10,
        model_calls=2,
        api_duration_ms=250,
        model_turns=model_turns,
    )


def _outer_response(
    inner: dict[str, object],
    *,
    session_id: str = SESSION_ID,
    stop_reason: str = "EndTurn",
    model_turns: int = 2,
    cost: float | None = 0.01,
) -> dict[str, object]:
    outer: dict[str, object] = {
        "text": json.dumps(inner),
        "stopReason": stop_reason,
        "sessionId": session_id,
        "requestId": "provider-request",
        "num_turns": model_turns,
        "usage": {"input_tokens": 100, "output_tokens": 50, "total_tokens": 150},
    }
    if cost is not None:
        outer["total_cost_usd"] = cost
    return outer


def _raw_x_output(*, stable_user_id: str | None = USER_ID) -> dict[str, object]:
    post: dict[str, object] = {"id": POST_ID, "canonical_url": POST_URL}
    if stable_user_id is not None:
        post["author_info"] = {
            "legacy": {"screen_name": "OpenAI"},
            "rest_id": stable_user_id,
        }
    return {"posts": [post]}


def _unbound_raw_x_output() -> dict[str, object]:
    return {"posts": [{"id": POST_ID, "canonical_url": POST_URL}]}


def _event(
    update: dict[str, object],
    *,
    session_id: str = SESSION_ID,
    terminal: bool = False,
    timestamp: int = 1,
) -> dict[str, object]:
    return {
        "method": "_x.ai/session/update" if terminal else "session/update",
        "params": {
            "_meta": {"agentTimestampMs": timestamp, "eventId": f"{session_id}-{timestamp}"},
            "sessionId": session_id,
            "update": update,
        },
        "timestamp": timestamp,
    }


def _tool_call(call_id: str, *, tool_name: str = "x_search", session_id: str = SESSION_ID) -> dict[str, object]:
    return _event(
        {
            "_meta": {"x.ai/tool": {"name": tool_name}},
            "sessionUpdate": "tool_call",
            "title": tool_name,
            "toolCallId": call_id,
        },
        session_id=session_id,
        timestamp=2,
    )


def _tool_result(
    call_id: str,
    *,
    raw_output: object,
    session_id: str = SESSION_ID,
) -> dict[str, object]:
    return _event(
        {
            "content": [],
            "rawOutput": raw_output,
            "sessionUpdate": "tool_call_update",
            "status": "completed",
            "toolCallId": call_id,
        },
        session_id=session_id,
        timestamp=3,
    )


def _terminal_event(
    *,
    session_id: str = SESSION_ID,
    stop_reason: str = "end_turn",
    model_turns: int = 2,
) -> dict[str, object]:
    return _event(
        {
            "prompt_id": "prompt-1",
            "sessionUpdate": "turn_completed",
            "stop_reason": stop_reason,
            "usage": _terminal_usage(model_turns=model_turns),
        },
        session_id=session_id,
        terminal=True,
        timestamp=4,
    )


def _session_events(
    *,
    session_id: str = SESSION_ID,
    raw_output: object | None = None,
    tool_name: str = "x_search",
    call_ids: tuple[str, ...] = ("x-call",),
    terminal_stop_reason: str = "end_turn",
    model_turns: int = 2,
) -> list[dict[str, object]]:
    events = [
        _event(
            {
                "_meta": {"modelId": "grok-4.5", "promptIndex": 0},
                "content": {},
                "sessionUpdate": "user_message_chunk",
            },
            session_id=session_id,
            timestamp=1,
        )
    ]
    for index, call_id in enumerate(call_ids):
        events.append(_tool_call(call_id, tool_name=tool_name, session_id=session_id))
        if raw_output is not None:
            events.append(_tool_result(call_id, raw_output=raw_output, session_id=session_id))
        events[-1]["timestamp"] = 2 + index  # type: ignore[index]
    events.append(_terminal_event(session_id=session_id, stop_reason=terminal_stop_reason, model_turns=model_turns))
    return events


def _write_events(path: Path, events: list[dict[str, object]]) -> None:
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    path.write_text("\n".join(json.dumps(event) for event in events) + "\n", encoding="utf-8")


def _proof(
    *,
    x_calls: int = 1,
    completed: int = 1,
    unexpected: tuple[str, ...] = (),
    stable_user_id: str | None = USER_ID,
    post_id: str = POST_ID,
) -> ToolProof:
    post_url = f"https://x.com/OpenAI/status/{post_id}"
    posts = (RawXPostReceipt(post_id, post_url, stable_user_id),) if x_calls else ()
    return ToolProof(
        session_id=SESSION_ID,
        x_search_calls=x_calls,
        x_search_completed_calls=completed,
        unexpected_tool_calls=unexpected,
        raw_result_posts=posts,
        observed_model_ids=("grok-4.5",),
        call_receipts=(
            ToolCallReceipt(
                call_id="x-call",
                tool_id="x_search",
                statuses=("completed", "in_progress") if completed else ("in_progress",),
                raw_result_posts=posts,
            ),
        )
        if x_calls
        else (),
        updates_sha256="a" * 64,
        update_bytes=120,
        terminal_stop_reason="end_turn",
        terminal_usage=_usage_receipt(),
        evidence_errors=(),
    )


def _approval_receipt(request: dict[str, object], *, run_id: str = RUN_ID) -> dict[str, object]:
    return {
        "schema_version": live_probe.APPROVAL_RECEIPT_SCHEMA_VERSION,
        "owner_id": GLOBAL_APPROVAL_OWNER_ID,
        "probe_id": request["probe_id"],
        "request_sha256": live_probe.canonical_sha256(request),
        "run_id": run_id,
        "consumed_at": STARTED_AT,
        "binary_sha256": PINNED_GROK_BINARY_SHA256,
        "state": "consumed_before_spawn",
    }


def _write_valid_bundle(
    root: Path,
    approval_root: Path,
    *,
    run_id: str = RUN_ID,
) -> tuple[Path, dict[str, object], dict[str, object], dict[str, object]]:
    request = build_live_request()
    proof = _proof()
    outer = _outer_response(_inner_response())
    approval = _approval_receipt(request, run_id=run_id)
    tool_receipt = live_probe._build_tool_receipt(proof=proof, session_id=SESSION_ID, outer=outer)
    result = build_live_result(
        request=request,
        run_id=run_id,
        session_id=SESSION_ID,
        started_at=STARTED_AT,
        completed_at=COMPLETED_AT,
        elapsed_ms=1000,
        outer=outer,
        inner=_inner_response(),
        proof=proof,
        approval_receipt_sha256=live_probe.canonical_sha256(approval),
        grok_binary_sha256=PINNED_GROK_BINARY_SHA256,
        tool_receipt_sha256=live_probe.canonical_sha256(tool_receipt),
    )
    runtime_root = root / "runtime/live-probes"
    runtime_root.mkdir(mode=0o700, parents=True)
    bundle = runtime_root / run_id
    bundle.mkdir(mode=0o700)
    for name, payload in (
        ("request.json", request),
        ("result.json", result),
        ("approval-receipt.json", approval),
        ("tool-receipt.json", tool_receipt),
    ):
        live_probe._atomic_write_json(bundle / name, payload)
    approval_root.mkdir(mode=0o700, parents=True)
    live_probe._atomic_write_json(approval_root / f"{request['probe_id']}.json", approval)
    return bundle, result, approval, tool_receipt


class XFirstLiveCapabilityContractTest(unittest.TestCase):
    def setUp(self) -> None:
        self.request = build_live_request()

    def _run_bounded_provider_events(
        self,
        events_factory: object,
        *,
        inner_mutator: object | None = None,
        outer_mutator: object | None = None,
    ) -> tuple[dict[str, object], dict[str, object], Path]:
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        root = Path(temporary.name)
        approval_root = root / "global-approval"
        binary = root / "grok"
        binary.write_text("#!/bin/sh\nexit 1\n", encoding="utf-8")
        binary.chmod(stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR)
        auth = root / "auth.json"
        auth.write_text('{"private":"credential-material"}', encoding="utf-8")
        auth.chmod(0o600)
        binary_digest = hashlib.sha256(binary.read_bytes()).hexdigest()

        def fake_run(command: list[str], **kwargs: object) -> BoundedCommandResult:
            session_id = command[command.index("--session-id") + 1]
            updates = Path(kwargs["updates_path"])
            events = events_factory(session_id)  # type: ignore[operator]
            _write_events(updates, events)
            invalid_inner = {**_inner_response(), "unexpected": "field"}
            if inner_mutator is not None:
                inner_mutator(invalid_inner)  # type: ignore[operator]
            outer = _outer_response(invalid_inner, session_id=session_id)
            if outer_mutator is not None:
                outer_mutator(outer)  # type: ignore[operator]
            return BoundedCommandResult(
                returncode=0,
                stdout=json.dumps(outer).encode(),
                stderr=b"",
                stop_reason=None,
            )

        with (
            mock.patch("x_first.live_probe._run_bounded_command", side_effect=fake_run),
            mock.patch("x_first.live_probe.project_root", return_value=root),
            mock.patch("x_first.live_probe._global_approval_root", return_value=approval_root),
            mock.patch("x_first.live_probe.PINNED_GROK_BINARY_SHA256", binary_digest),
        ):
            result, artifact_root = run_live_probe(execute_live=True, grok_binary=binary, auth_path=auth)
            self.assertEqual(
                validate_artifact_pair(artifact_root / "request.json", artifact_root / "result.json"),
                [],
            )
        tool_receipt = json.loads((artifact_root / "tool-receipt.json").read_text(encoding="utf-8"))
        runtime_root = root / "runtime/live-probes"
        self.assertEqual(len(list(runtime_root.glob("xprobe_run_*"))), 1)
        self.assertEqual(list(runtime_root.glob(".*.tmp")), [])
        return result, tool_receipt, artifact_root

    def test_live_request_and_declarative_contracts_are_closed(self) -> None:
        self.assertEqual(validate_live_request(self.request), [])
        request_schema = json.loads(
            (ROOT / "contracts/x.grok.capability_probe.request.v2.schema.json").read_text(encoding="utf-8")
        )
        result_schema = json.loads(
            (ROOT / "contracts/x.grok.capability_probe.result.v2.schema.json").read_text(encoding="utf-8")
        )
        approval_schema = json.loads(
            (ROOT / "contracts/x.grok.live_approval_consumption.v1.schema.json").read_text(encoding="utf-8")
        )
        tool_schema = json.loads(
            (ROOT / "contracts/x.grok.x_search_tool_receipt.v1.schema.json").read_text(encoding="utf-8")
        )
        self.assertEqual(set(request_schema["required"]), set(self.request))
        self.assertEqual(result_schema["properties"]["schema_version"]["const"], LIVE_RESULT_SCHEMA_VERSION)
        self.assertEqual(result_schema["$defs"]["usage"]["properties"]["elapsed_ms"]["maximum"], 200000)
        self.assertEqual(result_schema["$defs"]["usage"]["properties"]["model_turns"]["maximum"], 8)
        success_usage = result_schema["allOf"][0]["then"]["properties"]["usage"]["properties"]
        self.assertEqual(success_usage["model_turns"]["maximum"], 4)
        self.assertEqual(success_usage["elapsed_ms"]["maximum"], 180000)
        self.assertEqual(
            result_schema["allOf"][0]["then"]["properties"]["provenance"]["properties"]["provider_request_id"]["type"],
            "string",
        )
        self.assertEqual(
            result_schema["$defs"]["observation"]["properties"]["excerpt"]["pattern"],
            "^[^\\uD800-\\uDFFF]+$",
        )
        self.assertEqual(
            live_probe.GROK_RESPONSE_SCHEMA["properties"]["observations"]["items"]["properties"]["excerpt"]["pattern"],
            r"^[^\ud800-\udfff]+$",
        )
        self.assertNotIn("maximum", result_schema["$defs"]["usage"]["properties"]["cost_usd"])
        receipt_binding = result_schema["allOf"][1]
        self.assertEqual(
            receipt_binding["then"]["properties"]["provenance"]["properties"]["session_id"]["type"],
            "string",
        )
        self.assertEqual(
            receipt_binding["else"]["properties"]["provenance"]["properties"]["session_updates_sha256"]["type"],
            "null",
        )
        self.assertIn("owner_id", approval_schema["required"])
        self.assertEqual(approval_schema["properties"]["owner_id"]["const"], GLOBAL_APPROVAL_OWNER_ID)
        self.assertEqual(
            set(tool_schema["required"]),
            set(
                live_probe._build_tool_receipt(
                    proof=_proof(),
                    session_id=SESSION_ID,
                    outer=_outer_response(_inner_response()),
                )
            ),
        )
        self.assertEqual(tool_schema["properties"]["outer_model_turns"]["maximum"], 8)
        self.assertEqual(
            tool_schema["properties"]["terminal_usage"]["oneOf"][1]["properties"]["model_turns"]["maximum"],
            8,
        )
        self.assertEqual(tool_schema["properties"]["session_updates_sha256"]["type"], ["string", "null"])
        self.assertEqual(tool_schema["properties"]["session_update_bytes"]["minimum"], 0)
        outer_only_receipt = live_probe._build_tool_receipt(
            proof=None,
            session_id=SESSION_ID,
            outer=_outer_response(_inner_response()),
        )
        self.assertIsNone(outer_only_receipt["session_updates_sha256"])
        self.assertEqual(outer_only_receipt["session_update_bytes"], 0)
        self.assertEqual(outer_only_receipt["calls"], [])
        stage1_contract = (ROOT / "docs/STAGE1_LIVE_CAPABILITY_CONTRACT.md").read_text(encoding="utf-8")
        transport_decision = (ROOT / "docs/X_SEARCH_TRANSPORT_AND_SCALE_DECISION.md").read_text(encoding="utf-8")
        self.assertIn("bounded stdio/process-group monitor", stage1_contract)
        self.assertNotIn("process-tree monitor", stage1_contract)
        self.assertIn("It does not\nprobe or make any claim about profile/Bio availability", transport_decision)
        self.assertNotIn("- profile/Bio field availability;", transport_decision)
        for mutate in (
            lambda value: value["hard_budgets"].update(max_x_search_calls=2),
            lambda value: value["claims"].update(researcher_mapping_authorized=True),
            lambda value: value["owner_decisions"].update(retention_policy="unbounded"),
            lambda value: value.update(extra="field"),
        ):
            mutated = copy.deepcopy(self.request)
            mutate(mutated)
            self.assertTrue(validate_live_request(mutated))

    def test_tool_proof_requires_real_grok_0_2_99_envelope_and_terminal(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "updates.jsonl"
            _write_events(path, _session_events(raw_output=_raw_x_output()))
            proof = extract_tool_proof(path, expected_session_id=SESSION_ID)
            self.assertEqual(proof.x_search_calls, 1)
            self.assertEqual(proof.x_search_completed_calls, 1)
            self.assertEqual(proof.unexpected_tool_calls, ())
            self.assertEqual(proof.raw_result_post_pairs, ((POST_ID, POST_URL),))
            self.assertEqual(proof.raw_result_author_user_ids, (USER_ID,))
            self.assertEqual(proof.terminal_stop_reason, "end_turn")
            self.assertEqual(proof.terminal_usage, _usage_receipt())

            naked = Path(directory) / "naked.jsonl"
            naked.write_text(json.dumps({"sessionUpdate": "tool_call", "toolCallId": "x"}) + "\n")
            with self.assertRaises(ValueError):
                extract_tool_proof(naked, expected_session_id=SESSION_ID)

            wrong_session = Path(directory) / "wrong.jsonl"
            _write_events(wrong_session, _session_events(session_id="11234567-89ab-4cde-8fab-0123456789ab"))
            with self.assertRaises(ValueError):
                extract_tool_proof(wrong_session, expected_session_id=SESSION_ID)

            wrong_event = Path(directory) / "wrong-event.jsonl"
            wrong_event_values = _session_events(raw_output=_raw_x_output())
            wrong_event_values[0]["params"]["_meta"]["eventId"] = "unbound-event"  # type: ignore[index]
            _write_events(wrong_event, wrong_event_values)
            with self.assertRaises(ValueError):
                extract_tool_proof(wrong_event, expected_session_id=SESSION_ID)

            max_turns = Path(directory) / "max-turns.jsonl"
            _write_events(max_turns, _session_events(raw_output=_raw_x_output(), terminal_stop_reason="max_turns"))
            with self.assertRaises(ValueError):
                extract_tool_proof(max_turns, expected_session_id=SESSION_ID)

    def test_raw_post_proof_requires_a_structured_colocated_matching_id(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            diagnostic = root / "diagnostic.jsonl"
            _write_events(
                diagnostic,
                _session_events(
                    raw_output={"diagnostic": f"No post record was returned; requested URL was {POST_URL}"}
                ),
            )
            diagnostic_proof = extract_tool_proof(diagnostic, expected_session_id=SESSION_ID)
            self.assertEqual(diagnostic_proof.raw_result_post_pairs, ())
            self.assertIn("invalid_raw_output_shape", diagnostic_proof.unexpected_tool_calls)
            result = build_live_result(
                request=self.request,
                run_id=RUN_ID,
                session_id=SESSION_ID,
                started_at=STARTED_AT,
                completed_at=COMPLETED_AT,
                elapsed_ms=1000,
                outer=_outer_response(_inner_response(stable_user_id=None)),
                inner=_inner_response(stable_user_id=None),
                proof=diagnostic_proof,
                approval_receipt_sha256="b" * 64,
                grok_binary_sha256=PINNED_GROK_BINARY_SHA256,
                tool_receipt_sha256="c" * 64,
            )
            self.assertEqual(result["capability"]["verdict"], "capability_unavailable")

            mismatched = root / "mismatched.jsonl"
            _write_events(
                mismatched,
                _session_events(raw_output={"posts": [{"id": "1900000000000000002", "canonical_url": POST_URL}]}),
            )
            mismatched_proof = extract_tool_proof(mismatched, expected_session_id=SESSION_ID)
            self.assertEqual(mismatched_proof.raw_result_post_pairs, ())
            self.assertIn("invalid_raw_post_record_binding", mismatched_proof.unexpected_tool_calls)

            conflicting_ids = root / "conflicting-ids.jsonl"
            _write_events(
                conflicting_ids,
                _session_events(
                    raw_output={
                        "posts": [
                            {
                                "id": POST_ID,
                                "id_str": "1900000000000000002",
                                "canonical_url": POST_URL,
                            }
                        ]
                    }
                ),
            )
            conflicting_proof = extract_tool_proof(conflicting_ids, expected_session_id=SESSION_ID)
            self.assertEqual(conflicting_proof.raw_result_post_pairs, ())
            self.assertIn("invalid_raw_post_record_binding", conflicting_proof.unexpected_tool_calls)

            structured_diagnostic = root / "structured-diagnostic.jsonl"
            _write_events(
                structured_diagnostic,
                _session_events(
                    raw_output={
                        "diagnostic": {
                            "kind": "request_echo",
                            "id": POST_ID,
                            "canonical_url": POST_URL,
                            "author_info": {
                                "legacy": {"screen_name": "OpenAI"},
                                "rest_id": USER_ID,
                            },
                        }
                    }
                ),
            )
            structured_diagnostic_proof = extract_tool_proof(
                structured_diagnostic,
                expected_session_id=SESSION_ID,
            )
            self.assertEqual(structured_diagnostic_proof.raw_result_post_pairs, ())
            self.assertEqual(structured_diagnostic_proof.raw_result_author_user_ids, ())
            self.assertIn("invalid_raw_output_shape", structured_diagnostic_proof.unexpected_tool_calls)

            conflicting_author = root / "conflicting-author.jsonl"
            _write_events(
                conflicting_author,
                _session_events(
                    raw_output={
                        "posts": [
                            {
                                "id": POST_ID,
                                "canonical_url": POST_URL,
                                "author_info": {
                                    "legacy": {"screen_name": "OpenAI"},
                                    "rest_id": USER_ID,
                                },
                                "author": {"username": "DifferentAccount", "id": "9999999999"},
                            }
                        ]
                    }
                ),
            )
            conflicting_author_proof = extract_tool_proof(conflicting_author, expected_session_id=SESSION_ID)
            self.assertEqual(conflicting_author_proof.raw_result_post_pairs, ((POST_ID, POST_URL),))
            self.assertEqual(conflicting_author_proof.raw_result_author_user_ids, ())
            self.assertIn("conflicting_raw_post_author_binding", conflicting_author_proof.unexpected_tool_calls)

            duplicate_author = root / "duplicate-author.jsonl"
            _write_events(
                duplicate_author,
                _session_events(
                    raw_output={
                        "posts": [
                            {
                                "id": POST_ID,
                                "canonical_url": POST_URL,
                                "author_info": {
                                    "legacy": {"screen_name": "OpenAI"},
                                    "rest_id": USER_ID,
                                },
                                "author": {"username": "OpenAI", "id": USER_ID},
                            }
                        ]
                    }
                ),
            )
            duplicate_author_proof = extract_tool_proof(duplicate_author, expected_session_id=SESSION_ID)
            self.assertEqual(duplicate_author_proof.raw_result_author_user_ids, ())
            self.assertIn("duplicate_raw_post_author_identity", duplicate_author_proof.unexpected_tool_calls)

            duplicate_posts = root / "duplicate-posts.jsonl"
            duplicate_record = {"id": POST_ID, "canonical_url": POST_URL}
            _write_events(
                duplicate_posts,
                _session_events(raw_output={"posts": [duplicate_record, copy.deepcopy(duplicate_record)]}),
            )
            duplicate_proof = extract_tool_proof(duplicate_posts, expected_session_id=SESSION_ID)
            self.assertEqual(duplicate_proof.raw_result_post_pairs, ((POST_ID, POST_URL),))
            self.assertIn("duplicate_raw_post_record", duplicate_proof.unexpected_tool_calls)

            duplicate_updates = root / "duplicate-post-updates.jsonl"
            repeated_events = _session_events(raw_output=_unbound_raw_x_output())
            repeated_events.insert(-1, _tool_result("x-call", raw_output=_unbound_raw_x_output()))
            _write_events(duplicate_updates, repeated_events)
            duplicate_update_proof = extract_tool_proof(duplicate_updates, expected_session_id=SESSION_ID)
            self.assertIn("duplicate_raw_post_record", duplicate_update_proof.unexpected_tool_calls)

    def test_raw_post_registry_ignores_unread_provider_fields_without_recursive_adoption(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            extended = root / "extended-provider-record.jsonl"
            _write_events(
                extended,
                _session_events(
                    raw_output={
                        "metadata": {
                            "cursor": "provider-cursor",
                            "request_echo": {
                                "id": "1900000000000000999",
                                "canonical_url": "https://x.com/DifferentAccount/status/1900000000000000999",
                            },
                        },
                        "posts": [
                            {
                                "id": POST_ID,
                                "canonical_url": POST_URL,
                                "text": "An official technical update.",
                                "created_at": "2026-07-14T09:00:00.000Z",
                                "author_info": {
                                    "rest_id": USER_ID,
                                    "legacy": {
                                        "screen_name": "OpenAI",
                                        "name": "OpenAI",
                                        "description": "Unreviewed provider metadata",
                                    },
                                    "is_blue_verified": True,
                                },
                                "diagnostic": {"author": {"username": "DifferentAccount", "id": "9999999999"}},
                            }
                        ],
                    }
                ),
            )
            extended_proof = extract_tool_proof(extended, expected_session_id=SESSION_ID)
            self.assertEqual(extended_proof.raw_result_post_pairs, ((POST_ID, POST_URL),))
            self.assertEqual(extended_proof.raw_result_author_user_ids, (USER_ID,))
            self.assertEqual(extended_proof.unexpected_tool_calls, ())

            nested_only = root / "nested-request-echo.jsonl"
            _write_events(
                nested_only,
                _session_events(
                    raw_output={
                        "posts": [],
                        "metadata": {
                            "request_echo": {
                                "posts": [
                                    {
                                        "id": POST_ID,
                                        "canonical_url": POST_URL,
                                        "author_info": {
                                            "rest_id": USER_ID,
                                            "legacy": {"screen_name": "OpenAI"},
                                        },
                                    }
                                ]
                            }
                        },
                    }
                ),
            )
            nested_only_proof = extract_tool_proof(nested_only, expected_session_id=SESSION_ID)
            self.assertEqual(nested_only_proof.raw_result_post_pairs, ())
            self.assertEqual(nested_only_proof.raw_result_author_user_ids, ())

            incomplete_author = root / "incomplete-author-path.jsonl"
            _write_events(
                incomplete_author,
                _session_events(
                    raw_output={
                        "posts": [
                            {
                                "id": POST_ID,
                                "canonical_url": POST_URL,
                                "author": {"screen_name": "OpenAI", "display_name": "OpenAI"},
                            }
                        ]
                    }
                ),
            )
            incomplete_proof = extract_tool_proof(incomplete_author, expected_session_id=SESSION_ID)
            self.assertEqual(incomplete_proof.raw_result_author_user_ids, ())
            self.assertIn("invalid_raw_post_author_shape", incomplete_proof.unexpected_tool_calls)

    def test_raw_post_author_binding_is_per_post_not_global(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "updates.jsonl"
            _write_events(path, _session_events(raw_output=_unbound_raw_x_output()))
            proof = extract_tool_proof(path, expected_session_id=SESSION_ID)
        self.assertEqual(proof.raw_result_post_pairs, ((POST_ID, POST_URL),))
        self.assertEqual(proof.raw_result_author_user_ids, ())
        result = build_live_result(
            request=self.request,
            run_id=RUN_ID,
            session_id=SESSION_ID,
            started_at=STARTED_AT,
            completed_at=COMPLETED_AT,
            elapsed_ms=1000,
            outer=_outer_response(_inner_response()),
            inner=_inner_response(),
            proof=proof,
            approval_receipt_sha256="b" * 64,
            grok_binary_sha256=PINNED_GROK_BINARY_SHA256,
            tool_receipt_sha256="c" * 64,
        )
        self.assertEqual(result["capability"]["verdict"], "post_retrieval_only")
        self.assertIsNone(result["observations"][0]["platform_user_id"])
        self.assertEqual(validate_live_result(result, request=self.request), [])

        with tempfile.TemporaryDirectory() as directory:
            nested_unrelated = Path(directory) / "nested-unrelated.jsonl"
            _write_events(
                nested_unrelated,
                _session_events(
                    raw_output={
                        "posts": [
                            {
                                "id": POST_ID,
                                "canonical_url": POST_URL,
                                "author_info": {
                                    "legacy": {"screen_name": "OpenAI"},
                                    "unrelated_object": {"id": USER_ID},
                                },
                            }
                        ]
                    }
                ),
            )
            nested_proof = extract_tool_proof(nested_unrelated, expected_session_id=SESSION_ID)
        self.assertEqual(nested_proof.raw_result_post_pairs, ((POST_ID, POST_URL),))
        self.assertEqual(nested_proof.raw_result_author_user_ids, ())
        self.assertIn("invalid_raw_post_author_shape", nested_proof.unexpected_tool_calls)
        nested_result = build_live_result(
            request=self.request,
            run_id=RUN_ID,
            session_id=SESSION_ID,
            started_at=STARTED_AT,
            completed_at=COMPLETED_AT,
            elapsed_ms=1000,
            outer=_outer_response(_inner_response()),
            inner=_inner_response(),
            proof=nested_proof,
            approval_receipt_sha256="b" * 64,
            grok_binary_sha256=PINNED_GROK_BINARY_SHA256,
            tool_receipt_sha256="c" * 64,
        )
        self.assertEqual(nested_result["capability"]["verdict"], "capability_unavailable")
        self.assertEqual(nested_result["observations"], [])
        self.assertFalse(nested_result["capability"]["stable_account_id_proven"])

    def test_failure_receipts_reconcile_post_call_evidence_cost_and_cleanup_wall_time(self) -> None:
        outer = _outer_response(_inner_response(), cost=0.30)
        proof = _proof()
        tool_receipt = live_probe._build_tool_receipt(proof=proof, session_id=SESSION_ID, outer=outer)
        failure = live_probe._build_failure_result(
            request=self.request,
            run_id=RUN_ID,
            started_at=STARTED_AT,
            completed_at=COMPLETED_AT,
            elapsed_ms=1000,
            code="invalid_provider_evidence",
            message="Provider evidence was contradictory.",
            approval_receipt_sha256="b" * 64,
            grok_binary_sha256=PINNED_GROK_BINARY_SHA256,
            tool_receipt_sha256=live_probe.canonical_sha256(tool_receipt),
            session_id=SESSION_ID,
            proof=proof,
            outer=outer,
        )
        self.assertEqual(failure["provenance"]["provider_request_id"], "provider-request")
        self.assertEqual(failure["usage"]["result_sets"], 1)
        self.assertEqual(failure["usage"]["model_turns"], 2)
        self.assertEqual(failure["usage"]["cost_status"], "reported")
        self.assertEqual(failure["usage"]["cost_usd"], 0.30)
        self.assertEqual(validate_live_result(failure, request=self.request), [])
        self.assertEqual(live_probe._validate_tool_receipt(tool_receipt, result=failure), [])

        with tempfile.TemporaryDirectory() as directory:
            overrun_updates = Path(directory) / "model-turn-overrun.jsonl"
            _write_events(
                overrun_updates,
                _session_events(raw_output=_raw_x_output(), model_turns=5),
            )
            overrun_proof = live_probe._extract_partial_tool_proof(
                overrun_updates,
                expected_session_id=SESSION_ID,
                tolerate_trailing_partial=False,
            )
        self.assertEqual(overrun_proof.terminal_usage, _usage_receipt(model_turns=5))
        self.assertIn("session terminal model-turn budget is exceeded", overrun_proof.evidence_errors)
        overrun_outer = _outer_response(_inner_response(), model_turns=5, cost=0.30)
        overrun_receipt = live_probe._build_tool_receipt(
            proof=overrun_proof,
            session_id=SESSION_ID,
            outer=overrun_outer,
        )
        overrun_failure = live_probe._build_failure_result(
            request=self.request,
            run_id=RUN_ID,
            started_at=STARTED_AT,
            completed_at=COMPLETED_AT,
            elapsed_ms=1000,
            code="invalid_provider_evidence",
            message="The provider exceeded the reviewed model-turn budget.",
            approval_receipt_sha256="b" * 64,
            grok_binary_sha256=PINNED_GROK_BINARY_SHA256,
            tool_receipt_sha256=live_probe.canonical_sha256(overrun_receipt),
            session_id=SESSION_ID,
            proof=overrun_proof,
            outer=overrun_outer,
        )
        self.assertEqual(overrun_failure["usage"]["model_turns"], 5)
        self.assertEqual(overrun_failure["usage"]["cost_usd"], 0.30)
        self.assertEqual(validate_live_result(overrun_failure, request=self.request), [])
        self.assertEqual(live_probe._validate_tool_receipt(overrun_receipt, result=overrun_failure), [])

    def test_provider_collection_overflow_persists_one_truthful_failure_bundle(self) -> None:
        def user_event(session_id: str, model_id: str = "grok-4.5", timestamp: int = 1) -> dict[str, object]:
            return _event(
                {
                    "_meta": {"modelId": model_id, "promptIndex": timestamp - 1},
                    "content": {},
                    "sessionUpdate": "user_message_chunk",
                },
                session_id=session_id,
                timestamp=timestamp,
            )

        def mixed_calls(session_id: str) -> list[dict[str, object]]:
            events = [user_event(session_id)]
            for index in range(9):
                call_id = f"mixed-{index}"
                events.append(_tool_call(call_id, session_id=session_id))
                if index < 5:
                    events.append(_tool_result(call_id, raw_output={"posts": []}, session_id=session_id))
            events.append(_terminal_event(session_id=session_id))
            return events

        mixed_result, mixed_receipt, _ = self._run_bounded_provider_events(mixed_calls)
        self.assertEqual(
            mixed_result["usage"]["evidence_projection"],  # type: ignore[index]
            {
                "x_search_calls": {"observed": 9, "relation": "exact", "retained": 8, "truncated": True},
                "result_sets": {"observed": 5, "relation": "exact", "retained": 5, "truncated": False},
            },
        )
        self.assertEqual(len(mixed_receipt["calls"]), 8)
        self.assertEqual(sum("completed" in call["statuses"] for call in mixed_receipt["calls"]), 5)

        def completed_overflow(session_id: str) -> list[dict[str, object]]:
            events = [user_event(session_id)]
            for index in range(10):
                call_id = f"completed-{index}"
                events.append(_tool_call(call_id, session_id=session_id))
                events.append(
                    _tool_result(
                        call_id,
                        raw_output=_raw_x_output() if index == 9 else {"posts": []},
                        session_id=session_id,
                    )
                )
            events.append(_terminal_event(session_id=session_id))
            return events

        completed_result, completed_receipt, _ = self._run_bounded_provider_events(completed_overflow)
        self.assertEqual(
            completed_result["usage"]["evidence_projection"],  # type: ignore[index]
            {
                "x_search_calls": {"observed": 9, "relation": "at_least", "retained": 8, "truncated": True},
                "result_sets": {"observed": 9, "relation": "at_least", "retained": 8, "truncated": True},
            },
        )
        self.assertEqual(len(completed_receipt["calls"]), 8)
        self.assertEqual(
            completed_receipt["evidence_projection"]["raw_result_posts"],
            {"observed": 0, "relation": "at_least", "retained": 0, "truncated": True},
        )
        self.assertEqual(
            completed_receipt["evidence_projection"]["raw_result_author_user_ids"],
            {"observed": 0, "relation": "at_least", "retained": 0, "truncated": True},
        )

        def model_overflow(session_id: str) -> list[dict[str, object]]:
            return [
                *(user_event(session_id, f"provider-model-{index}", index + 1) for index in range(9)),
                _terminal_event(session_id=session_id),
            ]

        model_result, model_receipt, _ = self._run_bounded_provider_events(model_overflow)
        self.assertEqual(len(model_result["provenance"]["observed_model_ids"]), 8)  # type: ignore[index]
        self.assertEqual(
            model_receipt["evidence_projection"]["observed_model_ids"],
            {"observed": 9, "relation": "exact", "retained": 8, "truncated": True},
        )

        def author_overflow(session_id: str) -> list[dict[str, object]]:
            posts = []
            for index in range(9):
                object_id = str(1900000000000001000 + index)
                posts.append(
                    {
                        "id": object_id,
                        "canonical_url": f"https://x.com/OpenAI/status/{object_id}",
                        "author_info": {
                            "legacy": {"screen_name": "OpenAI"},
                            "rest_id": str(4398626200 + index),
                        },
                    }
                )
            return _session_events(session_id=session_id, raw_output={"posts": posts})

        author_result, author_receipt, _ = self._run_bounded_provider_events(author_overflow)
        self.assertEqual(len(author_result["provenance"]["raw_result_author_user_ids"]), 8)  # type: ignore[index]
        self.assertEqual(
            author_receipt["evidence_projection"]["raw_result_author_user_ids"],
            {"observed": 9, "relation": "exact", "retained": 8, "truncated": True},
        )

        def post_overflow(session_id: str) -> list[dict[str, object]]:
            event_values = [user_event(session_id)]
            for batch in range(2):
                posts = []
                for offset in range(25):
                    index = batch * 25 + offset
                    object_id = str(1900000000000002000 + index)
                    posts.append({"id": object_id, "canonical_url": f"https://x.com/OpenAI/status/{object_id}"})
                call_id = f"posts-{batch}"
                event_values.extend(
                    [
                        _tool_call(call_id, session_id=session_id),
                        _tool_result(call_id, raw_output={"posts": posts}, session_id=session_id),
                    ]
                )
            event_values.append(_terminal_event(session_id=session_id))
            return event_values

        post_result, post_receipt, _ = self._run_bounded_provider_events(post_overflow)
        self.assertEqual(len(post_result["provenance"]["raw_result_post_ids"]), 25)  # type: ignore[index]
        self.assertEqual(
            post_receipt["evidence_projection"]["raw_result_posts"],
            {"observed": 26, "relation": "at_least", "retained": 25, "truncated": True},
        )

        def unexpected_overflow(session_id: str) -> list[dict[str, object]]:
            return [
                user_event(session_id),
                *(
                    _tool_call(f"unexpected-{index}", tool_name=f"provider_tool_{index}", session_id=session_id)
                    for index in range(9)
                ),
                _terminal_event(session_id=session_id),
            ]

        _, unexpected_receipt, _ = self._run_bounded_provider_events(unexpected_overflow)
        self.assertEqual(len(unexpected_receipt["unexpected_tool_calls"]), 8)
        self.assertEqual(
            unexpected_receipt["evidence_projection"]["unexpected_tool_calls"],
            {"observed": 9, "relation": "exact", "retained": 8, "truncated": True},
        )

        def evidence_error_overflow(session_id: str) -> list[dict[str, object]]:
            missing_model = user_event(session_id)
            missing_model["params"]["update"]["_meta"] = {}  # type: ignore[index]
            invalid_status = _tool_result("error-call", raw_output={"posts": []}, session_id=session_id)
            invalid_status["params"]["update"]["status"] = "broken"  # type: ignore[index]
            unsupported = _event({"sessionUpdate": "provider_unknown"}, session_id=session_id, timestamp=8)
            invalid_timestamp = user_event(session_id, timestamp=6)
            invalid_timestamp["timestamp"] = True
            invalid_params = user_event(session_id, timestamp=7)
            invalid_params["params"]["unexpected"] = True  # type: ignore[index]
            invalid_method = user_event(session_id, timestamp=9)
            invalid_method["method"] = "_x.ai/session/update"
            return [
                missing_model,
                user_event(session_id, timestamp=2),
                user_event(session_id, timestamp=3),
                _tool_call("error-call", session_id=session_id),
                invalid_status,
                invalid_timestamp,
                invalid_params,
                invalid_method,
                unsupported,
                _terminal_event(session_id=session_id, stop_reason="max_turns", model_turns=5),
                unsupported,
                _terminal_event(session_id=session_id),
            ]

        _, error_receipt, _ = self._run_bounded_provider_events(evidence_error_overflow)
        self.assertEqual(len(error_receipt["evidence_errors"]), 8)
        self.assertEqual(
            error_receipt["evidence_projection"]["evidence_errors"],
            {"observed": 9, "relation": "at_least", "retained": 8, "truncated": True},
        )

        def oversized_scalar_evidence(session_id: str) -> list[dict[str, object]]:
            return [
                user_event(session_id, "model-" + "x" * 1000),
                _tool_call("oversized-tool", tool_name="tool-" + "y" * 1000, session_id=session_id),
                _terminal_event(session_id=session_id, stop_reason="stop-" + "z" * 1000),
            ]

        _, oversized_receipt, _ = self._run_bounded_provider_events(oversized_scalar_evidence)
        self.assertLessEqual(max(map(len, oversized_receipt["observed_model_ids"])), 80)
        self.assertLessEqual(max(map(len, oversized_receipt["unexpected_tool_calls"])), 160)
        self.assertIsNone(oversized_receipt["terminal_stop_reason"])

        deadline_failure = live_probe._build_failure_result(
            request=self.request,
            run_id=RUN_ID,
            started_at="2026-07-14T09:00:00.000Z",
            completed_at="2026-07-14T09:03:05.000Z",
            elapsed_ms=185000,
            code="deadline_exceeded",
            message="The provider deadline was exceeded before bounded cleanup completed.",
            approval_receipt_sha256="b" * 64,
            grok_binary_sha256=PINNED_GROK_BINARY_SHA256,
        )
        self.assertEqual(validate_live_result(deadline_failure, request=self.request), [])
        over_cleanup_budget = copy.deepcopy(deadline_failure)
        over_cleanup_budget["run"]["completed_at"] = "2026-07-14T09:03:21.000Z"
        over_cleanup_budget["usage"]["elapsed_ms"] = 201000
        over_cleanup_budget["retention"]["delete_after"] = "2026-07-15T09:03:21.000Z"
        self.assertTrue(validate_live_result(over_cleanup_budget, request=self.request))

    def test_provider_unpaired_surrogates_persist_one_terminal_failure_bundle(self) -> None:
        surrogate = "\ud800"

        call_result, call_receipt, _ = self._run_bounded_provider_events(
            lambda session_id: _session_events(session_id=session_id, call_ids=(f"x-{surrogate}",))
        )
        self.assertEqual(call_result["run"]["status"], "failed")  # type: ignore[index]
        self.assertEqual(call_receipt["calls"], [])
        self.assertIn("tool call id is invalid", call_receipt["evidence_errors"])

        outer_result, outer_receipt, _ = self._run_bounded_provider_events(
            lambda session_id: _session_events(session_id=session_id, raw_output={"posts": []}),
            outer_mutator=lambda outer: outer.update(requestId=f"req-{surrogate}"),
        )
        self.assertIsNone(outer_result["provenance"]["provider_request_id"])  # type: ignore[index]
        self.assertIsNone(outer_receipt["provider_request_id"])
        self.assertIn("outer_provider_request_id_is_invalid", outer_receipt["evidence_errors"])

        excerpt_result, excerpt_receipt, _ = self._run_bounded_provider_events(
            lambda session_id: _session_events(session_id=session_id, raw_output=_raw_x_output()),
            inner_mutator=lambda inner: inner["observations"][0].update(excerpt=f"bad-{surrogate}"),  # type: ignore[index]
        )
        self.assertEqual(excerpt_result["run"]["status"], "failed")  # type: ignore[index]
        self.assertEqual(excerpt_result["observations"], [])
        self.assertEqual(excerpt_receipt["calls"][0]["raw_result_posts"][0]["platform_object_id"], POST_ID)

    def test_generic_unknown_duplicate_and_local_tools_fail_closed(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for tool_name in ("web_search", "read_file"):
                path = root / f"{tool_name}.jsonl"
                _write_events(path, _session_events(tool_name=tool_name))
                proof = extract_tool_proof(path, expected_session_id=SESSION_ID)
                self.assertEqual(proof.x_search_calls, 0)
                self.assertIn(tool_name, proof.unexpected_tool_calls)

            alias = root / "x-alias.jsonl"
            _write_events(alias, _session_events(tool_name="X Search"))
            proof = extract_tool_proof(alias, expected_session_id=SESSION_ID)
            self.assertEqual(proof.x_search_calls, 0)
            self.assertIn("noncanonical_x_search_alias", proof.unexpected_tool_calls)

            duplicate = root / "duplicate.jsonl"
            events = _session_events(call_ids=("duplicate", "duplicate"))
            _write_events(duplicate, events)
            proof = extract_tool_proof(duplicate, expected_session_id=SESSION_ID)
            self.assertIn("duplicate_tool_call_id", proof.unexpected_tool_calls)

            unknown = root / "unknown.jsonl"
            events = _session_events(call_ids=())
            events.insert(
                -1,
                _event(
                    {
                        "sessionUpdate": "tool_call_update",
                        "status": "completed",
                        "toolCallId": "missing",
                    },
                    timestamp=3,
                ),
            )
            _write_events(unknown, events)
            proof = extract_tool_proof(unknown, expected_session_id=SESSION_ID)
            self.assertIn("unknown_tool_call_update", proof.unexpected_tool_calls)

            duplicate_model = root / "duplicate-model.jsonl"
            duplicate_model_events = _session_events(raw_output=_raw_x_output())
            duplicate_model_events.insert(1, copy.deepcopy(duplicate_model_events[0]))
            _write_events(duplicate_model, duplicate_model_events)
            duplicate_model_proof = live_probe._extract_partial_tool_proof(
                duplicate_model,
                expected_session_id=SESSION_ID,
                tolerate_trailing_partial=False,
            )
            self.assertIn(
                "session model identity evidence is duplicated",
                duplicate_model_proof.evidence_errors,
            )

    def test_outer_envelope_session_stop_and_usage_must_match_terminal(self) -> None:
        proof = _proof()
        common = {
            "request": self.request,
            "run_id": RUN_ID,
            "session_id": SESSION_ID,
            "started_at": STARTED_AT,
            "completed_at": COMPLETED_AT,
            "elapsed_ms": 1000,
            "inner": _inner_response(),
            "proof": proof,
            "approval_receipt_sha256": "b" * 64,
            "grok_binary_sha256": PINNED_GROK_BINARY_SHA256,
            "tool_receipt_sha256": "c" * 64,
        }
        for outer in (
            _outer_response(_inner_response(), stop_reason="MaxTurns"),
            _outer_response(_inner_response(), session_id="11234567-89ab-4cde-8fab-0123456789ab"),
            {**_outer_response(_inner_response()), "num_turns": 3},
            {**_outer_response(_inner_response()), "usage": {"input_tokens": 1, "output_tokens": 1, "total_tokens": 2}},
        ):
            with self.assertRaises(ValueError):
                build_live_result(**common, outer=outer)

    def test_result_verdicts_and_raw_receipt_mutations(self) -> None:
        common = {
            "request": self.request,
            "run_id": RUN_ID,
            "session_id": SESSION_ID,
            "started_at": STARTED_AT,
            "completed_at": COMPLETED_AT,
            "elapsed_ms": 1000,
            "approval_receipt_sha256": "b" * 64,
            "grok_binary_sha256": PINNED_GROK_BINARY_SHA256,
            "tool_receipt_sha256": "c" * 64,
        }
        ready = build_live_result(
            **common,
            outer=_outer_response(_inner_response()),
            inner=_inner_response(),
            proof=_proof(),
        )
        self.assertEqual(ready["capability"]["verdict"], "x_native_identity_ready")
        self.assertEqual(validate_live_result(ready, request=self.request), [])

        no_id_inner = _inner_response(stable_user_id=None)
        retrieval = build_live_result(
            **common,
            outer=_outer_response(no_id_inner, cost=None),
            inner=no_id_inner,
            proof=_proof(stable_user_id=None),
        )
        self.assertEqual(retrieval["capability"]["verdict"], "post_retrieval_only")
        self.assertEqual(validate_live_result(retrieval, request=self.request), [])

        no_call = build_live_result(
            **common,
            outer=_outer_response(_inner_response()),
            inner=_inner_response(),
            proof=_proof(x_calls=0, completed=0),
        )
        self.assertEqual(no_call["capability"]["verdict"], "capability_unavailable")
        self.assertEqual(no_call["observations"], [])

        mismatched = _proof(post_id="1900000000000000002")
        mismatch = build_live_result(
            **common,
            outer=_outer_response(_inner_response()),
            inner=_inner_response(),
            proof=mismatched,
        )
        self.assertEqual(mismatch["capability"]["verdict"], "capability_unavailable")

        over_cost = build_live_result(
            **common,
            outer=_outer_response(_inner_response(), cost=0.30),
            inner=_inner_response(),
            proof=_proof(),
        )
        self.assertEqual(over_cost["capability"]["verdict"], "budget_exceeded")
        self.assertEqual(over_cost["usage"]["cost_usd"], 0.30)
        self.assertEqual(validate_live_result(over_cost, request=self.request), [])

    def test_global_approval_owner_allows_only_one_concurrent_checkout(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            approval_root = Path(directory) / "global-owner"
            barrier = threading.Barrier(2)

            def consume(index: int) -> str:
                barrier.wait()
                try:
                    live_probe._consume_live_approval(
                        approval_root,
                        request=self.request,
                        run_id=f"xprobe_run_{index:032x}",
                        binary_sha256=PINNED_GROK_BINARY_SHA256,
                        consumed_at=STARTED_AT,
                    )
                except PermissionError:
                    return "blocked"
                return "consumed"

            with ThreadPoolExecutor(max_workers=2) as executor:
                outcomes = sorted(executor.map(consume, (1, 2)))
            self.assertEqual(outcomes, ["blocked", "consumed"])
            ledger = json.loads((approval_root / f"{self.request['probe_id']}.json").read_text())
            self.assertEqual(ledger["owner_id"], GLOBAL_APPROVAL_OWNER_ID)

    def test_monitor_detects_duplicate_calls_even_with_malformed_tail(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            updates = root / "updates.jsonl"
            events = _session_events(call_ids=("x-call-1", "x-call-2"))[:-1]
            updates.write_bytes(("\n".join(json.dumps(event) for event in events) + "\n{bad").encode())
            started = time.monotonic()
            completed = _run_bounded_command(
                [sys.executable, "-c", "import time; time.sleep(30)"],
                cwd=root,
                environment={"PATH": "/usr/bin:/bin", "HOME": str(root)},
                updates_path=updates,
                expected_session_id=SESSION_ID,
            )
        self.assertEqual(completed.stop_reason, "tool_kill_switch_tripped")
        self.assertLess(time.monotonic() - started, 2)

    def test_process_group_cleanup_kills_descendant_after_parent_exit(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            pid_path = root / "child.pid"
            code = (
                "import subprocess,sys; "
                "p=subprocess.Popen([sys.executable,'-c','import time; time.sleep(30)']); "
                "open(sys.argv[1],'w').write(str(p.pid))"
            )
            completed = _run_bounded_command(
                [sys.executable, "-c", code, str(pid_path)],
                cwd=root,
                environment={"PATH": "/usr/bin:/bin", "HOME": str(root)},
                updates_path=root / "missing.jsonl",
                expected_session_id=SESSION_ID,
            )
            child_pid = int(pid_path.read_text())
            child_alive = subprocess.run(
                ["/bin/ps", "-p", str(child_pid), "-o", "pid="],
                check=False,
                capture_output=True,
                text=True,
            ).stdout.strip()
        self.assertEqual(completed.returncode, 0)
        self.assertEqual(child_alive, "")

    def test_monitor_exception_is_typed_and_cannot_bypass_process_group_cleanup(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            updates = root / "updates.jsonl"
            updates.write_text("{}\n", encoding="utf-8")
            captured_pids: list[int] = []
            real_popen = subprocess.Popen

            def recording_popen(*args: object, **kwargs: object) -> subprocess.Popen[bytes]:
                process = real_popen(*args, **kwargs)
                captured_pids.append(process.pid)
                return process

            with (
                mock.patch("x_first.live_probe.subprocess.Popen", side_effect=recording_popen),
                mock.patch(
                    "x_first.live_probe._extract_partial_tool_proof",
                    side_effect=OSError("simulated provider-evidence read race"),
                ),
            ):
                completed = _run_bounded_command(
                    [sys.executable, "-c", "import time; time.sleep(30)"],
                    cwd=root,
                    environment={"PATH": "/usr/bin:/bin", "HOME": str(root)},
                    updates_path=updates,
                    expected_session_id=SESSION_ID,
                )
            alive = subprocess.run(
                ["/bin/ps", "-p", str(captured_pids[0]), "-o", "pid="],
                check=False,
                capture_output=True,
                text=True,
            ).stdout.strip()
        self.assertEqual(completed.execution_error, "monitor_failed")
        self.assertEqual(completed.stop_reason, "monitor_failed")
        self.assertEqual(alive, "")

    def test_cleanup_verification_exception_is_typed_after_descendant_kill(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            child_pid_path = root / "cleanup-child.pid"
            code = (
                "import subprocess,sys; "
                "p=subprocess.Popen([sys.executable,'-c','import time; time.sleep(30)']); "
                "open(sys.argv[1],'w').write(str(p.pid))"
            )
            with mock.patch(
                "x_first.live_probe._wait_for_process_group_exit",
                side_effect=RuntimeError("simulated cleanup verification failure"),
            ):
                completed = _run_bounded_command(
                    [sys.executable, "-c", code, str(child_pid_path)],
                    cwd=root,
                    environment={"PATH": "/usr/bin:/bin", "HOME": str(root)},
                    updates_path=root / "missing.jsonl",
                    expected_session_id=SESSION_ID,
                )
            child_pid = int(child_pid_path.read_text())
            child_alive = subprocess.run(
                ["/bin/ps", "-p", str(child_pid), "-o", "pid="],
                check=False,
                capture_output=True,
                text=True,
            ).stdout.strip()
        self.assertEqual(completed.cleanup_error, "process_group_cleanup_failed")
        self.assertEqual(completed.stop_reason, "process_group_cleanup_failed")
        self.assertEqual(child_alive, "")

    def test_artifact_validation_rejects_forgery_inventory_rename_and_impossible_time(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            approval_root = root / "global-approval"
            with (
                mock.patch("x_first.live_probe.project_root", return_value=root),
                mock.patch("x_first.live_probe._global_approval_root", return_value=approval_root),
            ):
                bundle, result, approval, _ = _write_valid_bundle(root, approval_root)
                self.assertEqual(validate_artifact_pair(bundle / "request.json", bundle / "result.json"), [])

                extra = bundle / "updates.jsonl"
                extra.write_text("forged transcript", encoding="utf-8")
                extra.chmod(0o600)
                self.assertTrue(validate_artifact_pair(bundle / "request.json", bundle / "result.json"))
                extra.unlink()

                renamed = bundle.with_name("renamed-success")
                bundle.rename(renamed)
                self.assertTrue(validate_artifact_pair(renamed / "request.json", renamed / "result.json"))
                renamed.rename(bundle)

                # A self-consistent local bundle is not valid without the checkout-independent owner ledger.
                ledger_path = approval_root / f"{self.request['probe_id']}.json"
                ledger_path.unlink()
                self.assertTrue(validate_artifact_pair(bundle / "request.json", bundle / "result.json"))
                live_probe._atomic_write_json(ledger_path, approval)
                self.assertEqual(validate_artifact_pair(bundle / "request.json", bundle / "result.json"), [])

                impossible = "2026-02-31T09:00:00.000Z"
                approval["consumed_at"] = impossible
                result["run"]["started_at"] = impossible  # type: ignore[index]
                result["run"]["completed_at"] = impossible  # type: ignore[index]
                result["retention"]["delete_after"] = None  # type: ignore[index]
                result["provenance"]["approval_receipt_sha256"] = live_probe.canonical_sha256(approval)  # type: ignore[index]
                live_probe._atomic_write_json(bundle / "approval-receipt.json", approval)
                live_probe._atomic_write_json(bundle / "result.json", result)
                live_probe._atomic_write_json(
                    approval_root / f"{self.request['probe_id']}.json",
                    approval,
                )
                self.assertTrue(validate_artifact_pair(bundle / "request.json", bundle / "result.json"))

    def test_purge_detects_renames_and_never_records_deletion_before_success(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            approval_root = root / "global-approval"
            with (
                mock.patch("x_first.live_probe.project_root", return_value=root),
                mock.patch("x_first.live_probe._global_approval_root", return_value=approval_root),
            ):
                bundle, _, _, _ = _write_valid_bundle(root, approval_root)
                renamed = bundle.with_name("renamed-success")
                bundle.rename(renamed)
                with self.assertRaises(ValueError):
                    purge_expired_live_artifacts(
                        runtime_root=root / "runtime/live-probes",
                        now=datetime.now(UTC) + timedelta(days=2),
                    )
                renamed.rename(bundle)

                with mock.patch("x_first.live_probe.shutil.rmtree", side_effect=OSError("blocked")):
                    with self.assertRaises(OSError):
                        purge_expired_live_artifacts(
                            runtime_root=root / "runtime/live-probes",
                            now=datetime.now(UTC) + timedelta(days=2),
                        )
                self.assertTrue(bundle.exists())
                self.assertFalse((root / "runtime/live-probes/.deletions" / f"{RUN_ID}.json").exists())

                receipts = purge_expired_live_artifacts(
                    runtime_root=root / "runtime/live-probes",
                    now=datetime.now(UTC) + timedelta(days=2),
                )
                self.assertEqual([receipt["run_id"] for receipt in receipts], [RUN_ID])
                self.assertFalse(bundle.exists())

    def test_provider_receipt_semantic_mutations_fail_even_when_rehashed(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            approval_root = root / "global-approval"
            with (
                mock.patch("x_first.live_probe.project_root", return_value=root),
                mock.patch("x_first.live_probe._global_approval_root", return_value=approval_root),
            ):
                bundle, base_result, _, base_tool = _write_valid_bundle(root, approval_root)

                def assert_rejected(mutator: object) -> None:
                    result = copy.deepcopy(base_result)
                    tool = copy.deepcopy(base_tool)
                    mutator(tool)  # type: ignore[operator]
                    result["provenance"]["tool_receipt_sha256"] = live_probe.canonical_sha256(tool)  # type: ignore[index]
                    live_probe._atomic_write_json(bundle / "tool-receipt.json", tool)
                    live_probe._atomic_write_json(bundle / "result.json", result)
                    self.assertTrue(validate_artifact_pair(bundle / "request.json", bundle / "result.json"))

                for mutate in (
                    lambda value: value.update(outer_stop_reason="MaxTurns"),
                    lambda value: value.update(outer_session_id="11234567-89ab-4cde-8fab-0123456789ab"),
                    lambda value: value.update(outer_model_turns=3),
                    lambda value: value.update(outer_total_cost_usd=0.02),
                    lambda value: value["terminal_usage"].update(total_tokens=151),
                    lambda value: value["calls"][0]["raw_result_posts"][0].update(platform_user_id=None),
                    lambda value: value["calls"][0]["raw_result_posts"].append(
                        copy.deepcopy(value["calls"][0]["raw_result_posts"][0])
                    ),
                    lambda value: value["calls"][0].update(raw_result_author_user_ids=[USER_ID, USER_ID]),
                    lambda value: value.update(observed_model_ids=[{"not": "a-string"}]),
                    lambda value: value["calls"][0].update(statuses=[{"not": "a-status"}]),
                    lambda value: value.update(evidence_errors=["forged"]),
                    lambda value: value.update(extra="field"),
                ):
                    assert_rejected(mutate)

    def test_rehashed_artifacts_reject_non_unicode_scalar_provider_strings(self) -> None:
        surrogate = "\ud800"
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            approval_root = root / "global-approval"
            with (
                mock.patch("x_first.live_probe.project_root", return_value=root),
                mock.patch("x_first.live_probe._global_approval_root", return_value=approval_root),
            ):
                bundle, base_result, _, base_tool = _write_valid_bundle(root, approval_root)

                def assert_success_bundle_rejected(mutator: object, expected_error: str) -> None:
                    result = copy.deepcopy(base_result)
                    tool = copy.deepcopy(base_tool)
                    mutator(result, tool)  # type: ignore[operator]
                    result["provenance"]["tool_receipt_sha256"] = live_probe.canonical_sha256(tool)  # type: ignore[index]
                    live_probe._atomic_write_json(bundle / "tool-receipt.json", tool)
                    live_probe._atomic_write_json(bundle / "result.json", result)
                    errors = validate_artifact_pair(bundle / "request.json", bundle / "result.json")
                    self.assertNotIn(
                        f"{live_probe.LIVE_DIAGNOSTIC_CODE}: live artifact pair could not be loaded",
                        errors,
                    )
                    self.assertIn(expected_error, errors)

                assert_success_bundle_rejected(
                    lambda result, tool: (
                        result["provenance"].update(provider_request_id=surrogate),
                        tool.update(provider_request_id=surrogate),
                    ),
                    f"{live_probe.LIVE_DIAGNOSTIC_CODE}: result provider request id is invalid",
                )
                assert_success_bundle_rejected(
                    lambda _result, tool: tool["calls"][0].update(call_id=surrogate),
                    f"{live_probe.LIVE_DIAGNOSTIC_CODE}: tool-call receipt id is invalid",
                )
                assert_success_bundle_rejected(
                    lambda result, tool: (
                        result["provenance"].update(provider_request_id=None),
                        tool.update(provider_request_id=None),
                    ),
                    f"{live_probe.LIVE_DIAGNOSTIC_CODE}: successful result lacks provider/session provenance",
                )

        def assert_failure_bundle_rejected(
            result: dict[str, object],
            tool: dict[str, object],
            bundle: Path,
            expected_error: str,
        ) -> None:
            root = bundle.parents[2]
            binary_sha256 = result["provenance"]["grok_binary_sha256"]  # type: ignore[index]
            with (
                mock.patch("x_first.live_probe.project_root", return_value=root),
                mock.patch(
                    "x_first.live_probe._global_approval_root",
                    return_value=root / "global-approval",
                ),
                mock.patch("x_first.live_probe.PINNED_GROK_BINARY_SHA256", binary_sha256),
            ):
                result["provenance"]["tool_receipt_sha256"] = live_probe.canonical_sha256(tool)  # type: ignore[index]
                live_probe._atomic_write_json(bundle / "tool-receipt.json", tool)
                live_probe._atomic_write_json(bundle / "result.json", result)
                errors = validate_artifact_pair(bundle / "request.json", bundle / "result.json")
            self.assertNotIn(
                f"{live_probe.LIVE_DIAGNOSTIC_CODE}: live artifact pair could not be loaded",
                errors,
            )
            self.assertIn(expected_error, errors)

        model_result, model_tool, model_bundle = self._run_bounded_provider_events(
            lambda session_id: _session_events(session_id=session_id, raw_output={"posts": []})
        )
        model_result["provenance"]["observed_model_ids"] = [surrogate]  # type: ignore[index]
        model_tool["observed_model_ids"] = [surrogate]
        assert_failure_bundle_rejected(
            model_result,
            model_tool,
            model_bundle,
            f"{live_probe.LIVE_DIAGNOSTIC_CODE}: tool receipt observed-model list is invalid",
        )

        unexpected_result, unexpected_tool, unexpected_bundle = self._run_bounded_provider_events(
            lambda session_id: _session_events(
                session_id=session_id,
                tool_name="provider_tool",
                call_ids=("unexpected",),
            )
        )
        unexpected_tool["unexpected_tool_calls"] = [surrogate]
        assert_failure_bundle_rejected(
            unexpected_result,
            unexpected_tool,
            unexpected_bundle,
            f"{live_probe.LIVE_DIAGNOSTIC_CODE}: tool receipt unexpected-tool list is invalid",
        )

        def evidence_error_events(session_id: str) -> list[dict[str, object]]:
            events = _session_events(session_id=session_id, call_ids=())
            events.insert(1, copy.deepcopy(events[0]))
            return events

        error_result, error_tool, error_bundle = self._run_bounded_provider_events(evidence_error_events)
        error_tool["evidence_errors"] = [surrogate]
        assert_failure_bundle_rejected(
            error_result,
            error_tool,
            error_bundle,
            f"{live_probe.LIVE_DIAGNOSTIC_CODE}: tool receipt evidence-error list is invalid",
        )

    def test_outer_only_and_receiptless_projections_fail_closed_before_publish(self) -> None:
        outer = _outer_response(_inner_response())
        outer_receipt = live_probe._build_tool_receipt(proof=None, session_id=SESSION_ID, outer=outer)
        outer_failure = live_probe._build_failure_result(
            request=self.request,
            run_id=RUN_ID,
            started_at=STARTED_AT,
            completed_at=COMPLETED_AT,
            elapsed_ms=1000,
            code="invalid_provider_evidence",
            message="Structured update evidence was unavailable.",
            approval_receipt_sha256="b" * 64,
            grok_binary_sha256=PINNED_GROK_BINARY_SHA256,
            tool_receipt_sha256=live_probe.canonical_sha256(outer_receipt),
            session_id=SESSION_ID,
            proof=None,
            outer=outer,
        )
        self.assertEqual(validate_live_result(outer_failure, request=self.request), [])
        self.assertEqual(live_probe._validate_tool_receipt(outer_receipt, result=outer_failure), [])
        for forged_zero in (False, 0.0, "0"):
            mutated_receipt = copy.deepcopy(outer_receipt)
            mutated_result = copy.deepcopy(outer_failure)
            mutated_receipt["session_update_bytes"] = forged_zero
            mutated_result["provenance"]["tool_receipt_sha256"] = live_probe.canonical_sha256(mutated_receipt)
            self.assertTrue(live_probe._validate_tool_receipt(mutated_receipt, result=mutated_result))

        receiptless = live_probe._build_failure_result(
            request=self.request,
            run_id=RUN_ID,
            started_at=STARTED_AT,
            completed_at=COMPLETED_AT,
            elapsed_ms=1000,
            code="process_spawn_failed",
            message="The process did not expose provider evidence.",
            approval_receipt_sha256="b" * 64,
            grok_binary_sha256=PINNED_GROK_BINARY_SHA256,
        )
        self.assertEqual(validate_live_result(receiptless, request=self.request), [])
        forged_calls = copy.deepcopy(receiptless)
        forged_calls["usage"]["x_search_calls"] = 1
        forged_calls["usage"]["evidence_projection"]["x_search_calls"] = {
            "observed": 1,
            "relation": "exact",
            "retained": 1,
            "truncated": False,
        }
        self.assertTrue(validate_live_result(forged_calls, request=self.request))
        forged_posts = copy.deepcopy(receiptless)
        forged_posts["provenance"]["raw_result_post_ids"] = [POST_ID]
        forged_posts["provenance"]["evidence_projection"]["raw_result_posts"] = {
            "observed": 1,
            "relation": "exact",
            "retained": 1,
            "truncated": False,
        }
        self.assertTrue(validate_live_result(forged_posts, request=self.request))

        proof = _proof()
        tool_receipt = live_probe._build_tool_receipt(proof=proof, session_id=SESSION_ID, outer=outer)
        approval = _approval_receipt(self.request)
        result = build_live_result(
            request=self.request,
            run_id=RUN_ID,
            session_id=SESSION_ID,
            started_at=STARTED_AT,
            completed_at=COMPLETED_AT,
            elapsed_ms=1000,
            outer=outer,
            inner=_inner_response(),
            proof=proof,
            approval_receipt_sha256=live_probe.canonical_sha256(approval),
            grok_binary_sha256=PINNED_GROK_BINARY_SHA256,
            tool_receipt_sha256=live_probe.canonical_sha256(tool_receipt),
        )
        forged_tool = copy.deepcopy(tool_receipt)
        forged_tool["session_update_bytes"] = False
        result["provenance"]["tool_receipt_sha256"] = live_probe.canonical_sha256(forged_tool)
        with tempfile.TemporaryDirectory() as directory:
            runtime_root = Path(directory) / "live-probes"
            runtime_root.mkdir(mode=0o700)
            with self.assertRaises(RuntimeError):
                live_probe._write_artifact_bundle(
                    runtime_root,
                    run_id=RUN_ID,
                    request=self.request,
                    result=result,
                    approval_receipt=approval,
                    tool_receipt=forged_tool,
                )
            self.assertEqual(list(runtime_root.iterdir()), [])

    def test_runner_uses_global_approval_strict_evidence_and_private_bundle(self) -> None:
        with self.assertRaises(PermissionError):
            run_live_probe(execute_live=False)

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            approval_root = root / "global-approval"
            binary = root / "grok"
            binary.write_text("#!/bin/sh\nexit 1\n", encoding="utf-8")
            binary.chmod(stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR)
            auth = root / "auth.json"
            auth.write_text('{"private":"credential-material"}', encoding="utf-8")
            auth.chmod(0o600)
            binary_digest = hashlib.sha256(binary.read_bytes()).hexdigest()

            def fake_run(command: list[str], **kwargs: object) -> BoundedCommandResult:
                environment = kwargs["environment"]
                self.assertIsInstance(environment, dict)
                self.assertEqual(
                    set(environment),
                    {
                        "GROK_HOME",
                        "GROK_DISABLE_AUTOUPDATER",
                        "HOME",
                        "LANG",
                        "LC_ALL",
                        "NO_COLOR",
                        "PATH",
                        "TERM",
                        "TMPDIR",
                    },
                )
                self.assertNotIn("XAI_API_KEY", environment)
                self.assertIn("--disable-web-search", command)
                self.assertNotIn("--tools", command)
                self.assertEqual(command[command.index("--sandbox") + 1], "read-only")
                session_id = command[command.index("--session-id") + 1]
                self.assertEqual(kwargs["expected_session_id"], session_id)
                updates = Path(kwargs["updates_path"])
                _write_events(updates, _session_events(session_id=session_id, raw_output=_raw_x_output()))
                inner = _inner_response()
                return BoundedCommandResult(
                    returncode=0,
                    stdout=json.dumps(_outer_response(inner, session_id=session_id)).encode(),
                    stderr=b"",
                    stop_reason=None,
                )

            with (
                mock.patch("x_first.live_probe._run_bounded_command", side_effect=fake_run),
                mock.patch("x_first.live_probe.project_root", return_value=root),
                mock.patch("x_first.live_probe._global_approval_root", return_value=approval_root),
                mock.patch("x_first.live_probe.PINNED_GROK_BINARY_SHA256", binary_digest),
            ):
                result, artifact_root = run_live_probe(execute_live=True, grok_binary=binary, auth_path=auth)
                self.assertEqual(result["capability"]["verdict"], "x_native_identity_ready")
                self.assertEqual(
                    validate_artifact_pair(artifact_root / "request.json", artifact_root / "result.json"),
                    [],
                )
                with self.assertRaises(PermissionError):
                    run_live_probe(execute_live=True, grok_binary=binary, auth_path=auth)
                receipts = purge_expired_live_artifacts(
                    runtime_root=root / "runtime/live-probes",
                    now=datetime.now(UTC) + timedelta(days=2),
                )
                self.assertEqual([receipt["run_id"] for receipt in receipts], [artifact_root.name])
            self.assertNotIn(
                "credential-material",
                json.dumps(result),
            )

    def test_runner_persists_a_valid_post_call_failure_bundle(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            approval_root = root / "global-approval"
            binary = root / "grok"
            binary.write_text("#!/bin/sh\nexit 1\n", encoding="utf-8")
            binary.chmod(stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR)
            auth = root / "auth.json"
            auth.write_text('{"private":"credential-material"}', encoding="utf-8")
            auth.chmod(0o600)
            binary_digest = hashlib.sha256(binary.read_bytes()).hexdigest()

            def fake_run(command: list[str], **kwargs: object) -> BoundedCommandResult:
                session_id = command[command.index("--session-id") + 1]
                updates = Path(kwargs["updates_path"])
                _write_events(updates, _session_events(session_id=session_id, raw_output=_raw_x_output()))
                invalid_inner = {**_inner_response(), "unexpected": "field"}
                return BoundedCommandResult(
                    returncode=0,
                    stdout=json.dumps(_outer_response(invalid_inner, session_id=session_id, cost=0.30)).encode(),
                    stderr=b"",
                    stop_reason=None,
                )

            with (
                mock.patch("x_first.live_probe._run_bounded_command", side_effect=fake_run),
                mock.patch("x_first.live_probe.project_root", return_value=root),
                mock.patch("x_first.live_probe._global_approval_root", return_value=approval_root),
                mock.patch("x_first.live_probe.PINNED_GROK_BINARY_SHA256", binary_digest),
            ):
                result, artifact_root = run_live_probe(execute_live=True, grok_binary=binary, auth_path=auth)
                self.assertEqual(result["run"]["status"], "failed")
                self.assertEqual(result["task"]["stop_reason"], "invalid_provider_evidence")
                self.assertEqual(result["usage"]["x_search_calls"], 1)
                self.assertEqual(result["usage"]["result_sets"], 1)
                self.assertEqual(result["usage"]["cost_usd"], 0.30)
                self.assertEqual(
                    validate_artifact_pair(artifact_root / "request.json", artifact_root / "result.json"),
                    [],
                )

    def test_runner_preserves_calls_and_outer_receipt_on_typed_monitor_failure(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            approval_root = root / "global-approval"
            binary = root / "grok"
            binary.write_text("#!/bin/sh\nexit 1\n", encoding="utf-8")
            binary.chmod(stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR)
            auth = root / "auth.json"
            auth.write_text('{"private":"credential-material"}', encoding="utf-8")
            auth.chmod(0o600)
            binary_digest = hashlib.sha256(binary.read_bytes()).hexdigest()

            def fake_run(command: list[str], **kwargs: object) -> BoundedCommandResult:
                session_id = command[command.index("--session-id") + 1]
                updates = Path(kwargs["updates_path"])
                _write_events(updates, _session_events(session_id=session_id, raw_output=_raw_x_output()))
                outer = _outer_response(_inner_response(), session_id=session_id, cost=0.30)
                outer["text"] = "{not-json"
                outer["unexpected"] = "field"
                return BoundedCommandResult(
                    returncode=-9,
                    stdout=json.dumps(outer).encode(),
                    stderr=b"",
                    stop_reason="monitor_failed",
                    execution_error="monitor_failed",
                )

            with (
                mock.patch("x_first.live_probe._run_bounded_command", side_effect=fake_run),
                mock.patch("x_first.live_probe.project_root", return_value=root),
                mock.patch("x_first.live_probe._global_approval_root", return_value=approval_root),
                mock.patch("x_first.live_probe.PINNED_GROK_BINARY_SHA256", binary_digest),
            ):
                result, artifact_root = run_live_probe(execute_live=True, grok_binary=binary, auth_path=auth)
                self.assertEqual(result["task"]["stop_reason"], "monitor_failed")
                self.assertEqual(result["provenance"]["provider_request_id"], "provider-request")
                self.assertEqual(result["provenance"]["raw_result_post_ids"], [POST_ID])
                self.assertEqual(result["usage"]["x_search_calls"], 1)
                self.assertEqual(result["usage"]["result_sets"], 1)
                self.assertEqual(result["usage"]["model_turns"], 2)
                self.assertEqual(result["usage"]["cost_status"], "reported")
                self.assertEqual(result["usage"]["cost_usd"], 0.30)
                self.assertEqual(
                    validate_artifact_pair(artifact_root / "request.json", artifact_root / "result.json"),
                    [],
                )

    def test_deep_provider_json_is_bounded_and_still_persists_one_failure_bundle(self) -> None:
        too_deep = (
            "[" * (live_probe.MAX_JSON_STRUCTURE_DEPTH + 1) + "0" + "]" * (live_probe.MAX_JSON_STRUCTURE_DEPTH + 1)
        )
        too_many_nodes = "[" + ",".join("0" for _ in range(live_probe.MAX_JSON_STRUCTURE_NODES)) + "]"
        with self.assertRaises(ValueError):
            live_probe._strict_json_loads(too_deep)
        with self.assertRaises(ValueError):
            live_probe._strict_json_loads(too_many_nodes)

        nested_json = b"[" * 10_000 + b"0" + b"]" * 10_000
        for malformed_source in ("stdout", "updates"):
            with self.subTest(malformed_source=malformed_source), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                approval_root = root / "global-approval"
                binary = root / "grok"
                binary.write_text("#!/bin/sh\nexit 1\n", encoding="utf-8")
                binary.chmod(stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR)
                auth = root / "auth.json"
                auth.write_text('{"private":"credential-material"}', encoding="utf-8")
                auth.chmod(0o600)
                binary_digest = hashlib.sha256(binary.read_bytes()).hexdigest()

                def fake_run(command: list[str], **kwargs: object) -> BoundedCommandResult:
                    session_id = command[command.index("--session-id") + 1]
                    stdout = nested_json
                    if malformed_source == "updates":
                        updates = Path(kwargs["updates_path"])
                        updates.parent.mkdir(mode=0o700, parents=True)
                        updates.write_bytes(nested_json + b"\n")
                        stdout = json.dumps(_outer_response(_inner_response(), session_id=session_id)).encode()
                    return BoundedCommandResult(returncode=0, stdout=stdout, stderr=b"", stop_reason=None)

                with (
                    mock.patch("x_first.live_probe._run_bounded_command", side_effect=fake_run),
                    mock.patch("x_first.live_probe.project_root", return_value=root),
                    mock.patch("x_first.live_probe._global_approval_root", return_value=approval_root),
                    mock.patch("x_first.live_probe.PINNED_GROK_BINARY_SHA256", binary_digest),
                ):
                    result, artifact_root = run_live_probe(execute_live=True, grok_binary=binary, auth_path=auth)
                self.assertEqual(result["run"]["status"], "failed")
                self.assertEqual(result["task"]["stop_reason"], "invalid_provider_evidence")
                self.assertEqual(len(list((root / "runtime/live-probes").glob("xprobe_run_*"))), 1)
                approval = json.loads((approval_root / f"{self.request['probe_id']}.json").read_text(encoding="utf-8"))
                self.assertEqual(approval["state"], "consumed_before_spawn")
                self.assertEqual(approval["run_id"], result["run"]["run_id"])
                with (
                    mock.patch("x_first.live_probe.project_root", return_value=root),
                    mock.patch("x_first.live_probe._global_approval_root", return_value=approval_root),
                    mock.patch("x_first.live_probe.PINNED_GROK_BINARY_SHA256", binary_digest),
                ):
                    artifact_errors = validate_artifact_pair(
                        artifact_root / "request.json",
                        artifact_root / "result.json",
                    )
                self.assertEqual(
                    artifact_errors,
                    [],
                )

    def test_runner_retains_outer_only_request_session_tokens_turns_and_cost(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            approval_root = root / "global-approval"
            binary = root / "grok"
            binary.write_text("#!/bin/sh\nexit 1\n", encoding="utf-8")
            binary.chmod(stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR)
            auth = root / "auth.json"
            auth.write_text('{"private":"credential-material"}', encoding="utf-8")
            auth.chmod(0o600)
            binary_digest = hashlib.sha256(binary.read_bytes()).hexdigest()

            def fake_run(command: list[str], **_: object) -> BoundedCommandResult:
                session_id = command[command.index("--session-id") + 1]
                outer = _outer_response(_inner_response(), session_id=session_id, cost=0.03)
                return BoundedCommandResult(
                    returncode=0,
                    stdout=json.dumps(outer).encode(),
                    stderr=b"",
                    stop_reason=None,
                )

            with (
                mock.patch("x_first.live_probe._run_bounded_command", side_effect=fake_run),
                mock.patch("x_first.live_probe.project_root", return_value=root),
                mock.patch("x_first.live_probe._global_approval_root", return_value=approval_root),
                mock.patch("x_first.live_probe.PINNED_GROK_BINARY_SHA256", binary_digest),
            ):
                result, artifact_root = run_live_probe(execute_live=True, grok_binary=binary, auth_path=auth)
            tool_receipt = json.loads((artifact_root / "tool-receipt.json").read_text(encoding="utf-8"))
            self.assertEqual(
                sorted(path.name for path in artifact_root.iterdir()),
                [
                    "approval-receipt.json",
                    "request.json",
                    "result.json",
                    "tool-receipt.json",
                ],
            )
            self.assertEqual(result["task"]["stop_reason"], "invalid_provider_evidence")
            self.assertEqual(result["provenance"]["provider_request_id"], "provider-request")
            self.assertEqual(result["provenance"]["session_id"], tool_receipt["session_id"])
            self.assertEqual(tool_receipt["outer_session_id"], tool_receipt["session_id"])
            self.assertEqual(
                tool_receipt["outer_usage"],
                {"input_tokens": 100, "output_tokens": 50, "total_tokens": 150},
            )
            self.assertEqual(tool_receipt["outer_model_turns"], 2)
            self.assertEqual(tool_receipt["outer_total_cost_usd"], 0.03)
            self.assertIsNone(tool_receipt["session_updates_sha256"])
            self.assertEqual(tool_receipt["session_update_bytes"], 0)
            self.assertEqual(tool_receipt["calls"], [])
            self.assertEqual(result["usage"]["x_search_calls"], 0)
            self.assertEqual(result["usage"]["result_sets"], 0)
            self.assertEqual(result["usage"]["model_turns"], 2)
            self.assertEqual(result["usage"]["cost_usd"], 0.03)
            with (
                mock.patch("x_first.live_probe.project_root", return_value=root),
                mock.patch("x_first.live_probe._global_approval_root", return_value=approval_root),
                mock.patch("x_first.live_probe.PINNED_GROK_BINARY_SHA256", binary_digest),
            ):
                artifact_errors = validate_artifact_pair(
                    artifact_root / "request.json",
                    artifact_root / "result.json",
                )
            self.assertEqual(
                artifact_errors,
                [],
            )


if __name__ == "__main__":
    unittest.main()
