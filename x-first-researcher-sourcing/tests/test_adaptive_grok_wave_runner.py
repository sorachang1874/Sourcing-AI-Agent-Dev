from __future__ import annotations

import copy
import hashlib
import json
import os
import runpy
import signal
import socket
import stat
import subprocess
import sys
import tempfile
import time
import unittest
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from x_first import adaptive_grok_wave_runner as runner  # noqa: E402
from x_first.adaptive_grok_wave_runner import (  # noqa: E402
    AUTHORITY,
    AdaptiveWaveValidationError,
    ProcessGroupExecutor,
    ProcessResult,
    _atomic_publish,
    _run_adaptive_wave,
    canonical_json,
    compile_prompt,
    issue_live_grant,
    load_prior_context,
    load_prior_handles,
    purge_expired_adaptive_runs,
    recover_incomplete_run,
    recover_pending_publications,
    run_adaptive_grok_wave_fixture,
    validate_model_result,
    validate_operator_bundle,
    validate_operator_receipt,
    validate_request,
)
from x_first.adaptive_recall_campaign_bridge import (  # noqa: E402
    BRIDGE_SCHEMA_FILE,
    REASON_CODE,
    ZERO_AUTHORITY,
    AdaptiveCampaignBridgeError,
    build_blocked_campaign_bridge,
)
from x_first.recall_pool_schema import (  # noqa: E402
    MiniDraft202012Error,
    assert_schema_valid,
    contract_schema_sha256,
)

FIXED_TIME = datetime(2026, 7, 14, 10, 0, 0, tzinfo=UTC)
PRODUCTION_EFFECTIVE_PROMPT_POLICY = ROOT / "configs/adaptive_grok_wave_effective_prompt_policy.v1.json"
TEST_EFFECTIVE_PROMPT_POLICY = ROOT / "tests/fixtures/adaptive_grok_wave_effective_prompt_policy.test.v1.json"


def _bytes_sha(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _write_private(path: Path, value: bytes) -> None:
    path.write_bytes(value)
    os.chmod(path, 0o600)


def _empty_result() -> dict[str, Any]:
    return {
        "status": "X_SEARCH_PARTIAL",
        "status_reason": "Synthetic native-X fixture response.",
        "native_x_tool_provenance": {
            "tools_reported": ["x_keyword_search"],
            "tool_calls_reported": 1,
            "queries": ["synthetic query"],
            "generic_web_used": False,
        },
        "counts": {"observations_inspected_reported": 1, "candidates_retained": 0},
        "candidates": [],
        "excluded_examples": [],
        "limitations": ["Synthetic output; no provider access."],
        "local_reconciliation": {
            "candidate_records_validated": 0,
            "evidence_items_validated": 0,
            "post_urls_structurally_validated": 0,
            "provider_post_bodies_replayable": False,
            "tool_calls_completed": 1,
            "tool_counts": {"x_keyword_search": 1},
        },
    }


def _candidate(handle: str, *, profile_host: str = "profiles.invalid") -> dict[str, Any]:
    return {
        "handle": handle,
        "profile_url": f"https://{profile_host}/{handle}",
        "platform_user_id": None,
        "bio_excerpt": None,
        "target_lab_affiliation_state": "ambiguous",
        "pretraining_experience_state": "ambiguous",
        "confidence": "low",
        "evidence": [],
        "caveats": ["Synthetic row."],
        "overlap_status": "novel",
    }


def _build_request(
    root: Path,
    *,
    wave_count: int = 0,
    binary_sha: str | None = None,
    auth_sha: str | None = None,
    grant_id: str | None = "synthetic_live_grant_001",
) -> tuple[dict[str, Any], Path]:
    if binary_sha is not None and auth_sha is None and (root / "auth.json").is_file():
        auth_sha = _bytes_sha((root / "auth.json").read_bytes())
    prompt = root / "prompt.md"
    prompt_raw = b"Find a broad public-professional synthetic researcher population.\n"
    target = {
        "lab_id": "synthetic_lab",
        "research_focus_id": "base_model_training",
        "scope": "Public professional evidence for a synthetic lab-neutral fixture.",
    }
    _write_private(prompt, prompt_raw)
    prior_waves: list[dict[str, str]] = []
    for index in range(wave_count):
        handle = f"fixture_{index:03d}"
        payload = {"candidates": [{"handle": handle}]}
        raw = (canonical_json(payload) + "\n").encode()
        path = root / f"prior_{index:03d}.json"
        _write_private(path, raw)
        prior_waves.append({"wave_id": f"wave_{index:03d}", "path": str(path), "sha256": _bytes_sha(raw)})
    request = {
        "schema_version": runner.REQUEST_SCHEMA_VERSION,
        "request_id": "xwave_req_11111111111111111111111111111111",
        "target": target,
        "prompt_source": {"path": str(prompt), "sha256": _bytes_sha(prompt_raw)},
        "prior_waves": prior_waves,
        "transport": {
            "provider_id": "grok_cli_oauth",
            "model_id": "grok-4.5",
            "reasoning_effort": "high",
            "grok_binary_sha256": binary_sha,
            "operator_account_ref": "synthetic_operator_account" if auth_sha is not None else None,
            "oauth_auth_sha256": auth_sha,
        },
        "emergency": {
            "max_turns": 64,
            "deadline_ms": 1_800_000,
            "term_grace_ms": 1_000,
            "kill_grace_ms": 1_000,
        },
        "technical_limits": {
            "max_stdout_bytes": 16_777_216,
            "max_stderr_bytes": 1_048_576,
            "max_json_bytes": 16_777_216,
            "max_json_depth": 64,
            "max_json_nodes": 250_000,
            "max_prompt_bytes": 1_048_576,
            "max_compiled_prompt_bytes": 16_777_216,
            "max_prior_wave_bytes": 16_777_216,
            "max_total_prior_wave_bytes": 268_435_456,
            "max_prior_json_depth": 64,
            "max_prior_json_nodes": 250_000,
            "max_session_files": 64,
            "max_session_file_bytes": 33_554_432,
            "max_session_total_bytes": 67_108_864,
            "max_session_updates_bytes": 16_777_216,
            "max_session_update_line_bytes": 4_194_304,
        },
        "budget": {
            "pricing_policy_id": "synthetic_pricing.v1",
            "max_total_tokens": 1_000_000,
            "max_cost_usd_micros": 100_000_000,
            "input_token_cost_usd_micros_per_million": 1_000_000,
            "output_token_cost_usd_micros_per_million": 2_000_000,
        },
        "retention": {
            "policy_id": "adaptive_private_24h.v1",
            "ttl_seconds": 86_400,
            "deletion_receipt_required": True,
        },
        "approval": {"grant_id": grant_id},
        "authority": copy.deepcopy(AUTHORITY),
    }
    request_path = root / "request.json"
    _write_private(request_path, (canonical_json(request) + "\n").encode())
    return request, request_path


def _live_material(root: Path) -> tuple[Path, Path, str]:
    canonical_binary = root / "grok-real"
    canonical_binary.write_bytes(b"#!/bin/sh\nexit 0\n")
    os.chmod(canonical_binary, 0o700)
    binary_locator = root / "grok"
    binary_locator.symlink_to(canonical_binary.name)
    auth = root / "auth.json"
    _write_private(auth, b'{"synthetic":"oauth"}\n')
    return binary_locator, auth, _bytes_sha(canonical_binary.read_bytes())


class MutableClock:
    def __init__(self) -> None:
        self.value = 100.0

    def __call__(self) -> float:
        return self.value

    def advance(self, seconds: float) -> None:
        self.value += seconds


class FakeExecutor:
    def __init__(
        self,
        clock: MutableClock,
        raw: bytes,
        *,
        stderr: bytes = b"",
        spawn: bool = False,
        exit_code: int | None = 0,
        timed_out: bool = False,
        term_sent: bool = False,
        kill_sent: bool = False,
        execution_error: str = "none",
        technical_limit_kind: str | None = None,
        session_mutator: Any = None,
        session_tree_mutator: Any = None,
        extra_session_bytes: int = 0,
    ) -> None:
        self.clock = clock
        self.raw = raw
        self.stderr = stderr
        self.spawn = spawn
        self.exit_code = exit_code
        self.timed_out = timed_out
        self.term_sent = term_sent
        self.kill_sent = kill_sent
        self.execution_error = execution_error
        self.technical_limit_kind = technical_limit_kind
        self.session_mutator = session_mutator
        self.session_tree_mutator = session_tree_mutator
        self.extra_session_bytes = extra_session_bytes
        self.commands: list[list[str]] = []
        self.environments: list[dict[str, str]] = []
        self.deadlines: list[float] = []
        self.target_release_count = 0

    def __call__(
        self,
        command: list[str],
        *,
        cwd: Path,
        environment: dict[str, str],
        stdout_spool: Path,
        stderr_spool: Path,
        deadline_at: float,
        term_grace_ms: int,
        kill_grace_ms: int,
        max_stdout_bytes: int,
        max_stderr_bytes: int,
        session_tree_root: Path,
        session_updates_path: Path,
        max_session_files: int,
        max_session_file_bytes: int,
        max_session_total_bytes: int,
        max_session_updates_bytes: int,
        monotonic: Any,
        on_spawn: Any,
    ) -> ProcessResult:
        del (
            cwd,
            term_grace_ms,
            kill_grace_ms,
            max_stdout_bytes,
            max_stderr_bytes,
            max_session_files,
            max_session_file_bytes,
            max_session_total_bytes,
            max_session_updates_bytes,
            monotonic,
        )
        self.commands.append(list(command))
        self.environments.append(dict(environment))
        self.deadlines.append(deadline_at)
        child_pid = 42_424 if self.spawn else None
        process_group_id = child_pid
        kernel_birth_identity = "synthetic-kernel-birth-42424" if self.spawn else None
        identity_token = "a" * 64 if self.spawn else None
        if self.spawn:
            on_spawn(child_pid, process_group_id, kernel_birth_identity, identity_token)
            self.target_release_count += 1
            session_updates_path.parent.mkdir(parents=True, mode=0o700)
            created_parent = session_updates_path.parent
            while created_parent != session_tree_root:
                os.chmod(created_parent, 0o700)
                created_parent = created_parent.parent
            model_id = command[command.index("--model") + 1]
            session_id = command[command.index("--session-id") + 1]
            prompt_id = "synthetic-prompt-1"
            common = {"sessionId": session_id, "_meta": {"promptId": prompt_id}}
            usage_scalars = {
                "inputTokens": 100,
                "outputTokens": 50,
                "totalTokens": 150,
                "cachedReadTokens": 0,
                "reasoningTokens": 10,
                "modelCalls": 1,
                "apiDurationMs": 1_000,
            }
            updates = [
                {
                    "params": {
                        **common,
                        "update": {
                            "sessionUpdate": "user_message_chunk",
                            "_meta": {"modelId": model_id},
                        },
                    }
                },
                {
                    "params": {
                        **common,
                        "update": {
                            "sessionUpdate": "tool_call",
                            "toolCallId": "tool-1",
                            "status": "in_progress",
                        },
                    }
                },
                {
                    "params": {
                        **common,
                        "update": {
                            "sessionUpdate": "tool_call_update",
                            "toolCallId": "tool-1",
                            "status": "completed",
                            "rawOutput": {
                                "call_id": "provider-call-1",
                                "id": "tool-1",
                                "input": canonical_json({"query": "synthetic query", "limit": "100", "mode": "Latest"}),
                                "name": "x_keyword_search",
                            },
                        },
                    }
                },
                {
                    "params": {
                        **common,
                        "update": {
                            "sessionUpdate": "agent_message_chunk",
                            "content": {"type": "text", "text": self.raw.decode().strip()},
                        },
                    }
                },
                {
                    "params": {
                        **common,
                        "update": {
                            "sessionUpdate": "turn_completed",
                            "prompt_id": prompt_id,
                            "stop_reason": "end_turn",
                            "usage": {
                                **usage_scalars,
                                "modelUsage": {model_id: usage_scalars},
                                "numTurns": 1,
                            },
                        },
                    }
                },
            ]
            for index, event in enumerate(updates, start=1):
                update_kind = event["params"]["update"]["sessionUpdate"]
                event["method"] = "_x.ai/session/update" if update_kind == "turn_completed" else "session/update"
                event["timestamp"] = index
                event["params"]["_meta"] = {
                    **event["params"]["_meta"],
                    "eventId": f"{session_id}-{index}",
                    "agentTimestampMs": index,
                }
            if self.session_mutator is not None:
                self.session_mutator(updates)
            session_updates_path.write_text("".join(canonical_json(row) + "\n" for row in updates))
            os.chmod(session_updates_path, 0o600)
            if self.extra_session_bytes:
                extra = session_tree_root / "oversized-session-artifact.bin"
                extra.write_bytes(b"x" * self.extra_session_bytes)
                os.chmod(extra, 0o600)
            if self.session_tree_mutator is not None:
                self.session_tree_mutator(session_tree_root, session_updates_path)
            self.assert_session_tree_root = session_tree_root
        _write_private(stdout_spool, self.raw)
        _write_private(stderr_spool, self.stderr)
        self.clock.advance(2.5)
        return ProcessResult(
            exit_code=self.exit_code,
            timed_out=self.timed_out,
            term_sent=self.term_sent,
            kill_sent=self.kill_sent,
            process_spawn_attempted=self.spawn,
            child_pid=child_pid,
            process_group_id=process_group_id,
            kernel_birth_identity=kernel_birth_identity,
            process_identity_token=identity_token,
            process_group_cleanup_confirmed=True,
            execution_error_code=self.execution_error,
            technical_limit_kind=self.technical_limit_kind,
        )


def _completed_live_run(root: Path) -> tuple[Path, Path]:
    os.chmod(root, 0o700)
    binary, auth, binary_sha = _live_material(root)
    request, request_path = _build_request(root, binary_sha=binary_sha)
    approvals = root / "approvals"
    issue_live_grant(
        request_path=request_path,
        grant_root=approvals,
        auth_source=auth,
        wall_clock=lambda: FIXED_TIME,
    )
    clock = MutableClock()
    executor = FakeExecutor(
        clock,
        (canonical_json(_empty_result()) + "\n").encode(),
        spawn=True,
    )
    _, run_root = _run_adaptive_wave(
        request=request,
        execution_mode="live",
        runtime_root=root / "runtime",
        approval_root=approvals,
        binary=binary,
        auth_source=auth,
        executor=executor,
        monotonic=clock,
        wall_clock=lambda: FIXED_TIME,
    )
    return run_root, approvals


class AdaptiveGrokWaveRunnerTests(unittest.TestCase):
    def setUp(self) -> None:
        self._effective_prompt_policy_patch = mock.patch.object(
            runner,
            "DEFAULT_EFFECTIVE_PROMPT_POLICY",
            TEST_EFFECTIVE_PROMPT_POLICY,
        )
        self._effective_prompt_policy_patch.start()

    def tearDown(self) -> None:
        self._effective_prompt_policy_patch.stop()

    def test_request_is_generic_closed_and_has_no_business_count_limits(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            request, _ = _build_request(root)
            self.assertEqual(validate_request(request), [])
            request["target"]["lab_id"] = "another_lab"
            request["target"]["research_focus_id"] = "multimodal_foundation_training"
            self.assertEqual(validate_request(request), [])
            request["unexpected"] = True
            self.assertEqual(validate_request(request), ["request_shape_invalid"])

        schema_names = (
            "x.grok.adaptive_recall_wave.request.v2.schema.json",
            "x.grok.adaptive_recall_wave.result.v2.schema.json",
            "x.grok.adaptive_recall_wave.intent.v2.schema.json",
            "x.grok.adaptive_recall_wave.operator_receipt.v3.schema.json",
            "x.grok.adaptive_recall_wave.live_grant.v2.schema.json",
            "x.grok.adaptive_recall_wave.live_grant_consumption.v2.schema.json",
            "x.grok.adaptive_recall_wave.process_ledger.v2.schema.json",
            "x.grok.adaptive_recall_wave.deletion_journal.v1.schema.json",
            "x.grok.adaptive_recall_wave.deletion_receipt.v1.schema.json",
            "x.grok.adaptive_recall_wave.campaign_bridge.v1.schema.json",
            "x.grok.adaptive_recall_wave.effective_prompt_policy.v1.schema.json",
        )
        for name in schema_names:
            schema = json.loads((ROOT / "contracts" / name).read_text())
            self.assertEqual(schema["$schema"], "https://json-schema.org/draft/2020-12/schema")
            self.assertIs(schema["additionalProperties"], False)
        all_schema_text = "".join((ROOT / "contracts" / name).read_text() for name in schema_names)
        self.assertNotIn("maxItems", all_schema_text)

    def test_schema_top_level_keys_match_runtime_registries(self) -> None:
        mappings = {
            "x.grok.adaptive_recall_wave.request.v2.schema.json": runner._REQUEST_KEYS,
            "x.grok.adaptive_recall_wave.result.v2.schema.json": runner._RESULT_KEYS,
            "x.grok.adaptive_recall_wave.intent.v2.schema.json": runner._INTENT_KEYS,
            "x.grok.adaptive_recall_wave.operator_receipt.v3.schema.json": runner._RECEIPT_KEYS,
            "x.grok.adaptive_recall_wave.live_grant.v2.schema.json": runner._GRANT_KEYS,
            "x.grok.adaptive_recall_wave.live_grant_consumption.v2.schema.json": runner._CONSUMPTION_KEYS,
            "x.grok.adaptive_recall_wave.process_ledger.v2.schema.json": runner._PROCESS_LEDGER_KEYS,
            "x.grok.adaptive_recall_wave.deletion_journal.v1.schema.json": runner._DELETION_JOURNAL_KEYS,
            "x.grok.adaptive_recall_wave.deletion_receipt.v1.schema.json": runner._DELETION_RECEIPT_KEYS,
        }
        for name, keys in mappings.items():
            schema = json.loads((ROOT / "contracts" / name).read_text())
            self.assertEqual(set(schema["required"]), keys, name)
            self.assertEqual(set(schema["properties"]), keys, name)
        receipt_schema = json.loads(
            (ROOT / "contracts/x.grok.adaptive_recall_wave.operator_receipt.v3.schema.json").read_text()
        )
        receipt_v2_schema = json.loads(
            (ROOT / "contracts/x.grok.adaptive_recall_wave.operator_receipt.v2.schema.json").read_text()
        )
        self.assertEqual(set(receipt_v2_schema["$defs"]["command_binding"]["required"]), runner._COMMAND_BINDING_KEYS)
        self.assertEqual(set(receipt_v2_schema["properties"]["process"]["required"]), runner._PROCESS_KEYS)
        self.assertEqual(set(receipt_schema["properties"]["session_proof"]["required"]), runner._SESSION_PROOF_KEYS)
        self.assertEqual(
            set(receipt_schema["$defs"]["reconciliation"]["required"]),
            runner._RECONCILIATION_RECEIPT_KEYS,
        )
        result_schema = json.loads((ROOT / "contracts/x.grok.adaptive_recall_wave.result.v2.schema.json").read_text())
        self.assertEqual(set(result_schema["$defs"]["evidence"]["required"]), runner._EVIDENCE_KEYS)
        self.assertEqual(set(result_schema["$defs"]["support_claim"]["required"]), runner._SUPPORT_CLAIM_KEYS)
        request_schema = json.loads((ROOT / "contracts/x.grok.adaptive_recall_wave.request.v2.schema.json").read_text())
        self.assertEqual(
            set(request_schema["properties"]["technical_limits"]["required"]),
            runner._TECHNICAL_LIMIT_KEYS,
        )

    def test_production_effective_prompt_policy_owns_exact_openai_and_google_deepmind_waves(self) -> None:
        policy = json.loads(PRODUCTION_EFFECTIVE_PROMPT_POLICY.read_text())
        assert_schema_valid(policy, "x.grok.adaptive_recall_wave.effective_prompt_policy.v1.schema.json")
        synthetic = [entry for entry in policy["entries"] if entry["target"]["lab_id"] == "synthetic_lab"]
        self.assertEqual(len(synthetic), 1)
        self.assertEqual(synthetic[0]["authority"], "fixture_only")
        openai_target = {
            "lab_id": "openai",
            "research_focus_id": "pretraining",
            "scope": (
                "Public professional evidence of current or historical OpenAI affiliation and current or historical "
                "pre-training or base-model training relevance."
            ),
        }
        google_deepmind_target = {
            "lab_id": "google_deepmind",
            "research_focus_id": "pretraining",
            "scope": (
                "Public professional evidence of current or historical Google DeepMind affiliation and current or "
                "historical pre-training or base-model training relevance."
            ),
        }
        live_entries = [entry for entry in policy["entries"] if entry["authority"] == "live_authorized"]
        openai_prompt_paths = sorted(
            (ROOT / "prompts/live-exploration").glob("2026-07-14-openai-pretrain-recall-wave*.md")
        )
        google_deepmind_prompt_paths = sorted(
            (ROOT / "prompts/live-exploration").glob("2026-07-15-google-deepmind-pretraining-recall-wave*.md")
        )
        openai_entries = [entry for entry in live_entries if entry["target"]["lab_id"] == "openai"]
        google_deepmind_entries = [
            entry for entry in live_entries if entry["target"]["lab_id"] == "google_deepmind"
        ]
        self.assertEqual(len(openai_entries), len(openai_prompt_paths), 7)
        self.assertEqual(len(google_deepmind_entries), len(google_deepmind_prompt_paths), 3)
        self.assertEqual(len(live_entries), 10)
        self.assertEqual({canonical_json(entry["target"]) for entry in openai_entries}, {canonical_json(openai_target)})
        self.assertEqual(
            {canonical_json(entry["target"]) for entry in google_deepmind_entries},
            {canonical_json(google_deepmind_target)},
        )
        self.assertEqual(
            {entry["source_prompt_sha256"] for entry in openai_entries},
            {_bytes_sha(path.read_bytes()) for path in openai_prompt_paths},
        )
        self.assertEqual(
            {entry["source_prompt_sha256"] for entry in google_deepmind_entries},
            {_bytes_sha(path.read_bytes()) for path in google_deepmind_prompt_paths},
        )
        with mock.patch.object(runner, "DEFAULT_EFFECTIVE_PROMPT_POLICY", PRODUCTION_EFFECTIVE_PROMPT_POLICY):
            with tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                os.chmod(root, 0o700)
                binary, auth, binary_sha = _live_material(root)
                request, request_path = _build_request(root, binary_sha=binary_sha)
                with self.assertRaisesRegex(PermissionError, "effective_prompt_target_not_approved"):
                    issue_live_grant(
                        request_path=request_path,
                        grant_root=root / "fixture-rejected-approvals",
                        auth_source=auth,
                        wall_clock=lambda: FIXED_TIME,
                    )
                prompt_raw = openai_prompt_paths[0].read_bytes()
                _write_private(Path(request["prompt_source"]["path"]), prompt_raw)
                request["prompt_source"]["sha256"] = _bytes_sha(prompt_raw)
                request["target"] = openai_target
                _write_private(request_path, (canonical_json(request) + "\n").encode())
                grant, _ = issue_live_grant(
                    request_path=request_path,
                    grant_root=root / "approvals",
                    auth_source=auth,
                    wall_clock=lambda: FIXED_TIME,
                )
                self.assertEqual(grant["effective_prompt_policy_entry_id"], "openai_pretraining_recall_wave1.v1")

                gdm_prompt_raw = google_deepmind_prompt_paths[0].read_bytes()
                _write_private(Path(request["prompt_source"]["path"]), gdm_prompt_raw)
                request["prompt_source"]["sha256"] = _bytes_sha(gdm_prompt_raw)
                request["target"] = openai_target
                _write_private(request_path, (canonical_json(request) + "\n").encode())
                with self.assertRaisesRegex(PermissionError, "effective_prompt_target_not_approved"):
                    issue_live_grant(
                        request_path=request_path,
                        grant_root=root / "gdm-prompt-wrong-target",
                        auth_source=auth,
                        wall_clock=lambda: FIXED_TIME,
                    )

                wrong_prompt_raw = b"Unregistered Google DeepMind prompt.\n"
                _write_private(Path(request["prompt_source"]["path"]), wrong_prompt_raw)
                request["prompt_source"]["sha256"] = _bytes_sha(wrong_prompt_raw)
                request["target"] = google_deepmind_target
                _write_private(request_path, (canonical_json(request) + "\n").encode())
                with self.assertRaisesRegex(PermissionError, "effective_prompt_target_not_approved"):
                    issue_live_grant(
                        request_path=request_path,
                        grant_root=root / "gdm-wrong-prompt",
                        auth_source=auth,
                        wall_clock=lambda: FIXED_TIME,
                    )

                _write_private(Path(request["prompt_source"]["path"]), gdm_prompt_raw)
                request["prompt_source"]["sha256"] = _bytes_sha(gdm_prompt_raw)
                _write_private(request_path, (canonical_json(request) + "\n").encode())
                grant, _ = issue_live_grant(
                    request_path=request_path,
                    grant_root=root / "gdm-approvals",
                    auth_source=auth,
                    wall_clock=lambda: FIXED_TIME,
                )
                self.assertEqual(
                    grant["effective_prompt_policy_entry_id"],
                    "google_deepmind_pretraining_recall_wave1.v1",
                )

    def test_effective_prompt_binding_survives_unrelated_append_and_purge_replay(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            run_root, approvals = _completed_live_run(root)
            original_binding = json.loads(run_root.joinpath("operator-receipt.json").read_text())["command_binding"]
            policy = json.loads(TEST_EFFECTIVE_PROMPT_POLICY.read_text())
            policy["entries"].append(
                {
                    "policy_entry_id": "unrelated_lab_unrelated_focus.v1",
                    "target": {
                        "lab_id": "unrelated_lab",
                        "research_focus_id": "unrelated_focus",
                        "scope": "Public professional evidence for an unrelated additive registry row.",
                    },
                    "source_prompt_sha256": "f" * 64,
                    "authority": "live_authorized",
                }
            )
            extended_policy = root / "extended-effective-prompt-policy.json"
            _write_private(extended_policy, (canonical_json(policy) + "\n").encode())
            with mock.patch.object(runner, "DEFAULT_EFFECTIVE_PROMPT_POLICY", extended_policy):
                self.assertEqual(validate_operator_bundle(run_root, approval_root=approvals), [])
                replayed_binding = runner._approved_effective_prompt_binding(
                    json.loads(run_root.joinpath("operator-request.json").read_text())
                )
                self.assertEqual(
                    replayed_binding.policy_sha256,
                    original_binding["effective_prompt_policy_sha256"],
                )
                deletion_receipts = purge_expired_adaptive_runs(
                    runtime_root=root / "runtime",
                    deletion_root=root / "deletions",
                    approval_root=approvals,
                    wall_clock=lambda: FIXED_TIME + timedelta(days=2),
                )
            self.assertEqual(len(deletion_receipts), 1)
            self.assertFalse(run_root.exists())

    def test_prior_waves_are_sha_bound_casefold_unique_and_exclusion_only(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            request, _ = _build_request(root, wave_count=120)
            handles, bindings = load_prior_handles(request)
            self.assertEqual(len(handles), 120)
            self.assertEqual(len(bindings), 120)
            self.assertEqual(len({handle.casefold() for handle in handles}), 120)
            self.assertTrue(all("handle_set_sha256" in row for row in bindings))
            prompt = compile_prompt("Synthetic base prompt.", request["target"], handles)
            self.assertIn("exclusion rules only", prompt)
            self.assertIn("not evidence, seeds, ranking inputs, or a business stop condition", prompt)
            self.assertIn("Do not impose a candidate, observation, query, or X-tool-call business quota", prompt)

            first = Path(request["prior_waves"][0]["path"])
            duplicate = {"candidates": [{"handle": "Same"}, {"handle": "sAME"}]}
            duplicate_raw = (canonical_json(duplicate) + "\n").encode()
            _write_private(first, duplicate_raw)
            request["prior_waves"][0]["sha256"] = _bytes_sha(duplicate_raw)
            with self.assertRaisesRegex(Exception, "prior_wave_casefold_handle_duplicate"):
                load_prior_handles(request)

    def test_model_result_arrays_are_unbounded_but_casefold_and_nullable_rules_are_strict(self) -> None:
        result = _empty_result()
        result["candidates"] = [_candidate(f"fixture_{index:04d}") for index in range(2_000)]
        result["counts"]["candidates_retained"] = 2_000
        result["local_reconciliation"]["candidate_records_validated"] = 2_000
        self.assertEqual(validate_model_result(result), [])
        result["candidates"][1]["handle"] = result["candidates"][0]["handle"].upper()
        result["candidates"][1]["profile_url"] = f"https://profiles.invalid/{result['candidates'][1]['handle']}"
        self.assertTrue(any(error.startswith("candidate_handle_duplicate") for error in validate_model_result(result)))
        result["candidates"][1] = _candidate("fixture_0001")
        result["candidates"][0]["bio_excerpt"] = ""
        self.assertTrue(any(error.startswith("candidate_value_invalid") for error in validate_model_result(result)))

    def test_current_evidence_is_bound_to_subject_author_url_and_post_id(self) -> None:
        result = _empty_result()
        candidate = _candidate("TargetPerson")
        candidate.update(
            {
                "target_lab_affiliation_state": "current",
                "pretraining_experience_state": "current",
                "confidence": "high",
            }
        )
        evidence = {
            "kind": "post",
            "relationship": "self",
            "subject_handle": "TargetPerson",
            "author_handle": "TargetPerson",
            "post_id": "123456",
            "url": "https://x.com/TargetPerson/status/123456",
            "published_at": "2026-07-01T00:00:00Z",
            "excerpt": "I currently work on pretraining at the target lab.",
            "thread_relation": "self_post",
            "supports": [
                {"dimension": "target_lab_affiliation_state", "asserted_value": "current"},
                {"dimension": "pretraining_experience_state", "asserted_value": "current"},
            ],
        }
        candidate["evidence"] = [evidence]
        result["candidates"] = [candidate]
        result["counts"] = {"observations_inspected_reported": 1, "candidates_retained": 1}
        result["local_reconciliation"].update(
            {
                "candidate_records_validated": 1,
                "evidence_items_validated": 1,
                "post_urls_structurally_validated": 1,
            }
        )
        self.assertEqual(validate_model_result(result), [])
        reply_classification = copy.deepcopy(evidence)
        reply_classification["thread_relation"] = "reply"
        self.assertEqual(
            runner._evidence_source_sha256(evidence, "TargetPerson"),
            runner._evidence_source_sha256(reply_classification, "TargetPerson"),
        )
        for field, value in (
            ("subject_handle", "OtherPerson"),
            ("author_handle", "OtherPerson"),
            ("post_id", "999999"),
            ("url", "https://x.com/OtherPerson/status/123456"),
        ):
            changed = copy.deepcopy(result)
            changed["candidates"][0]["evidence"][0][field] = value
            self.assertTrue(
                any(error.startswith("evidence_value_invalid") for error in validate_model_result(changed)),
                field,
            )
        legacy_supports = copy.deepcopy(result)
        legacy_supports["candidates"][0]["evidence"][0]["supports"] = ["target_lab_affiliation_state"]
        self.assertIn("evidence_value_invalid:0:0", validate_model_result(legacy_supports))
        missing_relation = copy.deepcopy(result)
        missing_relation["candidates"][0]["evidence"][0]["thread_relation"] = None
        self.assertIn("evidence_value_invalid:0:0", validate_model_result(missing_relation))

    def test_bio_requires_null_thread_relation_and_typed_temporal_support(self) -> None:
        result = _empty_result()
        candidate = _candidate("BioPerson")
        candidate.update(
            {
                "bio_excerpt": "Researcher at the target lab.",
                "target_lab_affiliation_state": "current",
                "confidence": "medium",
                "evidence": [
                    {
                        "kind": "bio",
                        "relationship": "self",
                        "subject_handle": "BioPerson",
                        "author_handle": "BioPerson",
                        "post_id": None,
                        "url": "https://profiles.invalid/BioPerson",
                        "published_at": None,
                        "excerpt": "Researcher at the target lab.",
                        "thread_relation": None,
                        "supports": [
                            {"dimension": "target_lab_affiliation_state", "asserted_value": "current"}
                        ],
                    }
                ],
            }
        )
        result["candidates"] = [candidate]
        result["counts"] = {"observations_inspected_reported": 1, "candidates_retained": 1}
        result["local_reconciliation"].update(
            {
                "candidate_records_validated": 1,
                "evidence_items_validated": 1,
            }
        )
        self.assertEqual(validate_model_result(result), [])
        assert_schema_valid(result, "x.grok.adaptive_recall_wave.result.v2.schema.json")
        candidate["evidence"][0]["thread_relation"] = "self_post"
        self.assertIn("evidence_value_invalid:0:0", validate_model_result(result))
        with self.assertRaises(MiniDraft202012Error):
            assert_schema_valid(result, "x.grok.adaptive_recall_wave.result.v2.schema.json")

    def test_prior_overlap_requires_mechanically_new_evidence_or_temporal_state(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            request, _ = _build_request(root)
            evidence = {
                "kind": "post",
                "relationship": "self",
                "subject_handle": "PriorPerson",
                "author_handle": "PriorPerson",
                "post_id": "123456",
                "url": "https://x.com/PriorPerson/status/123456",
                "published_at": "2026-07-01T00:00:00Z",
                "excerpt": "Synthetic pretraining evidence.",
                "supports": ["pretraining_experience_state"],
            }
            prior = {
                "candidates": [
                    {
                        "handle": "PriorPerson",
                        "target_lab_affiliation_state": "ambiguous",
                        "pretraining_experience_state": "historical",
                        "evidence": [evidence],
                    }
                ]
            }
            raw = (canonical_json(prior) + "\n").encode()
            path = root / "prior.json"
            _write_private(path, raw)
            request["prior_waves"] = [{"wave_id": "prior", "path": str(path), "sha256": _bytes_sha(raw)}]
            _, _, facts = load_prior_context(request)
            self.assertEqual(
                facts["priorperson"].pretraining_experience_latest_at,
                datetime(2026, 7, 1, tzinfo=UTC),
            )
            evidence = {
                **evidence,
                "thread_relation": "self_post",
                "supports": [
                    {"dimension": "pretraining_experience_state", "asserted_value": "historical"}
                ],
            }
            result = _empty_result()
            zero_evidence = _candidate("PriorPerson")
            zero_evidence["overlap_status"] = "prior_material_update"
            result["candidates"] = [zero_evidence]
            result["counts"] = {"observations_inspected_reported": 0, "candidates_retained": 1}
            result["local_reconciliation"]["candidate_records_validated"] = 1
            self.assertIn(
                "prior_overlap_without_material_update:0",
                validate_model_result(result, prior_candidates=facts),
            )
            candidate = _candidate("PriorPerson")
            candidate["pretraining_experience_state"] = "historical"
            candidate["evidence"] = [evidence]
            candidate["overlap_status"] = "prior_material_update"
            result["candidates"] = [candidate]
            result["counts"] = {"observations_inspected_reported": 1, "candidates_retained": 1}
            result["local_reconciliation"].update(
                {
                    "candidate_records_validated": 1,
                    "evidence_items_validated": 1,
                    "post_urls_structurally_validated": 1,
                }
            )
            self.assertIn(
                "prior_overlap_without_material_update:0",
                validate_model_result(result, prior_candidates=facts),
            )
            candidate["target_lab_affiliation_state"] = "current"
            excerpt_only = copy.deepcopy(evidence)
            excerpt_only["excerpt"] = "Model-edited prose for the same immutable source."
            excerpt_only["supports"] = [
                {"dimension": "target_lab_affiliation_state", "asserted_value": "current"},
                {"dimension": "pretraining_experience_state", "asserted_value": "historical"},
            ]
            candidate["evidence"] = [excerpt_only]
            self.assertIn(
                "prior_overlap_without_material_update:0",
                validate_model_result(result, prior_candidates=facts),
            )
            evidence2 = copy.deepcopy(evidence)
            evidence2["post_id"] = "234567"
            evidence2["url"] = "https://x.com/PriorPerson/status/234567"
            evidence2["published_at"] = "2026-07-02T00:00:00Z"
            evidence2["supports"] = [
                {"dimension": "target_lab_affiliation_state", "asserted_value": "current"},
                {"dimension": "pretraining_experience_state", "asserted_value": "historical"},
            ]
            evidence2["excerpt"] = "Synthetic current affiliation and pretraining evidence."
            candidate["evidence"] = [evidence, evidence2]
            result["counts"]["observations_inspected_reported"] = 2
            result["local_reconciliation"].update(
                {
                    "evidence_items_validated": 2,
                    "post_urls_structurally_validated": 2,
                }
            )
            self.assertEqual(validate_model_result(result, prior_candidates=facts), [])

    def test_completed_keyword_queries_mechanically_bind_candidate_surface_attempts(self) -> None:
        cases = (
            (
                "authored_post",
                "x_keyword_search",
                {"query": "from:TargetPerson pretraining -filter:replies", "limit": "100", "mode": "Latest"},
                "authored_post",
            ),
            (
                "authored_reply",
                "x_keyword_search",
                {"query": "from:TargetPerson pretraining filter:replies", "limit": "100", "mode": "Latest"},
                "authored_reply",
            ),
            (
                "global_reply",
                "x_keyword_search",
                {"query": "pretraining filter:replies", "limit": "100", "mode": "Latest"},
                None,
            ),
            (
                "multi_handle_reply",
                "x_keyword_search",
                {
                    "query": "(from:TargetPerson OR from:OtherPerson) pretraining filter:replies",
                    "limit": "100",
                    "mode": "Latest",
                },
                None,
            ),
            (
                "semantic_from_is_not_mechanical_keyword_coverage",
                "x_semantic_search",
                {"query": "from:TargetPerson pretraining filter:replies", "limit": "100"},
                None,
            ),
        )
        for label, tool_name, arguments, expected_surface in cases:
            with self.subTest(label=label), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                os.chmod(root, 0o700)
                binary, auth, binary_sha = _live_material(root)
                request, request_path = _build_request(root, binary_sha=binary_sha)
                approvals = root / "approvals"
                issue_live_grant(
                    request_path=request_path,
                    grant_root=approvals,
                    auth_source=auth,
                    wall_clock=lambda: FIXED_TIME,
                )
                result = _empty_result()
                result["native_x_tool_provenance"].update(
                    {
                        "tools_reported": [tool_name],
                        "queries": [arguments["query"]],
                    }
                )
                result["local_reconciliation"]["tool_counts"] = {tool_name: 1}
                result["candidates"] = [_candidate("TargetPerson", profile_host="x.com")]
                result["counts"]["candidates_retained"] = 1
                result["local_reconciliation"]["candidate_records_validated"] = 1

                def set_tool_call(events: list[dict[str, Any]]) -> None:
                    events[2]["params"]["update"]["rawOutput"]["name"] = tool_name
                    events[2]["params"]["update"]["rawOutput"]["input"] = canonical_json(arguments)

                fake = FakeExecutor(
                    MutableClock(),
                    (canonical_json(result) + "\n").encode(),
                    spawn=True,
                    session_mutator=set_tool_call,
                )
                receipt, run_root = _run_adaptive_wave(
                    request=request,
                    execution_mode="live",
                    runtime_root=root / "runtime",
                    approval_root=approvals,
                    binary=binary,
                    auth_source=auth,
                    executor=fake,
                    monotonic=fake.clock,
                    wall_clock=lambda: FIXED_TIME,
                )
                self.assertEqual(receipt["status"], "completed")
                assert_schema_valid(receipt, "x.grok.adaptive_recall_wave.operator_receipt.v3.schema.json")
                query_sha256 = runner.canonical_sha256({"tool_name": tool_name, "arguments": arguments})
                expected_attempts = (
                    [
                        {
                            "handle_key": "targetperson",
                            "surface": expected_surface,
                            "query_argument_sha256": query_sha256,
                        }
                    ]
                    if expected_surface is not None
                    else []
                )
                self.assertEqual(receipt["session_proof"]["candidate_surface_attempts"], expected_attempts)
                coverage = receipt["reconciliation"]["candidate_surface_coverage"]
                self.assertEqual(len(coverage), 1)
                for surface in ("authored_post", "authored_reply"):
                    expected_hashes = [query_sha256] if surface == expected_surface else []
                    self.assertEqual(
                        coverage[0][surface],
                        {"attempted": bool(expected_hashes), "query_argument_sha256s": expected_hashes},
                    )
                self.assertEqual(validate_operator_bundle(run_root, approval_root=approvals), [])

                if label == "authored_reply":
                    inconsistent = copy.deepcopy(receipt)
                    inconsistent["session_proof"]["candidate_surface_attempts"][0]["surface"] = "authored_post"
                    self.assertIn(
                        "receipt_candidate_surface_reconciliation_invalid",
                        validate_operator_receipt(inconsistent),
                    )
                    forged = copy.deepcopy(inconsistent)
                    forged_coverage = forged["reconciliation"]["candidate_surface_coverage"][0]
                    forged_coverage["authored_post"] = {
                        "attempted": True,
                        "query_argument_sha256s": [query_sha256],
                    }
                    forged_coverage["authored_reply"] = {
                        "attempted": False,
                        "query_argument_sha256s": [],
                    }
                    self.assertEqual(validate_operator_receipt(forged), [])
                    self.assertIn(
                        "session_proof_replay_mismatch",
                        validate_operator_bundle(
                            run_root,
                            approval_root=approvals,
                            receipt_override=forged,
                        ),
                    )

    def test_live_run_stages_binary_isolates_auth_uses_closed_tools_and_replays_bundle(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            os.chmod(root, 0o700)
            binary, auth, binary_sha = _live_material(root)
            request, request_path = _build_request(root, binary_sha=binary_sha)
            wrong_auth = root / "wrong-auth.json"
            _write_private(wrong_auth, b'{"synthetic":"different-oauth"}\n')
            with self.assertRaisesRegex(AdaptiveWaveValidationError, "live_grant_request_invalid"):
                issue_live_grant(
                    request_path=request_path,
                    grant_root=root / "wrong-auth-approvals",
                    auth_source=wrong_auth,
                    wall_clock=lambda: FIXED_TIME,
                )
            approvals = root / "approvals"
            issue_live_grant(
                request_path=request_path,
                grant_root=approvals,
                auth_source=auth,
                wall_clock=lambda: FIXED_TIME,
            )
            clock = MutableClock()
            raw = (canonical_json(_empty_result()) + "\n").encode()
            fake = FakeExecutor(clock, raw, spawn=True)
            receipt, run_root = _run_adaptive_wave(
                request=request,
                execution_mode="live",
                runtime_root=root / "runtime",
                approval_root=approvals,
                binary=binary,
                auth_source=auth,
                executor=fake,
                monotonic=clock,
                wall_clock=lambda: FIXED_TIME,
                run_id="grok_wave_live_22222222222222222222222222222222",
                session_id="33333333-3333-4333-8333-333333333333",
            )
            self.assertEqual(receipt["status"], "completed")
            self.assertEqual(receipt["process"]["elapsed_ms"], 2_500)
            self.assertEqual(fake.deadlines, [1_900.0])
            command = fake.commands[0]
            # Grok CLI 0.2.99 does not map the hosted x_* tool names through
            # its built-in --tools allowlist.  Passing them removes native X
            # capability in a real OAuth session, so the runner relies on the
            # explicit web/local deny surface plus replayed native-X proof.
            self.assertNotIn("--tools", command)
            self.assertEqual(command[command.index("--permission-mode") + 1], "dontAsk")
            self.assertNotIn("--always-approve", command)
            self.assertNotIn("bypassPermissions", command)
            self.assertNotIn("--json-schema", command)
            self.assertNotIn("Find a broad", " ".join(command))
            self.assertTrue(Path(command[0]).is_relative_to(run_root / "executable"))
            self.assertEqual(stat.S_IMODE(Path(command[0]).stat().st_mode), 0o700)
            environment = fake.environments[0]
            self.assertEqual(environment["HOME"], environment["GROK_HOME"])
            self.assertNotIn("OPENAI_API_KEY", environment)
            self.assertNotIn("HTTP_PROXY", environment)
            self.assertFalse((run_root / "ephemeral-home/auth.json").exists())
            self.assertEqual(validate_operator_bundle(run_root, approval_root=approvals), [])
            self.assertEqual(len(list(approvals.glob("grant-*.json"))), 1)
            self.assertEqual(len(list(approvals.glob("consumption-*.json"))), 1)
            updates_path = run_root / "session-updates.jsonl"
            original_updates = updates_path.read_bytes()
            for mutation in ("tool", "usage", "terminal"):
                events = [json.loads(line) for line in original_updates.splitlines()]
                if mutation == "tool":
                    events[2]["params"]["update"]["rawOutput"]["name"] = "web_search"
                elif mutation == "usage":
                    events[-1]["params"]["update"]["usage"]["totalTokens"] += 1
                else:
                    events[-1]["params"]["update"]["stop_reason"] = "max_turns"
                _write_private(
                    updates_path,
                    "".join(canonical_json(event) + "\n" for event in events).encode(),
                )
                self.assertIn(
                    "session_proof_replay_mismatch",
                    validate_operator_bundle(run_root, approval_root=approvals),
                    mutation,
                )
            _write_private(updates_path, original_updates)
            staged_binary = run_root / "executable/grok"
            staged_binary.write_bytes(b"tampered executable")
            os.chmod(staged_binary, 0o700)
            self.assertIn(
                "staged_binary_request_hash_mismatch",
                validate_operator_bundle(run_root, approval_root=approvals),
            )

    def test_campaign_bridge_is_source_bound_but_always_blocked_without_native_payloads(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            run_root, approvals = _completed_live_run(root)
            bridge = build_blocked_campaign_bridge(run_root, approval_root=approvals)

            self.assertEqual(bridge["campaign_admission"], "blocked")
            self.assertEqual(bridge["source_payload_status"], "replay_unavailable")
            self.assertEqual(bridge["reason_code"], REASON_CODE)
            self.assertIsNone(bridge["wave_input"])
            self.assertEqual(
                bridge["source_payload_binding"],
                {"captured_payload_count": 0, "captured_payload_sha256s": []},
            )
            self.assertEqual(bridge["authority"], ZERO_AUTHORITY)
            self.assertNotIn("candidates", canonical_json(bridge))
            binding = bridge["source_binding"]
            self.assertEqual(
                binding["operator_request_artifact_sha256"],
                _bytes_sha((run_root / "operator-request.json").read_bytes()),
            )
            self.assertEqual(
                binding["operator_receipt_artifact_sha256"],
                _bytes_sha((run_root / "operator-receipt.json").read_bytes()),
            )
            self.assertEqual(
                binding["sanitized_result_artifact_sha256"],
                _bytes_sha((run_root / "sanitized.json").read_bytes()),
            )
            self.assertEqual(
                binding["session_transcript_artifact_sha256"],
                _bytes_sha((run_root / "session-updates.jsonl").read_bytes()),
            )
            self.assertEqual(binding["bridge_schema_sha256"], contract_schema_sha256(BRIDGE_SCHEMA_FILE))
            assert_schema_valid(bridge, BRIDGE_SCHEMA_FILE)

    def test_campaign_bridge_fails_before_output_on_bundle_tamper_or_missing_source_artifacts(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            run_root, approvals = _completed_live_run(root)
            receipt_path = run_root / "operator-receipt.json"
            receipt = json.loads(receipt_path.read_text())
            receipt["status"] = "process_failed"
            _write_private(receipt_path, (canonical_json(receipt) + "\n").encode())
            with self.assertRaisesRegex(
                AdaptiveCampaignBridgeError,
                "adaptive_bundle_invalid_for_campaign_bridge",
            ):
                build_blocked_campaign_bridge(run_root, approval_root=approvals)

        for missing_name in ("sanitized.json", "session-updates.jsonl"):
            with self.subTest(missing_name=missing_name), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                run_root, approvals = _completed_live_run(root)
                (run_root / missing_name).unlink()
                with self.assertRaisesRegex(
                    AdaptiveCampaignBridgeError,
                    "adaptive_bundle_invalid_for_campaign_bridge",
                ):
                    build_blocked_campaign_bridge(run_root, approval_root=approvals)

    def test_campaign_bridge_holds_lease_and_rejects_same_path_bundle_swap(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            run_root, approvals = _completed_live_run(root)
            run_id = run_root.name
            original_location = root / "bundle-a"
            replacement_location = root / "bundle-b"
            os.rename(run_root, original_location)

            binary = root / "grok"
            auth = root / "auth.json"
            binary_sha = _bytes_sha((root / "grok-real").read_bytes())
            replacement_request, replacement_request_path = _build_request(
                root,
                binary_sha=binary_sha,
                grant_id="synthetic_live_grant_002",
            )
            replacement_request["request_id"] = "xwave_req_22222222222222222222222222222222"
            _write_private(
                replacement_request_path,
                (canonical_json(replacement_request) + "\n").encode(),
            )
            issue_live_grant(
                request_path=replacement_request_path,
                grant_root=approvals,
                auth_source=auth,
                wall_clock=lambda: FIXED_TIME,
            )
            replacement_clock = MutableClock()
            _, replacement_run = _run_adaptive_wave(
                request=replacement_request,
                execution_mode="live",
                runtime_root=root / "runtime",
                approval_root=approvals,
                binary=binary,
                auth_source=auth,
                executor=FakeExecutor(
                    replacement_clock,
                    (canonical_json(_empty_result()) + "\n").encode(),
                    spawn=True,
                ),
                monotonic=replacement_clock,
                wall_clock=lambda: FIXED_TIME,
                run_id=run_id,
            )
            os.rename(replacement_run, replacement_location)
            os.rename(original_location, run_root)

            os.rename(run_root, original_location)
            os.rename(replacement_location, run_root)
            self.assertEqual(validate_operator_bundle(run_root, approval_root=approvals), [])
            os.rename(run_root, replacement_location)
            os.rename(original_location, run_root)

            original_validate = validate_operator_bundle
            validation_calls = 0

            def validate_then_swap(path: Path, *, approval_root: Path) -> list[str]:
                nonlocal validation_calls
                validation_calls += 1
                if validation_calls == 2:
                    os.rename(run_root, original_location)
                    os.rename(replacement_location, run_root)
                return original_validate(path, approval_root=approval_root)

            with mock.patch(
                "x_first.adaptive_recall_campaign_bridge.validate_operator_bundle",
                side_effect=validate_then_swap,
            ):
                with self.assertRaisesRegex(
                    AdaptiveCampaignBridgeError,
                    "adaptive_bundle_changed_during_campaign_bridge",
                ):
                    build_blocked_campaign_bridge(run_root, approval_root=approvals)
            self.assertEqual(validation_calls, 2)

    def test_campaign_bridge_schema_rejects_admission_payload_and_authority_mutations(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            run_root, approvals = _completed_live_run(root)
            bridge = build_blocked_campaign_bridge(run_root, approval_root=approvals)
            mutations = (
                ("campaign_admission", "ready"),
                ("source_payload_status", "replay_available"),
                ("wave_input", {}),
                ("source_payload_binding", {"captured_payload_count": 1, "captured_payload_sha256s": ["0" * 64]}),
                ("authority", {**ZERO_AUTHORITY, "product_write_authorized": True}),
                ("candidates", []),
            )
            for field, value in mutations:
                with self.subTest(field=field):
                    mutated = copy.deepcopy(bridge)
                    mutated[field] = value
                    with self.assertRaises(MiniDraft202012Error):
                        assert_schema_valid(mutated, BRIDGE_SCHEMA_FILE)

    def test_bundle_recomputes_command_from_request_not_self_consistent_receipt(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            os.chmod(root, 0o700)
            _, request_path = _build_request(root)
            _, run_root = run_adaptive_grok_wave_fixture(
                request_path=request_path,
                runtime_root=root / "runtime",
            )
            intent_path = run_root / "operator-intent.json"
            receipt_path = run_root / "operator-receipt.json"
            intent = json.loads(intent_path.read_text())
            receipt = json.loads(receipt_path.read_text())
            for artifact in (intent, receipt):
                artifact["command_binding"]["model_id"] = "forged-model"
                artifact["command_binding"]["reasoning_effort"] = "low"
                artifact["command_binding"]["max_turns"] = 7
                artifact["command_binding"]["command_policy_sha256"] = runner.canonical_sha256(
                    runner._redacted_policy_from_bindings(artifact["input_binding"], artifact["command_binding"])
                )
            _write_private(intent_path, (canonical_json(intent) + "\n").encode())
            _write_private(receipt_path, (canonical_json(receipt) + "\n").encode())
            self.assertEqual(validate_operator_receipt(receipt), [])
            self.assertIn("command_binding_request_replay_mismatch", validate_operator_bundle(run_root))

    def test_live_grant_is_preissued_scope_bound_and_single_use(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            os.chmod(root, 0o700)
            binary, auth, binary_sha = _live_material(root)
            request, request_path = _build_request(root, binary_sha=binary_sha)
            approvals = root / "approvals"
            issue_live_grant(
                request_path=request_path,
                grant_root=approvals,
                auth_source=auth,
                wall_clock=lambda: FIXED_TIME,
            )
            raw = (canonical_json(_empty_result()) + "\n").encode()
            first = FakeExecutor(MutableClock(), raw, spawn=True)
            _run_adaptive_wave(
                request=request,
                execution_mode="live",
                runtime_root=root / "runtime",
                approval_root=approvals,
                binary=binary,
                auth_source=auth,
                executor=first,
                monotonic=first.clock,
                wall_clock=lambda: FIXED_TIME,
            )
            second = FakeExecutor(MutableClock(), raw, spawn=True)
            with self.assertRaisesRegex(PermissionError, "live_grant_already_consumed"):
                _run_adaptive_wave(
                    request=request,
                    execution_mode="live",
                    runtime_root=root / "runtime",
                    approval_root=approvals,
                    binary=binary,
                    auth_source=auth,
                    executor=second,
                    monotonic=second.clock,
                    wall_clock=lambda: FIXED_TIME,
                )
            self.assertEqual(second.commands, [])

    def test_grant_issuance_rejects_unregistered_target_and_base_prompt(self) -> None:
        for mutation in ("target", "prompt"):
            with self.subTest(mutation=mutation), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                os.chmod(root, 0o700)
                _, auth, binary_sha = _live_material(root)
                request, request_path = _build_request(root, binary_sha=binary_sha)
                if mutation == "target":
                    request["target"]["scope"] = "Find women in the synthetic lab."
                else:
                    prompt_raw = b"Find women researchers in the synthetic lab.\n"
                    _write_private(Path(request["prompt_source"]["path"]), prompt_raw)
                    request["prompt_source"]["sha256"] = _bytes_sha(prompt_raw)
                _write_private(request_path, (canonical_json(request) + "\n").encode())
                with self.assertRaisesRegex(PermissionError, "effective_prompt_target_not_approved"):
                    issue_live_grant(
                        request_path=request_path,
                        grant_root=root / "approvals",
                        auth_source=auth,
                        wall_clock=lambda: FIXED_TIME,
                    )
                self.assertFalse((root / "approvals").exists())

    def test_missing_expired_or_wrong_scope_grant_never_spawns_and_deletes_auth(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            os.chmod(root, 0o700)
            binary, auth, binary_sha = _live_material(root)
            request, request_path = _build_request(root, binary_sha=binary_sha)
            raw = (canonical_json(_empty_result()) + "\n").encode()
            fake = FakeExecutor(MutableClock(), raw, spawn=True)
            with self.assertRaises(PermissionError):
                _run_adaptive_wave(
                    request=request,
                    execution_mode="live",
                    runtime_root=root / "runtime-missing",
                    approval_root=root / "missing-approvals",
                    binary=binary,
                    auth_source=auth,
                    executor=fake,
                    monotonic=fake.clock,
                    wall_clock=lambda: FIXED_TIME,
                )
            self.assertEqual(fake.commands, [])
            self.assertFalse((root / "runtime-missing").exists())
            self.assertEqual(
                purge_expired_adaptive_runs(
                    runtime_root=root / "runtime-missing",
                    deletion_root=root / "missing-deletions",
                    approval_root=root / "missing-approvals",
                    wall_clock=lambda: FIXED_TIME + timedelta(days=2),
                ),
                [],
            )

            approvals = root / "approvals"
            issue_live_grant(
                request_path=request_path,
                grant_root=approvals,
                auth_source=auth,
                ttl_seconds=60,
                wall_clock=lambda: FIXED_TIME,
            )
            changed = copy.deepcopy(request)
            changed["target"]["scope"] = "Changed after grant issuance."
            wrong_scope = FakeExecutor(MutableClock(), raw, spawn=True)
            with self.assertRaisesRegex(PermissionError, "effective_prompt_target_not_approved"):
                _run_adaptive_wave(
                    request=changed,
                    execution_mode="live",
                    runtime_root=root / "runtime-scope",
                    approval_root=approvals,
                    binary=binary,
                    auth_source=auth,
                    executor=wrong_scope,
                    monotonic=wrong_scope.clock,
                    wall_clock=lambda: FIXED_TIME,
                )
            expired = FakeExecutor(MutableClock(), raw, spawn=True)
            with self.assertRaisesRegex(PermissionError, "preissued_live_grant_invalid"):
                _run_adaptive_wave(
                    request=request,
                    execution_mode="live",
                    runtime_root=root / "runtime-expired",
                    approval_root=approvals,
                    binary=binary,
                    auth_source=auth,
                    executor=expired,
                    monotonic=expired.clock,
                    wall_clock=lambda: FIXED_TIME + timedelta(seconds=61),
                )
            self.assertEqual(wrong_scope.commands + expired.commands, [])
            self.assertFalse((root / "runtime-scope").exists())
            self.assertFalse((root / "runtime-expired").exists())

    def test_preintent_binary_failure_leaves_no_run_root_or_purge_blocker(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            os.chmod(root, 0o700)
            binary, auth, binary_sha = _live_material(root)
            request, request_path = _build_request(root, binary_sha=binary_sha)
            approvals = root / "approvals"
            issue_live_grant(
                request_path=request_path,
                grant_root=approvals,
                auth_source=auth,
                wall_clock=lambda: FIXED_TIME,
            )
            binary.resolve().write_bytes(b"#!/bin/sh\nexit 1\n")
            os.chmod(binary.resolve(), 0o700)
            fake = FakeExecutor(MutableClock(), (canonical_json(_empty_result()) + "\n").encode(), spawn=True)
            runtime_root = root / "runtime"
            with self.assertRaisesRegex(AdaptiveWaveValidationError, "grok_binary_sha256_mismatch"):
                _run_adaptive_wave(
                    request=request,
                    execution_mode="live",
                    runtime_root=runtime_root,
                    approval_root=approvals,
                    binary=binary,
                    auth_source=auth,
                    executor=fake,
                    monotonic=fake.clock,
                    wall_clock=lambda: FIXED_TIME,
                )
            self.assertTrue(runtime_root.is_dir())
            self.assertEqual(list(runtime_root.iterdir()), [])
            self.assertEqual(
                purge_expired_adaptive_runs(
                    runtime_root=runtime_root,
                    deletion_root=root / "deletions",
                    approval_root=approvals,
                    wall_clock=lambda: FIXED_TIME + timedelta(days=2),
                ),
                [],
            )

    def test_non_json_prefix_is_retained_but_cannot_claim_completion(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            os.chmod(root, 0o700)
            request, _ = _build_request(root)
            clock = MutableClock()
            raw = b"MODEL NOTE\n" + (canonical_json(_empty_result()) + "\n").encode()
            fake = FakeExecutor(clock, raw)
            receipt, run_root = _run_adaptive_wave(
                request=request,
                execution_mode="fixture",
                runtime_root=root / "runtime",
                approval_root=root / "unused",
                binary=Path("fixture.invalid"),
                auth_source=None,
                executor=fake,
                monotonic=clock,
                wall_clock=lambda: FIXED_TIME,
            )
            self.assertEqual(receipt["status"], "structured_output_noncompliant")
            self.assertGreater(receipt["artifacts"]["non_json_prefix_bytes"], 0)
            self.assertTrue(receipt["artifacts"]["structured_output_contract_valid"])
            self.assertEqual(validate_operator_bundle(run_root), [])

    def test_json_structure_limit_is_technical_not_a_business_quota(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            os.chmod(root, 0o700)
            request, _ = _build_request(root)
            request["technical_limits"]["max_json_depth"] = 16
            nested: Any = "leaf"
            for _ in range(24):
                nested = [nested]
            raw = json.dumps({"too_deep": nested}).encode()
            fake = FakeExecutor(MutableClock(), raw)
            receipt, run_root = _run_adaptive_wave(
                request=request,
                execution_mode="fixture",
                runtime_root=root / "runtime",
                approval_root=root / "unused",
                binary=Path("fixture.invalid"),
                auth_source=None,
                executor=fake,
                monotonic=fake.clock,
                wall_clock=lambda: FIXED_TIME,
            )
            self.assertEqual(receipt["status"], "technical_limit_exceeded")
            self.assertEqual(receipt["process"]["technical_limit_kind"], "json_structure")
            self.assertEqual(validate_operator_bundle(run_root), [])

    def test_prompt_and_prior_inputs_enforce_byte_depth_and_node_ceilings(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            os.chmod(root, 0o700)
            request, _ = _build_request(root)
            prompt = Path(request["prompt_source"]["path"])
            oversized_prompt = b"p" * 65_537
            _write_private(prompt, oversized_prompt)
            request["prompt_source"]["sha256"] = _bytes_sha(oversized_prompt)
            request["technical_limits"]["max_prompt_bytes"] = 65_536
            with self.assertRaisesRegex(AdaptiveWaveValidationError, "bound_input_byte_ceiling_exceeded"):
                _run_adaptive_wave(
                    request=request,
                    execution_mode="fixture",
                    runtime_root=root / "runtime-prompt",
                    approval_root=root / "unused",
                    binary=Path("fixture.invalid"),
                    auth_source=None,
                    executor=runner.OfflineFixtureExecutor(),
                    monotonic=time.monotonic,
                    wall_clock=lambda: FIXED_TIME,
                )

            request, _ = _build_request(root)
            nested: Any = {"candidates": []}
            for _ in range(20):
                nested = {"nested": nested}
            nested_raw = (canonical_json(nested) + "\n").encode()
            nested_path = root / "nested-prior.json"
            _write_private(nested_path, nested_raw)
            request["prior_waves"] = [{"wave_id": "nested", "path": str(nested_path), "sha256": _bytes_sha(nested_raw)}]
            request["technical_limits"]["max_prior_json_depth"] = 16
            with self.assertRaisesRegex(AdaptiveWaveValidationError, "json_depth_ceiling_exceeded"):
                load_prior_context(request)

            many_nodes = {"candidates": [{"handle": "same"} for _ in range(10_001)]}
            nodes_raw = (canonical_json(many_nodes) + "\n").encode()
            nodes_path = root / "nodes-prior.json"
            _write_private(nodes_path, nodes_raw)
            request["prior_waves"] = [{"wave_id": "nodes", "path": str(nodes_path), "sha256": _bytes_sha(nodes_raw)}]
            request["technical_limits"]["max_prior_json_depth"] = 64
            request["technical_limits"]["max_prior_json_nodes"] = 10_000
            with self.assertRaisesRegex(AdaptiveWaveValidationError, "json_node_ceiling_exceeded"):
                load_prior_context(request)

            oversized_prior = b"{" + b" " * 1_000_000
            prior_path = root / "oversized-prior.json"
            _write_private(prior_path, oversized_prior)
            request["prior_waves"] = [
                {"wave_id": "oversized", "path": str(prior_path), "sha256": _bytes_sha(oversized_prior)}
            ]
            request["technical_limits"]["max_prior_wave_bytes"] = 1_000_000
            with self.assertRaisesRegex(AdaptiveWaveValidationError, "bound_input_byte_ceiling_exceeded"):
                load_prior_context(request)

    def test_invalid_calendar_dates_fail_validation_without_raising(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            os.chmod(root, 0o700)
            _, request_path = _build_request(root)
            receipt, run_root = run_adaptive_grok_wave_fixture(
                request_path=request_path,
                runtime_root=root / "runtime",
            )
            invalid_receipt = copy.deepcopy(receipt)
            invalid_receipt["process"]["completed_at"] = "2026-02-30T10:00:00.000Z"
            self.assertIn("receipt_process_value_invalid", validate_operator_receipt(invalid_receipt))
            intent = json.loads((run_root / "operator-intent.json").read_text())
            intent["started_at"] = "2026-02-30T10:00:00.000Z"
            self.assertFalse(runner._intent_valid(intent))

    def test_gated_launcher_persists_identity_before_target_exec(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            os.chmod(root, 0o700)
            marker = root / "target-ran"
            ledger_path = root / "durable-ledger.json"
            callback_facts: list[tuple[int, int, str, str]] = []

            def persist_before_release(child: int, group: int, birth: str, token: str) -> None:
                self.assertFalse(marker.exists())
                ledger = {
                    "child_pid": child,
                    "process_group_id": group,
                    "kernel_birth_identity": birth,
                    "process_identity_token": token,
                }
                self.assertTrue(runner._recorded_process_group_identity_matches(ledger))
                _atomic_publish(ledger_path, (canonical_json(ledger) + "\n").encode())
                callback_facts.append((child, group, birth, token))

            script = (
                f"import os,pathlib; pathlib.Path({str(marker)!r}).write_text(os.environ['X_FIRST_PROCESS_IDENTITY'])"
            )
            result = ProcessGroupExecutor()(
                [sys.executable, "-c", script],
                cwd=root,
                environment={"PATH": "/usr/bin:/bin", "LANG": "C", "LC_ALL": "C"},
                stdout_spool=root / "stdout",
                stderr_spool=root / "stderr",
                deadline_at=time.monotonic() + 5,
                term_grace_ms=100,
                kill_grace_ms=1_000,
                max_stdout_bytes=1_000_000,
                max_stderr_bytes=1_000_000,
                session_tree_root=root / "session",
                session_updates_path=root / "session/updates.jsonl",
                max_session_files=64,
                max_session_file_bytes=1_000_000,
                max_session_total_bytes=2_000_000,
                max_session_updates_bytes=1_000_000,
                monotonic=time.monotonic,
                on_spawn=persist_before_release,
            )
            self.assertEqual(result.exit_code, 0)
            self.assertTrue(result.process_group_cleanup_confirmed)
            self.assertEqual(len(callback_facts), 1)
            self.assertEqual(marker.read_text(), result.process_identity_token)
            self.assertTrue(ledger_path.is_file())

    def test_gated_launcher_never_execs_target_when_release_authorization_fails(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            marker = root / "target-ran"

            def reject_release(child: int, group: int, birth: str, token: str) -> None:
                del child, group, birth, token
                self.assertFalse(marker.exists())
                raise PermissionError("synthetic_grant_expiry")

            script = f"import pathlib; pathlib.Path({str(marker)!r}).write_text('ran')"
            result = ProcessGroupExecutor()(
                [sys.executable, "-c", script],
                cwd=root,
                environment={"PATH": "/usr/bin:/bin", "LANG": "C", "LC_ALL": "C"},
                stdout_spool=root / "stdout",
                stderr_spool=root / "stderr",
                deadline_at=time.monotonic() + 5,
                term_grace_ms=100,
                kill_grace_ms=1_000,
                max_stdout_bytes=1_000_000,
                max_stderr_bytes=1_000_000,
                session_tree_root=root / "session",
                session_updates_path=root / "session/updates.jsonl",
                max_session_files=64,
                max_session_file_bytes=1_000_000,
                max_session_total_bytes=2_000_000,
                max_session_updates_bytes=1_000_000,
                monotonic=time.monotonic,
                on_spawn=reject_release,
            )
            self.assertFalse(marker.exists())
            self.assertTrue(result.process_spawn_attempted)
            self.assertIn(
                result.execution_error_code,
                {"process_execution_failed", "process_group_cleanup_failed"},
            )

    def test_process_executor_streams_to_hard_byte_ceiling(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            stdout_spool = root / "stdout"
            stderr_spool = root / "stderr"
            spawned: list[tuple[int, int]] = []
            executor = ProcessGroupExecutor()
            result = executor(
                [sys.executable, "-c", "import sys; sys.stdout.buffer.write(b'x' * 200000)"],
                cwd=ROOT,
                environment=dict(os.environ),
                stdout_spool=stdout_spool,
                stderr_spool=stderr_spool,
                deadline_at=time.monotonic() + 5,
                term_grace_ms=100,
                kill_grace_ms=1_000,
                max_stdout_bytes=50_000,
                max_stderr_bytes=50_000,
                session_tree_root=root / "session",
                session_updates_path=root / "session/updates.jsonl",
                max_session_files=64,
                max_session_file_bytes=1_000_000,
                max_session_total_bytes=2_000_000,
                max_session_updates_bytes=1_000_000,
                monotonic=time.monotonic,
                on_spawn=lambda child, group, birth, token: spawned.append((child, group)),
            )
            self.assertEqual(result.technical_limit_kind, "stdout_bytes")
            self.assertEqual(stdout_spool.stat().st_size, 50_000)
            self.assertEqual(len(spawned), 1)

    def test_process_executor_escalates_timeout_term_to_kill(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            executor = ProcessGroupExecutor()
            readiness = root / "child-ready"
            script = (
                "import pathlib,signal,time; "
                "signal.signal(signal.SIGTERM, signal.SIG_IGN); "
                f"pathlib.Path({str(readiness)!r}).write_text('ready'); "
                "time.sleep(30)"
            )
            started = time.monotonic()
            result = executor(
                [sys.executable, "-c", script],
                cwd=ROOT,
                environment=dict(os.environ),
                stdout_spool=root / "stdout",
                stderr_spool=root / "stderr",
                deadline_at=started + 2.0,
                term_grace_ms=100,
                kill_grace_ms=1_000,
                max_stdout_bytes=1_000_000,
                max_stderr_bytes=1_000_000,
                session_tree_root=root / "session",
                session_updates_path=root / "session/updates.jsonl",
                max_session_files=64,
                max_session_file_bytes=1_000_000,
                max_session_total_bytes=2_000_000,
                max_session_updates_bytes=1_000_000,
                monotonic=time.monotonic,
                on_spawn=lambda child, group, birth, token: None,
            )
            self.assertTrue(result.timed_out)
            self.assertTrue(result.term_sent)
            self.assertTrue(readiness.is_file())
            self.assertTrue(result.process_group_cleanup_confirmed)
            self.assertIsNotNone(result.process_group_id)
            self.assertFalse(ProcessGroupExecutor._group_alive(result.process_group_id))
            self.assertLess(time.monotonic() - started, 5)

    def test_atomic_publication_is_exclusive_and_pending_files_recover(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            os.chmod(root, 0o700)
            target = root / "artifact.json"
            _atomic_publish(target, b"first")
            with self.assertRaises(FileExistsError):
                _atomic_publish(target, b"second")
            pending = root / ".pending-raw.stdout-66666666666666666666666666666666"
            _write_private(pending, b"orphan")
            self.assertEqual(recover_pending_publications(root), 1)
            self.assertFalse(pending.exists())

    def test_recovery_blocks_live_group_then_requires_explicit_bounded_termination(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            os.chmod(root, 0o700)
            binary, auth, binary_sha = _live_material(root)
            request, request_path = _build_request(root, binary_sha=binary_sha)
            approvals = root / "approvals"
            issue_live_grant(
                request_path=request_path,
                grant_root=approvals,
                auth_source=auth,
                wall_clock=lambda: FIXED_TIME,
            )
            raw = (canonical_json(_empty_result()) + "\n").encode()
            fake = FakeExecutor(MutableClock(), raw, spawn=True)
            _, run_root = _run_adaptive_wave(
                request=request,
                execution_mode="live",
                runtime_root=root / "runtime",
                approval_root=approvals,
                binary=binary,
                auth_source=auth,
                executor=fake,
                monotonic=fake.clock,
                wall_clock=lambda: FIXED_TIME,
            )
            (run_root / "operator-receipt.json").unlink()
            with self.assertRaisesRegex(AdaptiveWaveValidationError, "run_process_group_still_alive"):
                recover_incomplete_run(
                    run_root,
                    approval_root=approvals,
                    process_group_is_alive=lambda group: True,
                )

            state = {"alive": True}
            terminate_calls: list[int] = []

            def terminate(group: int, **kwargs: Any) -> tuple[bool, bool]:
                del kwargs
                terminate_calls.append(group)
                state["alive"] = False
                return True, False

            with self.assertRaisesRegex(AdaptiveWaveValidationError, "recovery_process_identity_mismatch"):
                recover_incomplete_run(
                    run_root,
                    approval_root=approvals,
                    terminate_orphan=True,
                    process_group_is_alive=lambda group: state["alive"],
                    process_group_identity_matches=lambda ledger: False,
                    terminate_process_group=terminate,
                )
            self.assertEqual(terminate_calls, [])
            recovered = recover_incomplete_run(
                run_root,
                approval_root=approvals,
                terminate_orphan=True,
                process_group_is_alive=lambda group: state["alive"],
                process_group_identity_matches=lambda ledger: True,
                terminate_process_group=terminate,
                wall_clock=lambda: FIXED_TIME + timedelta(minutes=5),
            )
            self.assertEqual(recovered["status"], "crash_recovered")
            self.assertTrue(recovered["process"]["term_sent"])
            self.assertEqual(validate_operator_bundle(run_root, approval_root=approvals), [])

    def test_fixture_crash_recovery_and_bundle_tamper_fail_closed(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            os.chmod(root, 0o700)
            _, request_path = _build_request(root)
            receipt, tampered_run_root = run_adaptive_grok_wave_fixture(
                request_path=request_path,
                runtime_root=root / "runtime",
            )
            self.assertEqual(receipt["status"], "fixture_complete")
            (tampered_run_root / "operator-receipt.json").unlink()
            _write_private(tampered_run_root / "sanitized.json", b'{"tampered":true}\n')
            with self.assertRaisesRegex(AdaptiveWaveValidationError, "recovery_sanitized_hash_mismatch"):
                recover_incomplete_run(
                    tampered_run_root,
                    process_group_is_alive=lambda group: False,
                )
            self.assertFalse((tampered_run_root / "operator-receipt.json").exists())

            _, run_root = run_adaptive_grok_wave_fixture(
                request_path=request_path,
                runtime_root=root / "runtime",
            )
            (run_root / "operator-receipt.json").unlink()
            recovered = recover_incomplete_run(
                run_root,
                process_group_is_alive=lambda group: False,
            )
            self.assertEqual(validate_operator_receipt(recovered), [])
            self.assertEqual(validate_operator_bundle(run_root), [])
            _write_private(run_root / "raw.stdout", b"tampered")
            self.assertIn("raw_stdout_hash_mismatch", validate_operator_bundle(run_root))

    def test_grant_expiry_is_checked_at_atomic_consumption_not_intent_time(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            os.chmod(root, 0o700)
            binary, auth, binary_sha = _live_material(root)
            request, request_path = _build_request(root, binary_sha=binary_sha)
            approvals = root / "approvals"
            issue_live_grant(
                request_path=request_path,
                grant_root=approvals,
                auth_source=auth,
                ttl_seconds=60,
                wall_clock=lambda: FIXED_TIME,
            )
            values = iter(
                (
                    FIXED_TIME,
                    FIXED_TIME + timedelta(seconds=59),
                    FIXED_TIME + timedelta(seconds=61),
                )
            )
            fake = FakeExecutor(MutableClock(), (canonical_json(_empty_result()) + "\n").encode(), spawn=True)
            with self.assertRaisesRegex(PermissionError, "preissued_live_grant_invalid"):
                _run_adaptive_wave(
                    request=request,
                    execution_mode="live",
                    runtime_root=root / "runtime",
                    approval_root=approvals,
                    binary=binary,
                    auth_source=auth,
                    executor=fake,
                    monotonic=fake.clock,
                    wall_clock=lambda: next(values),
                )
            self.assertEqual(fake.commands, [])
            self.assertEqual(list(approvals.glob("consumption-*.json")), [])

    def test_grant_expiry_after_consumption_link_never_invokes_executor(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            os.chmod(root, 0o700)
            binary, auth, binary_sha = _live_material(root)
            request, request_path = _build_request(root, binary_sha=binary_sha)
            approvals = root / "approvals"
            issue_live_grant(
                request_path=request_path,
                grant_root=approvals,
                auth_source=auth,
                ttl_seconds=60,
                wall_clock=lambda: FIXED_TIME,
            )
            values = iter(
                (
                    FIXED_TIME,
                    FIXED_TIME + timedelta(seconds=59),
                    FIXED_TIME + timedelta(seconds=59),
                    FIXED_TIME + timedelta(seconds=61),
                )
            )
            fake = FakeExecutor(MutableClock(), (canonical_json(_empty_result()) + "\n").encode(), spawn=True)
            with self.assertRaisesRegex(
                PermissionError,
                "preissued_live_grant_expired_after_consumption",
            ):
                _run_adaptive_wave(
                    request=request,
                    execution_mode="live",
                    runtime_root=root / "runtime",
                    approval_root=approvals,
                    binary=binary,
                    auth_source=auth,
                    executor=fake,
                    monotonic=fake.clock,
                    wall_clock=lambda: next(values),
                )
            self.assertEqual(fake.commands, [])
            self.assertEqual(fake.target_release_count, 0)
            self.assertEqual(len(list(approvals.glob("consumption-*.json"))), 1)

    def test_grant_expiry_after_durable_launcher_ledger_never_releases_target(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            os.chmod(root, 0o700)
            binary, auth, binary_sha = _live_material(root)
            request, request_path = _build_request(root, binary_sha=binary_sha)
            approvals = root / "approvals"
            issue_live_grant(
                request_path=request_path,
                grant_root=approvals,
                auth_source=auth,
                ttl_seconds=60,
                wall_clock=lambda: FIXED_TIME,
            )
            values = iter(
                (
                    FIXED_TIME,
                    FIXED_TIME + timedelta(seconds=59),
                    FIXED_TIME + timedelta(seconds=59),
                    FIXED_TIME + timedelta(seconds=59),
                    FIXED_TIME + timedelta(seconds=59),
                    FIXED_TIME + timedelta(seconds=61),
                )
            )
            fake = FakeExecutor(MutableClock(), (canonical_json(_empty_result()) + "\n").encode(), spawn=True)
            with self.assertRaisesRegex(
                PermissionError,
                "live_grant_expired_before_target_release",
            ):
                _run_adaptive_wave(
                    request=request,
                    execution_mode="live",
                    runtime_root=root / "runtime",
                    approval_root=approvals,
                    binary=binary,
                    auth_source=auth,
                    executor=fake,
                    monotonic=fake.clock,
                    wall_clock=lambda: next(values),
                )
            self.assertEqual(fake.target_release_count, 0)
            self.assertEqual(len(list(approvals.glob("consumption-*.json"))), 1)
            run_root = next((root / "runtime").iterdir())
            self.assertTrue((run_root / "process-ledger.json").is_file())
            recovered = recover_incomplete_run(
                run_root,
                approval_root=approvals,
                process_group_is_alive=lambda group: False,
                wall_clock=lambda: FIXED_TIME + timedelta(minutes=5),
            )
            self.assertEqual(recovered["status"], "crash_recovered")
            self.assertEqual(validate_operator_bundle(run_root, approval_root=approvals), [])

    def test_bundle_replay_rejects_launcher_verification_outside_grant_window(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            run_root, approvals = _completed_live_run(root)
            ledger_path = run_root / "process-ledger.json"
            ledger = json.loads(ledger_path.read_text())
            ledger["spawned_at"] = runner._timestamp(FIXED_TIME + timedelta(minutes=16))
            ledger_raw = (canonical_json(ledger) + "\n").encode()
            _write_private(ledger_path, ledger_raw)
            receipt_path = run_root / "operator-receipt.json"
            receipt = json.loads(receipt_path.read_text())
            receipt["process"]["process_ledger_sha256"] = _bytes_sha(ledger_raw)
            _write_private(receipt_path, (canonical_json(receipt) + "\n").encode())
            self.assertIn(
                "process_release_outside_grant_window",
                validate_operator_bundle(run_root, approval_root=approvals),
            )

    def test_live_completion_requires_model_tool_usage_and_terminal_transcript_proof(self) -> None:
        def wrong_model(updates: list[dict[str, Any]]) -> None:
            updates[0]["params"]["update"]["_meta"]["modelId"] = "grok-forged"

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            os.chmod(root, 0o700)
            binary, auth, binary_sha = _live_material(root)
            request, request_path = _build_request(root, binary_sha=binary_sha)
            approvals = root / "approvals"
            issue_live_grant(
                request_path=request_path,
                grant_root=approvals,
                auth_source=auth,
                wall_clock=lambda: FIXED_TIME,
            )
            fake = FakeExecutor(
                MutableClock(),
                (canonical_json(_empty_result()) + "\n").encode(),
                spawn=True,
                session_mutator=wrong_model,
            )
            receipt, run_root = _run_adaptive_wave(
                request=request,
                execution_mode="live",
                runtime_root=root / "runtime",
                approval_root=approvals,
                binary=binary,
                auth_source=auth,
                executor=fake,
                monotonic=fake.clock,
                wall_clock=lambda: FIXED_TIME,
            )
            self.assertEqual(receipt["status"], "provider_evidence_invalid")
            self.assertEqual(receipt["session_proof"]["status"], "invalid")
            self.assertEqual(validate_operator_bundle(run_root, approval_root=approvals), [])

    def test_live_transcript_rejects_protected_and_secondary_semantic_arguments(self) -> None:
        rejected_arguments = (
            {"query": "OpenAI autistic pretraining researcher", "limit": "100", "mode": "Latest"},
            {
                "query": "OpenAI pretraining researcher",
                "limit": "100",
                "mode": "Latest",
                "exclude": "women researchers",
            },
            {
                "query": "OpenAI pretraining researcher",
                "limit": "100",
                "mode": "Latest",
                "url": "https://x.com/OpenAI/status/123456",
            },
        )
        for arguments in rejected_arguments:
            with self.subTest(arguments=arguments), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                os.chmod(root, 0o700)
                binary, auth, binary_sha = _live_material(root)
                request, request_path = _build_request(root, binary_sha=binary_sha)
                approvals = root / "approvals"
                issue_live_grant(
                    request_path=request_path,
                    grant_root=approvals,
                    auth_source=auth,
                    wall_clock=lambda: FIXED_TIME,
                )

                def rejected_tool_arguments(updates: list[dict[str, Any]]) -> None:
                    updates[2]["params"]["update"]["rawOutput"]["input"] = canonical_json(arguments)

                fake = FakeExecutor(
                    MutableClock(),
                    (canonical_json(_empty_result()) + "\n").encode(),
                    spawn=True,
                    session_mutator=rejected_tool_arguments,
                )
                receipt, run_root = _run_adaptive_wave(
                    request=request,
                    execution_mode="live",
                    runtime_root=root / "runtime",
                    approval_root=approvals,
                    binary=binary,
                    auth_source=auth,
                    executor=fake,
                    monotonic=fake.clock,
                    wall_clock=lambda: FIXED_TIME,
                )
                self.assertEqual(receipt["status"], "provider_evidence_invalid")
                self.assertEqual(receipt["session_proof"]["status"], "invalid")
                self.assertEqual(validate_operator_bundle(run_root, approval_root=approvals), [])

    def test_intent_precedes_auth_and_executor_exception_deletes_entire_ephemeral_home(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            os.chmod(root, 0o700)
            binary, auth, binary_sha = _live_material(root)
            request, request_path = _build_request(root, binary_sha=binary_sha)
            approvals = root / "approvals"
            issue_live_grant(
                request_path=request_path,
                grant_root=approvals,
                auth_source=auth,
                wall_clock=lambda: FIXED_TIME,
            )

            def explode(command: Any, **kwargs: Any) -> ProcessResult:
                del command, kwargs
                raise RuntimeError("synthetic_executor_failure")

            with self.assertRaisesRegex(RuntimeError, "synthetic_executor_failure"):
                _run_adaptive_wave(
                    request=request,
                    execution_mode="live",
                    runtime_root=root / "runtime",
                    approval_root=approvals,
                    binary=binary,
                    auth_source=auth,
                    executor=explode,
                    monotonic=MutableClock(),
                    wall_clock=lambda: FIXED_TIME,
                )
            run_root = next((root / "runtime").iterdir())
            self.assertTrue((run_root / "operator-intent.json").is_file())
            self.assertFalse((run_root / "ephemeral-home").exists())

    def test_preconsumption_auth_failure_recovers_with_explicit_unconsumed_grant(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            os.chmod(root, 0o700)
            binary, auth, binary_sha = _live_material(root)
            request, request_path = _build_request(root, binary_sha=binary_sha)
            approvals = root / "approvals"
            issue_live_grant(
                request_path=request_path,
                grant_root=approvals,
                auth_source=auth,
                wall_clock=lambda: FIXED_TIME,
            )
            _write_private(auth, b'{"synthetic":"rotated-after-intent-binding"}\n')
            fake = FakeExecutor(MutableClock(), (canonical_json(_empty_result()) + "\n").encode(), spawn=True)
            with self.assertRaisesRegex(AdaptiveWaveValidationError, "live_auth_fingerprint_mismatch"):
                _run_adaptive_wave(
                    request=request,
                    execution_mode="live",
                    runtime_root=root / "runtime",
                    approval_root=approvals,
                    binary=binary,
                    auth_source=auth,
                    executor=fake,
                    monotonic=fake.clock,
                    wall_clock=lambda: FIXED_TIME,
                )
            run_root = next((root / "runtime").iterdir())
            recovered = recover_incomplete_run(
                run_root,
                approval_root=approvals,
                process_group_is_alive=lambda group: False,
                wall_clock=lambda: FIXED_TIME + timedelta(minutes=5),
            )
            self.assertIsNone(recovered["approval"]["consumption_sha256"])
            self.assertFalse(recovered["process"]["process_spawn_attempted"])
            self.assertEqual(validate_operator_bundle(run_root, approval_root=approvals), [])

    def test_post_executor_publication_failure_deletes_auth_immediately_and_recovers(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            os.chmod(root, 0o700)
            binary, auth, binary_sha = _live_material(root)
            request, request_path = _build_request(root, binary_sha=binary_sha)
            approvals = root / "approvals"
            issue_live_grant(
                request_path=request_path,
                grant_root=approvals,
                auth_source=auth,
                wall_clock=lambda: FIXED_TIME,
            )
            fake = FakeExecutor(MutableClock(), (canonical_json(_empty_result()) + "\n").encode(), spawn=True)
            original_publish = runner._atomic_publish

            def fail_raw_stdout(path: Path, value: bytes, *, pre_publish: Any = None) -> None:
                if path.name == "raw.stdout":
                    raise FileExistsError("synthetic_raw_stdout_conflict")
                original_publish(path, value, pre_publish=pre_publish)

            with mock.patch.object(runner, "_atomic_publish", side_effect=fail_raw_stdout):
                with self.assertRaisesRegex(FileExistsError, "synthetic_raw_stdout_conflict"):
                    _run_adaptive_wave(
                        request=request,
                        execution_mode="live",
                        runtime_root=root / "runtime",
                        approval_root=approvals,
                        binary=binary,
                        auth_source=auth,
                        executor=fake,
                        monotonic=fake.clock,
                        wall_clock=lambda: FIXED_TIME,
                    )
            run_root = next((root / "runtime").iterdir())
            self.assertTrue((run_root / "operator-intent.json").is_file())
            self.assertFalse((run_root / "ephemeral-home").exists())
            recovered = recover_incomplete_run(
                run_root,
                approval_root=approvals,
                process_group_is_alive=lambda group: False,
                wall_clock=lambda: FIXED_TIME + timedelta(minutes=5),
            )
            self.assertEqual(recovered["status"], "crash_recovered")
            self.assertEqual(validate_operator_bundle(run_root, approval_root=approvals), [])

    def test_run_root_creation_failure_rolls_back_before_purge_inventory(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            runtime_root = root / "runtime"
            runtime_root.mkdir(mode=0o700)
            real_fsync = runner._fsync_directory
            calls = 0

            def fail_first_parent_fsync(path: Path) -> None:
                nonlocal calls
                calls += 1
                if calls == 1:
                    raise OSError("synthetic_parent_fsync_failure")
                real_fsync(path)

            with mock.patch.object(runner, "_fsync_directory", side_effect=fail_first_parent_fsync):
                with self.assertRaisesRegex(OSError, "synthetic_parent_fsync_failure"):
                    runner._create_run_root(
                        runtime_root,
                        "grok_wave_fixture_44444444444444444444444444444444",
                    )
            self.assertEqual(list(runtime_root.iterdir()), [])
            self.assertEqual(
                purge_expired_adaptive_runs(
                    runtime_root=runtime_root,
                    deletion_root=root / "deletions",
                ),
                [],
            )

    def test_post_process_scan_preserves_actual_process_timeout_ownership(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            os.chmod(root, 0o700)
            binary, auth, binary_sha = _live_material(root)
            request, request_path = _build_request(root, binary_sha=binary_sha)
            request["emergency"]["deadline_ms"] = 1_000
            _write_private(request_path, (canonical_json(request) + "\n").encode())
            approvals = root / "approvals"
            issue_live_grant(
                request_path=request_path,
                grant_root=approvals,
                auth_source=auth,
                wall_clock=lambda: FIXED_TIME,
            )
            fake = FakeExecutor(
                MutableClock(),
                (canonical_json(_empty_result()) + "\n").encode(),
                spawn=True,
                exit_code=-signal.SIGTERM,
                timed_out=True,
                term_sent=True,
            )
            receipt, run_root = _run_adaptive_wave(
                request=request,
                execution_mode="live",
                runtime_root=root / "runtime",
                approval_root=approvals,
                binary=binary,
                auth_source=auth,
                executor=fake,
                monotonic=fake.clock,
                wall_clock=lambda: FIXED_TIME,
            )
            self.assertEqual(receipt["status"], "timed_out")
            self.assertTrue(receipt["process"]["timed_out"])
            self.assertFalse(receipt["process"]["technical_limit_exceeded"])
            self.assertIsNone(receipt["process"]["technical_limit_kind"])
            self.assertEqual(validate_operator_bundle(run_root, approval_root=approvals), [])
            contradictory = copy.deepcopy(receipt)
            contradictory["status"] = "technical_limit_exceeded"
            contradictory["process"]["technical_limit_exceeded"] = True
            contradictory["process"]["technical_limit_kind"] = "session_tree_scan_deadline"
            self.assertIn("receipt_timeout_ownership_invalid", validate_operator_receipt(contradictory))
            with self.assertRaises(MiniDraft202012Error):
                assert_schema_valid(
                    contradictory,
                    "x.grok.adaptive_recall_wave.operator_receipt.v3.schema.json",
                )

    def test_provider_mode_tampering_fails_closed_and_mode_zero_tree_is_deleted(self) -> None:
        def expose_auth(session_root: Path, updates_path: Path) -> None:
            del updates_path
            os.chmod(session_root / "auth.json", 0o644)

        def seal_tree(session_root: Path, updates_path: Path) -> None:
            del updates_path
            sealed = session_root / "provider-sealed" / "nested"
            sealed.mkdir(parents=True, mode=0o700)
            secret = sealed / "provider-secret"
            _write_private(secret, b"synthetic secret")
            os.chmod(sealed, 0o000)
            os.chmod(sealed.parent, 0o000)
            os.chmod(session_root, 0o000)

        for mutation in (expose_auth, seal_tree):
            with self.subTest(mutation=mutation.__name__), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                os.chmod(root, 0o700)
                binary, auth, binary_sha = _live_material(root)
                request, request_path = _build_request(root, binary_sha=binary_sha)
                approvals = root / "approvals"
                issue_live_grant(
                    request_path=request_path,
                    grant_root=approvals,
                    auth_source=auth,
                    wall_clock=lambda: FIXED_TIME,
                )
                fake = FakeExecutor(
                    MutableClock(),
                    (canonical_json(_empty_result()) + "\n").encode(),
                    spawn=True,
                    session_tree_mutator=mutation,
                )
                receipt, run_root = _run_adaptive_wave(
                    request=request,
                    execution_mode="live",
                    runtime_root=root / "runtime",
                    approval_root=approvals,
                    binary=binary,
                    auth_source=auth,
                    executor=fake,
                    monotonic=fake.clock,
                    wall_clock=lambda: FIXED_TIME,
                )
                self.assertEqual(receipt["status"], "technical_limit_exceeded")
                self.assertEqual(receipt["process"]["technical_limit_kind"], "session_tree_entry_invalid")
                self.assertFalse((run_root / "ephemeral-home").exists())
                self.assertEqual(validate_operator_bundle(run_root, approval_root=approvals), [])

    def test_recovery_deletes_mode_zero_auth_without_manual_chmod(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            run_root, approvals = _completed_live_run(root)
            (run_root / "operator-receipt.json").unlink()
            ephemeral_home = run_root / "ephemeral-home"
            nested = ephemeral_home / "provider-sealed"
            nested.mkdir(parents=True, mode=0o700)
            _write_private(ephemeral_home / "auth.json", b'{"synthetic":"oauth-copy"}\n')
            _write_private(nested / "secret", b"synthetic secret")
            os.chmod(nested, 0o000)
            os.chmod(ephemeral_home, 0o000)
            recovered = recover_incomplete_run(
                run_root,
                approval_root=approvals,
                process_group_is_alive=lambda group: False,
                wall_clock=lambda: FIXED_TIME + timedelta(minutes=5),
            )
            self.assertEqual(recovered["status"], "crash_recovered")
            self.assertFalse(ephemeral_home.exists())
            self.assertEqual(validate_operator_bundle(run_root, approval_root=approvals), [])

    def test_session_tree_measurement_runtime_matches_receipt_schema_boundaries(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            os.chmod(root, 0o700)
            receipt, _ = run_adaptive_grok_wave_fixture(
                request_path=_build_request(root)[1],
                runtime_root=root / "runtime",
            )
            mutations = {
                "session_tree_file_count": -1,
                "session_tree_entry_count": -1,
                "session_tree_max_depth": runner.MAX_SESSION_TREE_DEPTH + 1,
                "session_tree_total_bytes": -1,
                "session_tree_max_file_bytes": False,
            }
            for field, value in mutations.items():
                with self.subTest(field=field):
                    changed = copy.deepcopy(receipt)
                    changed["artifacts"][field] = value
                    with self.assertRaises(MiniDraft202012Error):
                        assert_schema_valid(
                            changed,
                            "x.grok.adaptive_recall_wave.operator_receipt.v3.schema.json",
                        )
                    self.assertIn("receipt_artifacts_value_invalid", validate_operator_receipt(changed))

    def test_session_tree_counts_directories_allows_one_socket_and_bounds_depth_and_deadline(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "session"
            root.mkdir(mode=0o700)
            for index in range(4):
                (root / f"directory-{index}").mkdir(mode=0o700)
            entries = runner._measure_session_tree(
                root,
                max_files=3,
                max_file_bytes=1_000_000,
                max_total_bytes=2_000_000,
            )
            self.assertEqual(entries.limit_kind, "session_tree_entries")
            self.assertGreater(entries.entry_count, entries.file_count)
            self.assertEqual(entries.entry_count, 3)
            os.chmod(root / "directory-0", 0o755)
            widened = runner._measure_session_tree(
                root,
                max_files=16,
                max_file_bytes=1_000_000,
                max_total_bytes=2_000_000,
            )
            self.assertEqual(widened.limit_kind, "session_tree_entry_invalid")
            os.chmod(root / "directory-0", 0o700)

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "session"
            root.mkdir(mode=0o700)
            leader_path = root / "leader.sock"
            leader = socket.socket(socket.AF_UNIX)
            leader.bind(str(leader_path))
            try:
                allowed = runner._measure_session_tree(
                    root,
                    max_files=16,
                    max_file_bytes=1_000_000,
                    max_total_bytes=2_000_000,
                    expected_socket_path=leader_path,
                )
                self.assertIsNone(allowed.limit_kind)
                self.assertEqual(allowed.entry_count, 1)
                unexpected_path = root / "unexpected.sock"
                unexpected = socket.socket(socket.AF_UNIX)
                unexpected.bind(str(unexpected_path))
                try:
                    rejected = runner._measure_session_tree(
                        root,
                        max_files=16,
                        max_file_bytes=1_000_000,
                        max_total_bytes=2_000_000,
                        expected_socket_path=leader_path,
                    )
                    self.assertEqual(rejected.limit_kind, "session_tree_unexpected_socket")
                finally:
                    unexpected.close()
            finally:
                leader.close()

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "session"
            root.mkdir(mode=0o700)
            nested = root
            for name in ("one", "two", "three"):
                nested /= name
                nested.mkdir(mode=0o700, parents=True)
            depth = runner._measure_session_tree(
                root,
                max_files=16,
                max_file_bytes=1_000_000,
                max_total_bytes=2_000_000,
                max_depth=2,
            )
            self.assertEqual(depth.limit_kind, "session_tree_depth")
            self.assertEqual(depth.max_depth, 2)
            deadline_clock = MutableClock()
            deadline = runner._measure_session_tree(
                root,
                max_files=16,
                max_file_bytes=1_000_000,
                max_total_bytes=2_000_000,
                deadline_at=deadline_clock(),
                monotonic=deadline_clock,
            )
            self.assertEqual(deadline.limit_kind, "session_tree_scan_deadline")

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "session"
            root.mkdir(mode=0o700)
            nested = root
            for index in range(runner.MAX_SESSION_TREE_DEPTH + 1):
                nested /= f"d{index}"
                nested.mkdir(mode=0o700)
            overflow = runner._measure_session_tree(
                root,
                max_files=1_000,
                max_file_bytes=1_000_000,
                max_total_bytes=2_000_000,
                max_depth=runner.MAX_SESSION_TREE_DEPTH,
            )
            self.assertEqual(overflow.limit_kind, "session_tree_depth")
            self.assertEqual(overflow.max_depth, runner.MAX_SESSION_TREE_DEPTH)

    def test_session_tree_ceiling_is_a_technical_failure_and_home_is_deleted(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            os.chmod(root, 0o700)
            binary, auth, binary_sha = _live_material(root)
            request, request_path = _build_request(root, binary_sha=binary_sha)
            request["technical_limits"].update(
                {
                    "max_session_file_bytes": 65_536,
                    "max_session_total_bytes": 131_072,
                    "max_session_updates_bytes": 65_536,
                    "max_session_update_line_bytes": 4_096,
                }
            )
            _write_private(request_path, (canonical_json(request) + "\n").encode())
            approvals = root / "approvals"
            issue_live_grant(
                request_path=request_path,
                grant_root=approvals,
                auth_source=auth,
                wall_clock=lambda: FIXED_TIME,
            )
            fake = FakeExecutor(
                MutableClock(),
                (canonical_json(_empty_result()) + "\n").encode(),
                spawn=True,
                extra_session_bytes=65_537,
            )
            receipt, run_root = _run_adaptive_wave(
                request=request,
                execution_mode="live",
                runtime_root=root / "runtime",
                approval_root=approvals,
                binary=binary,
                auth_source=auth,
                executor=fake,
                monotonic=fake.clock,
                wall_clock=lambda: FIXED_TIME,
            )
            self.assertEqual(receipt["status"], "technical_limit_exceeded")
            self.assertEqual(receipt["process"]["technical_limit_kind"], "session_tree_file_bytes")
            self.assertFalse((run_root / "ephemeral-home").exists())

    def test_recovery_lease_and_full_bundle_replay_fail_before_terminal_publication(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            os.chmod(root, 0o700)
            _, request_path = _build_request(root)
            _, run_root = run_adaptive_grok_wave_fixture(
                request_path=request_path,
                runtime_root=root / "runtime",
            )
            with runner._run_lease(run_root, create=False):
                with self.assertRaisesRegex(AdaptiveWaveValidationError, "run_active_owner_present"):
                    recover_incomplete_run(run_root)
            (run_root / "operator-receipt.json").unlink()
            intent_path = run_root / "operator-intent.json"
            intent = json.loads(intent_path.read_text())
            intent["emergency"]["deadline_ms"] -= 1_000
            _write_private(intent_path, (canonical_json(intent) + "\n").encode())
            with self.assertRaisesRegex(AdaptiveWaveValidationError, "recovery_bundle_replay_invalid"):
                recover_incomplete_run(run_root)
            self.assertFalse((run_root / "operator-receipt.json").exists())

    def test_bundle_descriptor_reads_and_emergency_binding_fail_closed(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            os.chmod(root, 0o700)
            _, request_path = _build_request(root)
            _, run_root = run_adaptive_grok_wave_fixture(
                request_path=request_path,
                runtime_root=root / "runtime",
            )
            receipt_path = run_root / "operator-receipt.json"
            receipt = json.loads(receipt_path.read_text())
            receipt["process"]["deadline_ms"] -= 1_000
            _write_private(receipt_path, (canonical_json(receipt) + "\n").encode())
            self.assertIn("receipt_emergency_binding_mismatch", validate_operator_bundle(run_root))
            receipt["process"]["deadline_ms"] += 1_000
            _write_private(receipt_path, (canonical_json(receipt) + "\n").encode())
            raw_path = run_root / "raw.stdout"
            hardlink = root / "raw-hardlink.stdout"
            os.link(raw_path, hardlink)
            self.assertIn("required_artifact_permissions_invalid", validate_operator_bundle(run_root))
            hardlink.unlink()
            outside = root / "outside.stdout"
            _write_private(outside, raw_path.read_bytes())
            raw_path.unlink()
            raw_path.symlink_to(outside)
            self.assertIn("required_artifact_permissions_invalid", validate_operator_bundle(run_root))

    def test_expired_purge_journals_before_deletion_and_emits_bound_receipt(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            os.chmod(root, 0o700)
            request, _ = _build_request(root)
            runtime_root = root / "runtime"
            receipt, run_root = _run_adaptive_wave(
                request=request,
                execution_mode="fixture",
                runtime_root=runtime_root,
                approval_root=root / "unused",
                binary=Path("fixture.invalid"),
                auth_source=None,
                executor=runner.OfflineFixtureExecutor(),
                monotonic=MutableClock(),
                wall_clock=lambda: FIXED_TIME,
            )
            deletion_root = root / "deletions"
            with runner._run_lease(run_root, create=False):
                with self.assertRaisesRegex(AdaptiveWaveValidationError, "run_active_owner_present"):
                    purge_expired_adaptive_runs(
                        runtime_root=runtime_root,
                        deletion_root=deletion_root,
                        approval_root=root / "unused",
                        wall_clock=lambda: FIXED_TIME + timedelta(days=2),
                    )
            self.assertEqual(list(deletion_root.glob("journal-*.json")), [])
            publish_receipt = runner._publish_deletion_receipt

            def crash_after_delete(**kwargs: Any) -> dict[str, Any]:
                del kwargs
                raise RuntimeError("synthetic_post_delete_crash")

            runner._publish_deletion_receipt = crash_after_delete
            try:
                with self.assertRaisesRegex(RuntimeError, "synthetic_post_delete_crash"):
                    purge_expired_adaptive_runs(
                        runtime_root=runtime_root,
                        deletion_root=deletion_root,
                        approval_root=root / "unused",
                        wall_clock=lambda: FIXED_TIME + timedelta(days=2),
                    )
            finally:
                runner._publish_deletion_receipt = publish_receipt
            self.assertFalse(run_root.exists())
            self.assertEqual(len(list(deletion_root.glob("journal-*.json"))), 1)
            self.assertEqual(list(deletion_root.glob("receipt-*.json")), [])
            deletion_receipts = purge_expired_adaptive_runs(
                runtime_root=runtime_root,
                deletion_root=deletion_root,
                approval_root=root / "unused",
                wall_clock=lambda: FIXED_TIME + timedelta(days=2),
            )
            self.assertEqual(len(deletion_receipts), 1)
            self.assertFalse(run_root.exists())
            journal_path = deletion_root / f"journal-{receipt['run_id']}.json"
            deletion_path = deletion_root / f"receipt-{receipt['run_id']}.json"
            self.assertTrue(journal_path.is_file())
            self.assertTrue(deletion_path.is_file())
            deletion = json.loads(deletion_path.read_text())
            self.assertEqual(deletion["deletion_journal_sha256"], _bytes_sha(journal_path.read_bytes()))
            self.assertEqual(
                purge_expired_adaptive_runs(
                    runtime_root=runtime_root,
                    deletion_root=deletion_root,
                    approval_root=root / "unused",
                    wall_clock=lambda: FIXED_TIME + timedelta(days=3),
                ),
                [],
            )

    def test_cli_defaults_to_fixture_issues_grant_separately_and_redacts_failures(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            os.chmod(root, 0o700)
            _, request_path = _build_request(root)
            output_root = root / "fixture-artifacts"
            completed = subprocess.run(
                [
                    sys.executable,
                    str(ROOT / "scripts/run_adaptive_grok_wave.py"),
                    "--request",
                    str(request_path),
                    "--fixture-output-root",
                    str(output_root),
                ],
                cwd=ROOT,
                capture_output=True,
                text=True,
                check=False,
            )
            self.assertEqual(completed.returncode, 0, completed.stderr)
            self.assertEqual(json.loads(completed.stdout), {"external_execution": False, "status": "fixture_complete"})
            self.assertNotIn(str(root), completed.stdout + completed.stderr)

        failed = subprocess.run(
            [sys.executable, str(ROOT / "scripts/run_adaptive_grok_wave.py"), "--request", "/private/missing.json"],
            cwd=ROOT,
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(failed.returncode, 1)
        self.assertEqual(json.loads(failed.stdout), {"error": "ADAPTIVE_GROK_WAVE_FAILED", "status": "failed"})
        self.assertNotIn("/private/missing.json", failed.stdout + failed.stderr)

        script_namespace = runpy.run_path(str(ROOT / "scripts/run_adaptive_grok_wave.py"))
        summary = script_namespace["_public_receipt_summary"]
        self.assertEqual(
            summary(
                {
                    "execution_mode": "live",
                    "status": "process_failed",
                    "process": {"process_spawn_attempted": False},
                }
            ),
            {"external_execution": False, "status": "process_failed"},
        )


if __name__ == "__main__":
    unittest.main()
