from __future__ import annotations

import copy
import hashlib
import json
import os
import runpy
import signal
import stat
import subprocess
import sys
import tempfile
import time
import unittest
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

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
    recover_incomplete_run,
    recover_pending_publications,
    run_adaptive_grok_wave_fixture,
    validate_model_result,
    validate_operator_bundle,
    validate_operator_receipt,
    validate_request,
)

FIXED_TIME = datetime(2026, 7, 14, 10, 0, 0, tzinfo=UTC)


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
    grant_id: str | None = "synthetic_live_grant_001",
) -> tuple[dict[str, Any], Path]:
    prompt = root / "prompt.md"
    prompt_raw = b"Find a broad public-professional synthetic researcher population.\n"
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
        "target": {
            "lab_id": "synthetic_lab",
            "research_focus_id": "base_model_training",
            "scope": "Public professional evidence for a synthetic lab-neutral fixture.",
        },
        "prompt_source": {"path": str(prompt), "sha256": _bytes_sha(prompt_raw)},
        "prior_waves": prior_waves,
        "transport": {
            "provider_id": "grok_cli_oauth",
            "model_id": "grok-4.5",
            "reasoning_effort": "high",
            "grok_binary_sha256": binary_sha,
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
        self.commands: list[list[str]] = []
        self.environments: list[dict[str, str]] = []
        self.deadlines: list[float] = []

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
        monotonic: Any,
        on_spawn: Any,
    ) -> ProcessResult:
        del cwd, term_grace_ms, kill_grace_ms, max_stdout_bytes, max_stderr_bytes, monotonic
        self.commands.append(list(command))
        self.environments.append(dict(environment))
        self.deadlines.append(deadline_at)
        child_pid = 42_424 if self.spawn else None
        process_group_id = child_pid
        kernel_birth_identity = "synthetic-kernel-birth-42424" if self.spawn else None
        identity_token = "a" * 64 if self.spawn else None
        if self.spawn:
            on_spawn(child_pid, process_group_id, kernel_birth_identity, identity_token)
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


class AdaptiveGrokWaveRunnerTests(unittest.TestCase):
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
            "x.grok.adaptive_recall_wave.request.v1.schema.json",
            "x.grok.adaptive_recall_wave.result.v1.schema.json",
            "x.grok.adaptive_recall_wave.intent.v1.schema.json",
            "x.grok.adaptive_recall_wave.operator_receipt.v1.schema.json",
            "x.grok.adaptive_recall_wave.live_grant.v1.schema.json",
            "x.grok.adaptive_recall_wave.live_grant_consumption.v1.schema.json",
            "x.grok.adaptive_recall_wave.process_ledger.v1.schema.json",
        )
        for name in schema_names:
            schema = json.loads((ROOT / "contracts" / name).read_text())
            self.assertEqual(schema["$schema"], "https://json-schema.org/draft/2020-12/schema")
            self.assertIs(schema["additionalProperties"], False)
        all_schema_text = "".join((ROOT / "contracts" / name).read_text() for name in schema_names)
        self.assertNotIn("maxItems", all_schema_text)

    def test_schema_top_level_keys_match_runtime_registries(self) -> None:
        mappings = {
            "x.grok.adaptive_recall_wave.request.v1.schema.json": runner._REQUEST_KEYS,
            "x.grok.adaptive_recall_wave.result.v1.schema.json": runner._RESULT_KEYS,
            "x.grok.adaptive_recall_wave.intent.v1.schema.json": runner._INTENT_KEYS,
            "x.grok.adaptive_recall_wave.operator_receipt.v1.schema.json": runner._RECEIPT_KEYS,
            "x.grok.adaptive_recall_wave.live_grant.v1.schema.json": runner._GRANT_KEYS,
            "x.grok.adaptive_recall_wave.live_grant_consumption.v1.schema.json": runner._CONSUMPTION_KEYS,
            "x.grok.adaptive_recall_wave.process_ledger.v1.schema.json": runner._PROCESS_LEDGER_KEYS,
        }
        for name, keys in mappings.items():
            schema = json.loads((ROOT / "contracts" / name).read_text())
            self.assertEqual(set(schema["required"]), keys, name)
            self.assertEqual(set(schema["properties"]), keys, name)
        receipt_schema = json.loads(
            (ROOT / "contracts/x.grok.adaptive_recall_wave.operator_receipt.v1.schema.json").read_text()
        )
        self.assertEqual(set(receipt_schema["$defs"]["command_binding"]["required"]), runner._COMMAND_BINDING_KEYS)
        self.assertEqual(set(receipt_schema["properties"]["process"]["required"]), runner._PROCESS_KEYS)
        result_schema = json.loads(
            (ROOT / "contracts/x.grok.adaptive_recall_wave.result.v1.schema.json").read_text()
        )
        self.assertEqual(set(result_schema["$defs"]["evidence"]["required"]), runner._EVIDENCE_KEYS)
        request_schema = json.loads(
            (ROOT / "contracts/x.grok.adaptive_recall_wave.request.v1.schema.json").read_text()
        )
        self.assertEqual(
            set(request_schema["properties"]["technical_limits"]["required"]),
            runner._TECHNICAL_LIMIT_KEYS,
        )

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
        result["candidates"][1]["profile_url"] = (
            f"https://profiles.invalid/{result['candidates'][1]['handle']}"
        )
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
            "supports": ["target_lab_affiliation_state", "pretraining_experience_state"],
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
            excerpt_only["supports"] = ["target_lab_affiliation_state", "pretraining_experience_state"]
            candidate["evidence"] = [excerpt_only]
            self.assertIn(
                "prior_overlap_without_material_update:0",
                validate_model_result(result, prior_candidates=facts),
            )
            evidence2 = copy.deepcopy(evidence)
            evidence2["post_id"] = "234567"
            evidence2["url"] = "https://x.com/PriorPerson/status/234567"
            evidence2["published_at"] = "2026-07-02T00:00:00Z"
            evidence2["supports"] = ["target_lab_affiliation_state", "pretraining_experience_state"]
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

    def test_live_run_stages_binary_isolates_auth_uses_closed_tools_and_replays_bundle(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            os.chmod(root, 0o700)
            binary, auth, binary_sha = _live_material(root)
            request, request_path = _build_request(root, binary_sha=binary_sha)
            approvals = root / "approvals"
            issue_live_grant(
                request_path=request_path,
                grant_root=approvals,
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
            self.assertEqual(command[command.index("--tools") + 1], ",".join(runner.NATIVE_X_TOOLS))
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
            staged_binary = run_root / "executable/grok"
            staged_binary.write_bytes(b"tampered executable")
            os.chmod(staged_binary, 0o700)
            self.assertIn(
                "staged_binary_request_hash_mismatch",
                validate_operator_bundle(run_root, approval_root=approvals),
            )

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
                artifact["command_binding"]["cli_flags_sha256"] = runner.canonical_sha256(
                    runner._redacted_policy_from_bindings(
                        artifact["input_binding"], artifact["command_binding"]
                    )
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
            issue_live_grant(request_path=request_path, grant_root=approvals, wall_clock=lambda: FIXED_TIME)
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
            incomplete = next((root / "runtime-missing").iterdir())
            self.assertFalse((incomplete / "ephemeral-home/auth.json").exists())

            approvals = root / "approvals"
            issue_live_grant(
                request_path=request_path,
                grant_root=approvals,
                ttl_seconds=60,
                wall_clock=lambda: FIXED_TIME,
            )
            changed = copy.deepcopy(request)
            changed["target"]["scope"] = "Changed after grant issuance."
            wrong_scope = FakeExecutor(MutableClock(), raw, spawn=True)
            with self.assertRaisesRegex(PermissionError, "preissued_live_grant_invalid"):
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
            request["prior_waves"] = [
                {"wave_id": "nested", "path": str(nested_path), "sha256": _bytes_sha(nested_raw)}
            ]
            request["technical_limits"]["max_prior_json_depth"] = 16
            with self.assertRaisesRegex(AdaptiveWaveValidationError, "json_depth_ceiling_exceeded"):
                load_prior_context(request)

            many_nodes = {"candidates": [{"handle": "same"} for _ in range(10_001)]}
            nodes_raw = (canonical_json(many_nodes) + "\n").encode()
            nodes_path = root / "nodes-prior.json"
            _write_private(nodes_path, nodes_raw)
            request["prior_waves"] = [
                {"wave_id": "nodes", "path": str(nodes_path), "sha256": _bytes_sha(nodes_raw)}
            ]
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
                "import os,pathlib; "
                f"pathlib.Path({str(marker)!r}).write_text(os.environ['X_FIRST_PROCESS_IDENTITY'])"
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
                monotonic=time.monotonic,
                on_spawn=persist_before_release,
            )
            self.assertEqual(result.exit_code, 0)
            self.assertTrue(result.process_group_cleanup_confirmed)
            self.assertEqual(len(callback_facts), 1)
            self.assertEqual(marker.read_text(), result.process_identity_token)
            self.assertTrue(ledger_path.is_file())

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
            script = "import signal,time; signal.signal(signal.SIGTERM, signal.SIG_IGN); time.sleep(30)"
            started = time.monotonic()
            result = executor(
                [sys.executable, "-c", script],
                cwd=ROOT,
                environment=dict(os.environ),
                stdout_spool=root / "stdout",
                stderr_spool=root / "stderr",
                deadline_at=started + 0.5,
                term_grace_ms=100,
                kill_grace_ms=1_000,
                max_stdout_bytes=1_000_000,
                max_stderr_bytes=1_000_000,
                monotonic=time.monotonic,
                on_spawn=lambda child, group, birth, token: None,
            )
            self.assertTrue(result.timed_out)
            self.assertTrue(result.term_sent)
            self.assertTrue(result.kill_sent)
            self.assertEqual(result.exit_code, -signal.SIGKILL)
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
            issue_live_grant(request_path=request_path, grant_root=approvals, wall_clock=lambda: FIXED_TIME)
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
                recover_incomplete_run(run_root, process_group_is_alive=lambda group: True)

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
                    terminate_orphan=True,
                    process_group_is_alive=lambda group: state["alive"],
                    process_group_identity_matches=lambda ledger: False,
                    terminate_process_group=terminate,
                )
            self.assertEqual(terminate_calls, [])
            recovered = recover_incomplete_run(
                run_root,
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
