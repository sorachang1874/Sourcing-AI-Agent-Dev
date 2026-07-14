from __future__ import annotations

import copy
import hashlib
import json
import stat
import sys
import tempfile
import unittest
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from x_first.live_probe import (  # noqa: E402
    LIVE_RESULT_SCHEMA_VERSION,
    PINNED_GROK_BINARY_SHA256,
    BoundedCommandResult,
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


def _inner_response(*, stable_user_id: str | None = "4398626122") -> dict[str, object]:
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
                "platform_object_id": "1900000000000000001",
                "platform_user_id": stable_user_id,
                "author_handle": "OpenAI",
                "canonical_url": "https://x.com/OpenAI/status/1900000000000000001",
                "authored_at": "2026-07-13T08:00:00Z",
                "excerpt": "Technical model-training update from the official lab account.",
                "full_body_stored": False,
            }
        ],
        "errors": [],
    }


def _outer_response(inner: dict[str, object], *, cost: float | None = 0.01) -> dict[str, object]:
    outer: dict[str, object] = {
        "text": json.dumps(inner),
        "stopReason": "EndTurn",
        "sessionId": "provider-session",
        "requestId": "provider-request",
        "num_turns": 2,
        "usage": {"input_tokens": 100, "output_tokens": 50, "total_tokens": 150},
    }
    if cost is not None:
        outer["total_cost_usd"] = cost
    return outer


def _raw_x_output(*, stable_user_id: str | None = "4398626122") -> dict[str, object]:
    author_info: dict[str, object] = {"legacy": {"screen_name": "OpenAI"}}
    if stable_user_id is not None:
        author_info["rest_id"] = stable_user_id
    return {
        "posts": [
            {
                "canonical_url": "https://x.com/OpenAI/status/1900000000000000001",
                "author_info": author_info,
            }
        ]
    }


def _proof(
    *,
    x_calls: int = 1,
    completed: int = 1,
    unexpected: tuple[str, ...] = (),
    stable_user_id: str | None = "4398626122",
) -> ToolProof:
    return ToolProof(
        x_search_calls=x_calls,
        x_search_completed_calls=completed,
        unexpected_tool_calls=unexpected,
        raw_result_post_pairs=(("1900000000000000001", "https://x.com/OpenAI/status/1900000000000000001"),),
        raw_result_author_user_ids=((stable_user_id,) if stable_user_id is not None else ()),
        observed_model_ids=("grok-4.5",),
        call_receipts=(
            ToolCallReceipt(
                call_id="x-call",
                tool_id="x_search",
                statuses=("completed",),
                raw_result_post_pairs=(("1900000000000000001", "https://x.com/OpenAI/status/1900000000000000001"),),
                raw_result_author_user_ids=((stable_user_id,) if stable_user_id is not None else ()),
            ),
        )
        if x_calls
        else (),
        updates_sha256="a" * 64,
        update_bytes=120,
    )


class XFirstLiveCapabilityContractTest(unittest.TestCase):
    def setUp(self) -> None:
        self.request = build_live_request()

    def test_live_request_is_exact_and_mutations_fail_closed(self) -> None:
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
        self.assertEqual(request_schema["properties"]["schema_version"]["const"], self.request["schema_version"])
        self.assertEqual(set(request_schema["required"]), set(self.request))
        self.assertEqual(result_schema["properties"]["schema_version"]["const"], LIVE_RESULT_SCHEMA_VERSION)
        self.assertEqual(
            approval_schema["properties"]["schema_version"]["const"], "x.grok.live_approval_consumption.v1"
        )
        self.assertEqual(tool_schema["properties"]["schema_version"]["const"], "x.grok.x_search_tool_receipt.v1")
        for mutate in (
            lambda value: value["hard_budgets"].update(max_x_search_calls=2),
            lambda value: value["claims"].update(researcher_mapping_authorized=True),
            lambda value: value["owner_decisions"].update(retention_policy="unbounded"),
            lambda value: value.update(extra="field"),
        ):
            mutated = copy.deepcopy(self.request)
            mutate(mutated)
            self.assertTrue(validate_live_request(mutated))

    def test_tool_proof_uses_only_structured_tool_events(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "updates.jsonl"
            path.write_text(
                "\n".join(
                    (
                        json.dumps(
                            {
                                "sessionUpdate": "user_message_chunk",
                                "_meta": {"modelId": "grok-4.5"},
                            }
                        ),
                        json.dumps(
                            {
                                "sessionUpdate": "agent_message_chunk",
                                "content": "The prompt requested x_search and prohibited web_search.",
                            }
                        ),
                        json.dumps(
                            {
                                "sessionUpdate": "tool_call",
                                "toolCallId": "call-1",
                                "title": "X Search",
                                "status": "in_progress",
                            }
                        ),
                        json.dumps(
                            {
                                "sessionUpdate": "tool_call_update",
                                "toolCallId": "call-1",
                                "status": "completed",
                                "rawOutput": _raw_x_output(),
                            }
                        ),
                    )
                )
                + "\n",
                encoding="utf-8",
            )
            proof = extract_tool_proof(path)
        self.assertEqual(proof.x_search_calls, 1)
        self.assertEqual(proof.x_search_completed_calls, 1)
        self.assertEqual(proof.unexpected_tool_calls, ())
        self.assertEqual(
            proof.raw_result_post_pairs,
            (("1900000000000000001", "https://x.com/OpenAI/status/1900000000000000001"),),
        )
        self.assertEqual(proof.raw_result_author_user_ids, ("4398626122",))
        self.assertEqual(proof.observed_model_ids, ("grok-4.5",))

    def test_tool_proof_rejects_generic_web_fallback(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "updates.jsonl"
            path.write_text(
                json.dumps(
                    {
                        "sessionUpdate": "tool_call",
                        "toolCallId": "call-web",
                        "title": "Web Search",
                        "status": "completed",
                    }
                )
                + "\n",
                encoding="utf-8",
            )
            proof = extract_tool_proof(path)
        self.assertEqual(proof.x_search_calls, 0)
        self.assertEqual(proof.unexpected_tool_calls, ("web_search",))

    def test_tool_proof_rejects_duplicate_unknown_and_local_tool_events(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "updates.jsonl"
            events = [
                {
                    "sessionUpdate": "tool_call",
                    "toolCallId": "duplicate",
                    "title": "X Search",
                    "status": "in_progress",
                },
                {
                    "sessionUpdate": "tool_call",
                    "toolCallId": "duplicate",
                    "title": "X Search",
                    "status": "in_progress",
                },
                {
                    "sessionUpdate": "tool_call_update",
                    "toolCallId": "isolated",
                    "status": "completed",
                },
                {
                    "sessionUpdate": "tool_call",
                    "toolCallId": "shell",
                    "title": "Run Terminal Cmd",
                    "status": "completed",
                },
            ]
            path.write_text("\n".join(json.dumps(event) for event in events) + "\n", encoding="utf-8")
            proof = extract_tool_proof(path)
        self.assertEqual(proof.x_search_calls, 1)
        self.assertEqual(
            proof.unexpected_tool_calls,
            ("duplicate_tool_call_id", "run_terminal_cmd", "unknown"),
        )

    def test_process_monitor_kills_on_second_x_call_before_waiting_for_completion(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            updates = root / "updates.jsonl"
            events = [
                {
                    "sessionUpdate": "tool_call",
                    "toolCallId": f"x-call-{index}",
                    "title": "X Search",
                }
                for index in (1, 2)
            ]
            updates.write_text("\n".join(json.dumps(event) for event in events) + "\n", encoding="utf-8")
            started = datetime.now(UTC)
            completed = _run_bounded_command(
                [sys.executable, "-c", "import time; time.sleep(30)"],
                cwd=root,
                environment={"PATH": "/usr/bin:/bin", "HOME": str(root)},
                updates_path=updates,
            )
            elapsed = (datetime.now(UTC) - started).total_seconds()
        self.assertEqual(completed.stop_reason, "tool_kill_switch_tripped")
        self.assertLess(elapsed, 2)

    def test_result_distinguishes_x_post_proof_from_stable_identity_proof(self) -> None:
        common = {
            "request": self.request,
            "run_id": "xprobe_run_0123456789abcdef0123456789abcdef",
            "session_id": "01234567-89ab-4cde-8fab-0123456789ab",
            "started_at": "2026-07-14T09:00:00.000Z",
            "completed_at": "2026-07-14T09:00:01.000Z",
            "elapsed_ms": 1000,
            "proof": _proof(),
            "approval_receipt_sha256": "b" * 64,
            "grok_binary_sha256": PINNED_GROK_BINARY_SHA256,
            "tool_receipt_sha256": "c" * 64,
        }
        ready = build_live_result(
            **common,
            outer=_outer_response(_inner_response()),
            inner=_inner_response(),
        )
        self.assertEqual(ready["schema_version"], LIVE_RESULT_SCHEMA_VERSION)
        self.assertEqual(ready["capability"]["verdict"], "x_native_identity_ready")
        self.assertTrue(ready["capability"]["x_native_access_proven"])
        self.assertTrue(ready["capability"]["stable_account_id_proven"])
        self.assertTrue(ready["capability"]["stage2_eligible_for_owner_review"])
        self.assertEqual(validate_live_result(ready, request=self.request), [])

        no_stable_id = _inner_response(stable_user_id=None)
        no_stable_common = {**common, "proof": _proof(stable_user_id=None)}
        retrieval_only = build_live_result(
            **no_stable_common,
            outer=_outer_response(no_stable_id, cost=None),
            inner=no_stable_id,
        )
        self.assertEqual(retrieval_only["capability"]["verdict"], "post_retrieval_only")
        self.assertTrue(retrieval_only["capability"]["x_native_access_proven"])
        self.assertFalse(retrieval_only["capability"]["stable_account_id_proven"])
        self.assertFalse(retrieval_only["capability"]["stage2_eligible_for_owner_review"])
        self.assertEqual(retrieval_only["usage"]["cost_status"], "unreported")
        self.assertIsNone(retrieval_only["usage"]["cost_usd"])
        self.assertEqual(validate_live_result(retrieval_only, request=self.request), [])

    def test_result_drops_observations_without_verified_x_tool_provenance(self) -> None:
        inner = _inner_response()
        result = build_live_result(
            request=self.request,
            run_id="xprobe_run_0123456789abcdef0123456789abcdef",
            session_id="01234567-89ab-4cde-8fab-0123456789ab",
            started_at="2026-07-14T09:00:00.000Z",
            completed_at="2026-07-14T09:00:01.000Z",
            elapsed_ms=1000,
            outer=_outer_response(inner),
            inner=inner,
            proof=_proof(x_calls=0, completed=0),
            approval_receipt_sha256="b" * 64,
            grok_binary_sha256=PINNED_GROK_BINARY_SHA256,
            tool_receipt_sha256="c" * 64,
        )
        self.assertEqual(result["capability"]["verdict"], "capability_unavailable")
        self.assertEqual(result["observations"], [])
        self.assertEqual(result["usage"]["observations"], 0)
        self.assertEqual(validate_live_result(result, request=self.request), [])

    def test_result_drops_model_claim_when_raw_x_receipt_does_not_match(self) -> None:
        inner = _inner_response()
        proof = _proof()
        mismatched = ToolProof(
            **{
                **proof.__dict__,
                "raw_result_post_pairs": (("1900000000000000002", "https://x.com/OpenAI/status/1900000000000000002"),),
            }
        )
        result = build_live_result(
            request=self.request,
            run_id="xprobe_run_0123456789abcdef0123456789abcdef",
            session_id="01234567-89ab-4cde-8fab-0123456789ab",
            started_at="2026-07-14T09:00:00.000Z",
            completed_at="2026-07-14T09:00:01.000Z",
            elapsed_ms=1000,
            outer=_outer_response(inner),
            inner=inner,
            proof=mismatched,
            approval_receipt_sha256="b" * 64,
            grok_binary_sha256=PINNED_GROK_BINARY_SHA256,
            tool_receipt_sha256="c" * 64,
        )
        self.assertEqual(result["capability"]["verdict"], "capability_unavailable")
        self.assertEqual(result["observations"], [])

    def test_runner_requires_explicit_gate_and_uses_ephemeral_grok_home(self) -> None:
        with self.assertRaises(PermissionError):
            run_live_probe(execute_live=False)

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            binary = root / "grok"
            binary.write_text("#!/bin/sh\nexit 1\n", encoding="utf-8")
            binary.chmod(stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR)
            auth = root / "auth.json"
            auth.write_text('{"private":"credential-material"}', encoding="utf-8")
            auth.chmod(0o600)
            inner = _inner_response()
            binary_digest = hashlib.sha256(binary.read_bytes()).hexdigest()

            def fake_run(command: list[str], **kwargs: object) -> BoundedCommandResult:
                environment = kwargs["environment"]
                self.assertIsInstance(environment, dict)
                grok_home = Path(environment["GROK_HOME"])  # type: ignore[index]
                self.assertNotEqual(grok_home, Path.home() / ".grok")
                self.assertNotIn("XAI_API_KEY", environment)  # type: ignore[operator]
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
                self.assertIn("--disable-web-search", command)
                self.assertNotIn("--tools", command)
                self.assertEqual(command[command.index("--sandbox") + 1], "read-only")
                self.assertNotEqual(Path(kwargs["cwd"]), ROOT)
                updates = Path(kwargs["updates_path"])
                updates.parent.mkdir(parents=True)
                updates.write_text(
                    "\n".join(
                        (
                            json.dumps(
                                {
                                    "sessionUpdate": "tool_call",
                                    "toolCallId": "x-call",
                                    "title": "X Search",
                                    "status": "in_progress",
                                }
                            ),
                            json.dumps(
                                {
                                    "sessionUpdate": "tool_call_update",
                                    "toolCallId": "x-call",
                                    "status": "completed",
                                    "rawOutput": _raw_x_output(),
                                }
                            ),
                            json.dumps(
                                {
                                    "sessionUpdate": "user_message_chunk",
                                    "_meta": {"modelId": "grok-4.5"},
                                }
                            ),
                        )
                    )
                    + "\n",
                    encoding="utf-8",
                )
                return BoundedCommandResult(
                    returncode=0,
                    stdout=json.dumps(_outer_response(inner)).encode("utf-8"),
                    stderr=b"",
                    stop_reason=None,
                )

            with (
                mock.patch("x_first.live_probe._run_bounded_command", side_effect=fake_run),
                mock.patch("x_first.live_probe.project_root", return_value=root),
                mock.patch("x_first.live_probe.PINNED_GROK_BINARY_SHA256", binary_digest),
            ):
                result, artifact_root = run_live_probe(
                    execute_live=True,
                    grok_binary=binary,
                    auth_path=auth,
                )
            self.assertEqual(result["capability"]["verdict"], "x_native_identity_ready")
            with mock.patch("x_first.live_probe.PINNED_GROK_BINARY_SHA256", binary_digest):
                self.assertEqual(
                    validate_artifact_pair(artifact_root / "request.json", artifact_root / "result.json"), []
                )
                tool_path = artifact_root / "tool-receipt.json"
                tool_receipt = json.loads(tool_path.read_text(encoding="utf-8"))
                tool_receipt["calls"][0]["raw_result_posts"][0]["platform_object_id"] = "1900000000000000002"
                tool_path.write_text(json.dumps(tool_receipt), encoding="utf-8")
                tool_path.chmod(0o600)
                self.assertTrue(validate_artifact_pair(artifact_root / "request.json", artifact_root / "result.json"))
            rendered = (artifact_root / "result.json").read_text(encoding="utf-8")
            self.assertNotIn("credential-material", rendered)
            self.assertEqual(stat.S_IMODE((artifact_root / "result.json").stat().st_mode), 0o600)
            self.assertTrue((artifact_root / "approval-receipt.json").is_file())
            self.assertTrue((artifact_root / "tool-receipt.json").is_file())
            with (
                mock.patch("x_first.live_probe.project_root", return_value=root),
                mock.patch("x_first.live_probe.PINNED_GROK_BINARY_SHA256", binary_digest),
            ):
                with self.assertRaises(PermissionError):
                    run_live_probe(execute_live=True, grok_binary=binary, auth_path=auth)
            deletion_receipts = purge_expired_live_artifacts(
                runtime_root=root / "runtime/live-probes",
                now=datetime.now(UTC) + timedelta(days=2),
            )
            self.assertEqual([receipt["run_id"] for receipt in deletion_receipts], [artifact_root.name])
            self.assertFalse(artifact_root.exists())


if __name__ == "__main__":
    unittest.main()
