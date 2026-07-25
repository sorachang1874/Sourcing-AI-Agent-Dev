"""Offline tests for the committed DeepSeek chat-completions Luna transport.

Every test stubs the HTTP boundary (the ``HttpClient`` seam, mirroring the
canary's offline style); no network, no real API key, no live provider call.
"""

from __future__ import annotations

import json
import os
import sys
import tempfile
import threading
import unittest
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from x_first import deepseek_luna_transport as dlt  # noqa: E402
from x_first import luna_batch_runner as lbr  # noqa: E402
from x_first.luna_live_canary import HttpResponse  # noqa: E402
from x_first.profile_bio_semantic_v2 import canonical_json, canonical_sha256  # noqa: E402

API_KEY = "sk-offlinefaketestkey0001"  # synthetic; matches the accepted key shape only


class FakeHttpClient:
    """Deterministic offline ``HttpClient``: queued script or per-request responder."""

    def __init__(
        self,
        *,
        script: list[Any] | None = None,
        responder: Any | None = None,
    ) -> None:
        self._script = list(script or [])
        self._responder = responder
        self.calls: list[dict[str, Any]] = []
        self._lock = threading.Lock()

    def request(
        self,
        *,
        method: str,
        url: str,
        headers: Any,
        body: bytes | None,
        timeout_ms: int,
        max_response_bytes: int,
    ) -> HttpResponse:
        with self._lock:
            self.calls.append(
                {
                    "method": method,
                    "url": url,
                    "headers": dict(headers),
                    "body": bytes(body) if body is not None else None,
                    "timeout_ms": timeout_ms,
                    "max_response_bytes": max_response_bytes,
                }
            )
        if self._responder is not None:
            return self._responder(json.loads(body.decode("utf-8")))
        with self._lock:
            if not self._script:
                raise AssertionError("unscripted_http_call")
            action = self._script.pop(0)
        if isinstance(action, Exception):
            raise action
        return action


def chat_response(judged: Any, *, model: str = dlt.SERVED_MODEL_ID) -> HttpResponse:
    body = json.dumps(
        {
            "id": "chatcmpl-offline-fake",
            "object": "chat.completion",
            "created": 1784000000,
            "model": model,
            "choices": [
                {
                    "index": 0,
                    "message": {"role": "assistant", "content": canonical_json(judged)},
                    "finish_reason": "stop",
                }
            ],
            "usage": {"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2},
        }
    ).encode("utf-8")
    return HttpResponse(status_code=200, headers={"content-type": "application/json"}, body=body)


def http_error(status: int) -> HttpResponse:
    return HttpResponse(
        status_code=status,
        headers={"content-type": "application/json"},
        body=b'{"error":{"message":"offline fake error"}}',
    )


class DeepSeekLunaTransportTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.fixture = lbr.load_json(ROOT / "fixtures/luna_candidate_review_fixture_v1.json")
        cls.seeds = cls.fixture["seeds"]
        cls.bundles = cls.fixture["bundles"]
        cls.outputs = cls.fixture["model_outputs"]
        cls.refs = [seed["seed_ref"] for seed in cls.seeds]

    def setUp(self) -> None:
        self.sleeps: list[float] = []
        self.transport: dlt.DeepSeekChatCompletionsLunaTransport | None = None

    def _transport(self, client: FakeHttpClient, **overrides: Any) -> dlt.DeepSeekChatCompletionsLunaTransport:
        return dlt.DeepSeekChatCompletionsLunaTransport(
            api_key=API_KEY,
            http_client=client,
            sleeper=self.sleeps.append,
            **overrides,
        )

    def _approval(self, refs: list[str] | None = None) -> dict[str, Any]:
        return {
            "schema_version": lbr.APPROVAL_RECEIPT_SCHEMA_VERSION,
            "approval_id": self.fixture["approval_id"],
            "approved_at": "2026-07-20T00:00:00.000Z",
            "candidate_refs_sha256": canonical_sha256(list(refs if refs is not None else self.refs)),
        }

    def _payload(self, ref: str) -> dict[str, Any]:
        binding = dlt.deepseek_judgment_binding()
        prompt = lbr.load_prompt(binding=binding)
        seed = next(seed for seed in self.seeds if seed["seed_ref"] == ref)
        return lbr.build_luna_responses_payload(seed, self.bundles[ref], prompt=prompt, binding=binding)

    def _responder(self, body: dict[str, Any]) -> HttpResponse:
        ref = json.loads(body["messages"][1]["content"])["candidate_ref"]
        return chat_response(self.outputs[ref])

    def _run_deepseek_batch(self, client: FakeHttpClient, **overrides: Any) -> dict[str, Any]:
        self.transport = self._transport(client)
        return lbr.run_luna_batch(
            seeds=self.seeds,
            bundles=self.bundles,
            transport=self.transport,
            approval=overrides.pop("approval", self._approval()),
            binding=dlt.deepseek_judgment_binding(),
            worker_count=4,
            **overrides,
        )

    # ------------------------------------------------------------------
    # Binding + key gate.
    # ------------------------------------------------------------------

    def test_binding_records_served_identity_and_recomputes_prompt_hash(self) -> None:
        binding = dlt.deepseek_judgment_binding()
        self.assertEqual(binding.provider_id, "deepseek_chat_completions_translated")
        self.assertEqual(binding.endpoint, "https://api.deepseek.com/chat/completions")
        self.assertEqual(binding.model_id, "deepseek-v4-flash")
        self.assertNotEqual(binding.prompt_sha256, lbr.CANONICAL_PROMPT_SHA256)
        prompt = lbr.load_json(ROOT / lbr.PROMPT_PATH)
        prompt["model_id"] = "deepseek-v4-flash"
        self.assertEqual(binding.prompt_sha256, canonical_sha256(prompt))
        loaded = lbr.load_prompt(binding=binding)
        self.assertEqual(loaded["model_id"], "deepseek-v4-flash")
        self.assertEqual(loaded["developer_instructions"], prompt["developer_instructions"])

    def test_missing_or_malformed_key_fails_closed_before_any_call(self) -> None:
        client = FakeHttpClient()
        with self.assertRaisesRegex(PermissionError, "deepseek_api_key_missing_or_invalid"):
            dlt.DeepSeekChatCompletionsLunaTransport(environ={}, http_client=client)
        with self.assertRaisesRegex(PermissionError, "deepseek_api_key_missing_or_invalid"):
            dlt.DeepSeekChatCompletionsLunaTransport(
                environ={dlt.KEY_ENVIRONMENT_VARIABLE: "not-a-key"}, http_client=client
            )
        with self.assertRaisesRegex(PermissionError, "deepseek_api_key_missing_or_invalid"):
            dlt.DeepSeekChatCompletionsLunaTransport(api_key="short", http_client=client)
        with mock.patch.dict(os.environ, {}, clear=True):
            with self.assertRaisesRegex(PermissionError, "deepseek_api_key_missing_or_invalid"):
                dlt.DeepSeekChatCompletionsLunaTransport(http_client=client)
        with mock.patch.dict(os.environ, {dlt.KEY_ENVIRONMENT_VARIABLE: API_KEY}):
            transport = dlt.DeepSeekChatCompletionsLunaTransport(http_client=client, sleeper=self.sleeps.append)
        self.assertEqual(transport.attempts, [])
        self.assertEqual(client.calls, [])

    def test_constructor_rejects_bad_retry_budgets(self) -> None:
        client = FakeHttpClient()
        for bad in (0, dlt.MAX_ATTEMPTS + 1):
            with self.assertRaisesRegex(ValueError, "deepseek_max_attempts_invalid"):
                self._transport(client, max_attempts=bad)
        with self.assertRaisesRegex(ValueError, "deepseek_retry_backoff_invalid"):
            self._transport(client, retry_backoff_s=-1.0)

    # ------------------------------------------------------------------
    # Request translation + response envelope.
    # ------------------------------------------------------------------

    def test_request_translation_auth_header_and_envelope(self) -> None:
        ref = self.refs[0]
        client = FakeHttpClient(script=[chat_response(self.outputs[ref])])
        transport = self._transport(client)
        payload = self._payload(ref)
        envelope = transport.complete(payload=payload, timeout_ms=300_000)

        self.assertEqual(len(client.calls), 1)
        call = client.calls[0]
        self.assertEqual(call["method"], "POST")
        self.assertEqual(call["url"], dlt.CHAT_COMPLETIONS_URL)
        self.assertEqual(
            call["headers"],
            {"Authorization": f"Bearer {API_KEY}", "Content-Type": "application/json"},
        )
        self.assertEqual(call["timeout_ms"], 300_000)
        self.assertEqual(call["max_response_bytes"], dlt.MAX_RESPONSE_BYTES)

        body = json.loads(call["body"].decode("utf-8"))
        self.assertEqual(body["model"], "deepseek-reasoner")
        self.assertEqual([message["role"] for message in body["messages"]], ["system", "user"])
        binding = dlt.deepseek_judgment_binding()
        prompt = lbr.load_prompt(binding=binding)
        system = body["messages"][0]["content"]
        self.assertTrue(system.startswith(prompt["developer_instructions"]))
        self.assertIn("\n\n# Output contract\n", system)
        self.assertIn(json.dumps(lbr.judged_output_schema(), ensure_ascii=False), system)
        # The user message is exactly the runner-built candidate bundle payload:
        # the span-citation contract (post/bio/fact item refs) is preserved verbatim.
        user_text = body["messages"][1]["content"]
        self.assertEqual(user_text, payload["input"][0]["content"][0]["text"])
        source_payload = json.loads(user_text)
        self.assertEqual(source_payload["candidate_ref"], ref)
        self.assertEqual(source_payload["judged_bundle_sha256"], payload["metadata"]["judged_bundle_sha256"])
        judged_refs = {item["item_ref"] for item in source_payload["judged_bundle_manifest"]}
        for axis_citations in (
            self.outputs[ref]["lab_affiliation_evidence_citations"],
            self.outputs[ref]["pretraining_experience_evidence_citations"],
        ):
            self.assertTrue(set(axis_citations) <= judged_refs)
        self.assertEqual(body["response_format"], {"type": "json_object"})
        self.assertEqual(body["max_tokens"], 8192)

        judged, returned_model = lbr.extract_judged_output(envelope, binding=binding)
        self.assertEqual(returned_model, "deepseek-v4-flash")
        self.assertEqual(json.loads(canonical_json(judged)), json.loads(canonical_json(self.outputs[ref])))
        self.assertEqual(len(transport.attempts), 1)
        self.assertEqual(
            transport.attempts[0],
            {
                "candidate_ref": ref,
                "attempt": 1,
                "status_code": 200,
                "outcome": "completed",
                "error_code": None,
            },
        )

    # ------------------------------------------------------------------
    # Retry / failure surface.
    # ------------------------------------------------------------------

    def test_transport_error_retried_with_backoff_then_exhausted(self) -> None:
        client = FakeHttpClient(script=[RuntimeError("deepseek_http_transport_failed")] * 3)
        transport = self._transport(client)
        with self.assertRaisesRegex(lbr.LunaBatchRunnerError, "luna_transport_retry_exhausted"):
            transport.complete(payload=self._payload(self.refs[0]), timeout_ms=1000)
        self.assertEqual(len(client.calls), 3)
        self.assertEqual(self.sleeps, [1.0, 2.0])
        self.assertEqual(
            [(row["attempt"], row["status_code"], row["outcome"], row["error_code"]) for row in transport.attempts],
            [
                (1, None, "retry_scheduled", "deepseek_http_transport_failed"),
                (2, None, "retry_scheduled", "deepseek_http_transport_failed"),
                (3, None, "failed", "deepseek_http_transport_failed"),
            ],
        )

    def test_retryable_http_statuses_recover_on_later_attempt(self) -> None:
        ref = self.refs[0]
        client = FakeHttpClient(script=[http_error(429), http_error(500), chat_response(self.outputs[ref])])
        transport = self._transport(client)
        envelope = transport.complete(payload=self._payload(ref), timeout_ms=1000)
        self.assertEqual(envelope["status"], "completed")
        self.assertEqual(len(client.calls), 3)
        self.assertEqual(self.sleeps, [1.0, 2.0])
        self.assertEqual(
            [(row["status_code"], row["outcome"]) for row in transport.attempts],
            [(429, "retry_scheduled"), (500, "retry_scheduled"), (200, "completed")],
        )

    def test_retryable_http_statuses_exhaust_to_deterministic_failure(self) -> None:
        client = FakeHttpClient(script=[http_error(503), http_error(429)])
        transport = self._transport(client, max_attempts=2)
        with self.assertRaisesRegex(lbr.LunaBatchRunnerError, "luna_transport_retry_exhausted"):
            transport.complete(payload=self._payload(self.refs[0]), timeout_ms=1000)
        self.assertEqual(len(client.calls), 2)
        self.assertEqual(transport.attempts[-1]["outcome"], "failed")
        self.assertEqual(transport.attempts[-1]["error_code"], "luna_http_status_retryable:429")

    def test_terminal_http_status_fails_fast_without_retry(self) -> None:
        for status in (400, 401, 403, 404, 422):
            sleeps: list[float] = []
            client = FakeHttpClient(script=[http_error(status)])
            transport = dlt.DeepSeekChatCompletionsLunaTransport(
                api_key=API_KEY, http_client=client, sleeper=sleeps.append
            )
            with self.assertRaisesRegex(lbr.LunaBatchRunnerError, f"luna_http_status_rejected:{status}"):
                transport.complete(payload=self._payload(self.refs[0]), timeout_ms=1000)
            self.assertEqual(len(client.calls), 1)
            self.assertEqual(sleeps, [])
            self.assertEqual(transport.attempts[-1]["outcome"], "failed")
            self.assertEqual(transport.attempts[-1]["error_code"], f"luna_http_status_rejected:{status}")

    def test_undecodable_body_retried_then_success(self) -> None:
        ref = self.refs[0]
        garbage = HttpResponse(status_code=200, headers={"content-type": "application/json"}, body=b"not json{")
        client = FakeHttpClient(script=[garbage, chat_response(self.outputs[ref])])
        transport = self._transport(client)
        envelope = transport.complete(payload=self._payload(ref), timeout_ms=1000)
        self.assertEqual(envelope["status"], "completed")
        self.assertEqual(len(client.calls), 2)
        self.assertEqual(transport.attempts[0]["error_code"], "luna_response_json_invalid")
        self.assertEqual(transport.attempts[0]["outcome"], "retry_scheduled")

    def test_undecodable_body_exhausts_to_deterministic_failure(self) -> None:
        garbage = HttpResponse(status_code=200, headers={"content-type": "application/json"}, body=b"[1,2,3]")
        client = FakeHttpClient(script=[garbage, garbage])
        transport = self._transport(client, max_attempts=2)
        with self.assertRaisesRegex(lbr.LunaBatchRunnerError, "luna_transport_retry_exhausted"):
            transport.complete(payload=self._payload(self.refs[0]), timeout_ms=1000)
        self.assertEqual(len(client.calls), 2)

    def test_envelope_shape_violation_fails_fast_without_retry(self) -> None:
        bad_bodies = (
            b'{"model":"deepseek-v4-flash","choices":[]}',
            b'{"model":"deepseek-v4-flash","choices":[{"message":{"role":"assistant","content":"  "}}]}',
            b'{"model":"deepseek-v4-flash","choices":[{"message":null}]}',
        )
        for raw in bad_bodies:
            client = FakeHttpClient(
                script=[HttpResponse(status_code=200, headers={"content-type": "application/json"}, body=raw)]
            )
            transport = self._transport(client)
            with self.assertRaisesRegex(lbr.LunaBatchRunnerError, "luna_response_invalid"):
                transport.complete(payload=self._payload(self.refs[0]), timeout_ms=1000)
            self.assertEqual(len(client.calls), 1)
            self.assertEqual(transport.attempts[-1]["outcome"], "failed")
            self.assertEqual(transport.attempts[-1]["error_code"], "luna_response_invalid")

    def test_malformed_request_payload_fails_closed(self) -> None:
        client = FakeHttpClient()
        transport = self._transport(client)
        with self.assertRaisesRegex(lbr.LunaBatchRunnerError, "luna_request_payload_invalid"):
            transport.complete(payload={"model": "deepseek-v4-flash"}, timeout_ms=1000)
        self.assertEqual(client.calls, [])

    # ------------------------------------------------------------------
    # Composition with the runner: approval gate, schema fail-closed, receipts.
    # ------------------------------------------------------------------

    def test_batch_end_to_end_receipts_record_deepseek_binding(self) -> None:
        client = FakeHttpClient(responder=self._responder)
        batch = self._run_deepseek_batch(client)
        binding = dlt.deepseek_judgment_binding()
        self.assertEqual(batch["completed_count"], len(self.refs))
        self.assertEqual(batch["failed_count"], 0)
        self.assertEqual(batch["prompt_sha256"], binding.prompt_sha256)
        for row in batch["results"]:
            receipt = row["execution_receipt"]
            self.assertEqual(receipt["provider"], "deepseek_chat_completions_translated")
            self.assertEqual(receipt["endpoint"], "https://api.deepseek.com/chat/completions")
            self.assertEqual(receipt["requested_model"], "deepseek-v4-flash")
            self.assertEqual(receipt["returned_model"], "deepseek-v4-flash")
            self.assertTrue(receipt["exact_model_match"])
            self.assertEqual(receipt["prompt_sha256"], binding.prompt_sha256)
            self.assertEqual(receipt["outcome"], "completed")
            self.assertEqual(row["review"]["authority_status"], "diagnostic_only_unattested")
        # The API key never leaks into receipts, attempts, or the batch artifact.
        self.assertNotIn(API_KEY, json.dumps(batch))
        # Attempt receipts cover every judged candidate exactly once.
        assert self.transport is not None
        self.assertEqual(
            sorted(row["candidate_ref"] for row in self.transport.attempts),
            sorted(self.refs),
        )

    def test_batch_composes_with_approval_gate_before_any_http_call(self) -> None:
        client = FakeHttpClient(responder=self._responder)
        with self.assertRaisesRegex(PermissionError, "luna_approval_receipt_required"):
            self._run_deepseek_batch(client, approval=None)
        self.assertEqual(client.calls, [])

    def test_batch_returned_model_mismatch_fails_closed(self) -> None:
        def responder(body: dict[str, Any]) -> HttpResponse:
            ref = json.loads(body["messages"][1]["content"])["candidate_ref"]
            return chat_response(self.outputs[ref], model="deepseek-other-model")

        batch = self._run_deepseek_batch(FakeHttpClient(responder=responder))
        self.assertEqual(batch["completed_count"], 0)
        self.assertEqual(batch["failed_count"], len(self.refs))
        for row in batch["results"]:
            self.assertEqual(row["status"], "failed")
            self.assertEqual(row["error_code"], "luna_response_model_mismatch")
            self.assertIsNone(row["review"])
            self.assertEqual(row["execution_receipt"]["outcome"], "failed")

    def test_batch_judged_output_schema_violations_fail_closed(self) -> None:
        def bad_state(body: dict[str, Any]) -> HttpResponse:
            ref = json.loads(body["messages"][1]["content"])["candidate_ref"]
            judged = dict(self.outputs[ref])
            judged["proposed_lab_affiliation_state"] = "employed"
            return chat_response(judged)

        batch = self._run_deepseek_batch(FakeHttpClient(responder=bad_state))
        self.assertEqual(batch["failed_count"], len(self.refs))
        for row in batch["results"]:
            self.assertEqual(row["error_code"], "luna_output_state_invalid")

        def bad_citation(body: dict[str, Any]) -> HttpResponse:
            ref = json.loads(body["messages"][1]["content"])["candidate_ref"]
            judged = dict(self.outputs[ref])
            judged["lab_affiliation_evidence_citations"] = ["post:9999999999999999999"]
            return chat_response(judged)

        batch = self._run_deepseek_batch(FakeHttpClient(responder=bad_citation))
        self.assertEqual(batch["failed_count"], len(self.refs))
        for row in batch["results"]:
            self.assertEqual(row["error_code"], "luna_output_citation_not_judged")

    def test_batch_terminal_http_failure_isolates_per_candidate(self) -> None:
        def responder(body: dict[str, Any]) -> HttpResponse:
            ref = json.loads(body["messages"][1]["content"])["candidate_ref"]
            if ref == self.refs[1]:
                return http_error(400)
            return chat_response(self.outputs[ref])

        client = FakeHttpClient(responder=responder)
        batch = self._run_deepseek_batch(client)
        self.assertEqual(batch["completed_count"], 1)
        self.assertEqual(batch["failed_count"], 1)
        by_ref = {row["candidate_ref"]: row for row in batch["results"]}
        self.assertEqual(by_ref[self.refs[0]]["status"], "completed")
        self.assertEqual(by_ref[self.refs[1]]["status"], "failed")
        self.assertEqual(by_ref[self.refs[1]]["error_code"], "luna_http_status_rejected:400")

    def test_attempt_receipts_thread_safe_under_worker_pool(self) -> None:
        client = FakeHttpClient(responder=self._responder)
        batch = self._run_deepseek_batch(client)
        self.assertEqual(batch["completed_count"], len(self.refs))
        assert self.transport is not None
        self.assertEqual(len(self.transport.attempts), len(self.refs))
        self.assertEqual(
            sorted(row["candidate_ref"] for row in self.transport.attempts),
            sorted(self.refs),
        )
        for row in self.transport.attempts:
            self.assertEqual(row["outcome"], "completed")
            self.assertEqual(row["attempt"], 1)
        self.assertNotIn(API_KEY, json.dumps(self.transport.attempts))

    # ------------------------------------------------------------------
    # Committed file driver with the DeepSeek binding.
    # ------------------------------------------------------------------

    def test_file_driver_runs_deepseek_binding_end_to_end(self) -> None:
        client = FakeHttpClient(responder=self._responder)
        transport = self._transport(client)
        binding = dlt.deepseek_judgment_binding()
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            seeds_path = tmp_path / "seeds.json"
            collection_path = tmp_path / "collection.json"
            out_dir = tmp_path / "out"
            seeds_path.write_text(json.dumps({"seeds": self.seeds}), encoding="utf-8")
            collection_path.write_text(
                json.dumps(
                    {
                        "results": [
                            {
                                "candidate_ref": ref,
                                "status": "completed",
                                "bundle": self.bundles[ref],
                            }
                            for ref in self.refs
                        ]
                    }
                ),
                encoding="utf-8",
            )
            batch = lbr.run_luna_batch_from_files(
                seeds_path=seeds_path,
                collection_path=collection_path,
                out_dir=out_dir,
                transport=transport,
                approval_id="deepseek_driver_test_v1",
                binding=binding,
                worker_count=4,
                wall_clock=lambda: datetime(2026, 7, 20, tzinfo=UTC),
            )
            self.assertEqual(batch["completed_count"], len(self.refs))
            self.assertEqual(batch["approval_receipt"]["approval_id"], "deepseek_driver_test_v1")
            self.assertEqual(batch["approval_receipt"]["approved_at"], "2026-07-20T00:00:00.000Z")
            self.assertEqual(batch["approval_receipt"]["candidate_refs_sha256"], canonical_sha256(self.refs))
            self.assertEqual(batch["prompt_sha256"], binding.prompt_sha256)
            for row in batch["results"]:
                self.assertEqual(row["execution_receipt"]["provider"], "deepseek_chat_completions_translated")
            written = json.loads((out_dir / "luna_batch.json").read_text(encoding="utf-8"))
            self.assertEqual(written, batch)
            self.assertNotIn(API_KEY, json.dumps(written))

    def test_supporting_context_flows_through_translation(self) -> None:
        block = {
            "source": "linkedin_raw_profile_envelope_v1",
            "candidate_ref": self.refs[0],
            "field_dictionary": {"about": "candidate-authored About text"},
            "raw_profile": {"about": "raw about text"},
        }
        seen: list[dict[str, Any]] = []

        def responder(body: dict[str, Any]) -> HttpResponse:
            seen.append(json.loads(body["messages"][1]["content"]))
            return chat_response(self.outputs[self.refs[0]])

        client = FakeHttpClient(responder=responder)
        transport = self._transport(client)
        binding = dlt.deepseek_judgment_binding()
        prompt = lbr.load_prompt(binding=binding)
        payload = lbr.build_luna_responses_payload(
            self.seeds[0],
            self.bundles[self.refs[0]],
            prompt=prompt,
            binding=binding,
            extra_source_context=block,
        )
        transport.complete(payload=payload, timeout_ms=1000)
        # The chat user message carries the full source payload including the
        # comprehension block; the sha binds it without touching judged_bundle_sha256.
        self.assertEqual(seen[0]["supporting_context"], block)
        self.assertEqual(payload["metadata"]["supporting_context_sha256"], canonical_sha256(block))
        source_digest = seen[0]["judged_bundle_sha256"]
        plain = lbr.build_luna_responses_payload(
            self.seeds[0], self.bundles[self.refs[0]], prompt=prompt, binding=binding
        )
        self.assertEqual(source_digest, plain["metadata"]["judged_bundle_sha256"])


if __name__ == "__main__":
    unittest.main()
