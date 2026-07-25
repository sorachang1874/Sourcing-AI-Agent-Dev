from __future__ import annotations

import contextlib
import copy
import hashlib
import io
import json
import os
import subprocess
import sys
import tempfile
import threading
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from x_first import luna_live_canary as canary  # noqa: E402
from x_first import profile_bio_semantic_legacy_v21 as semantic  # noqa: E402

FAKE_KEY = "sk-" + "a" * 32


def _json_response(value: Any, *, status: int = 200) -> canary.HttpResponse:
    return canary.HttpResponse(
        status_code=status,
        headers={"Content-Type": "application/json; charset=utf-8"},
        body=json.dumps(value, ensure_ascii=False).encode("utf-8"),
    )


def _catalog(*models: str) -> canary.HttpResponse:
    return _json_response({"object": "list", "data": [{"id": value} for value in models]})


def _provider_response(*, model: str = canary.MODEL_ID, malformed: bool = False) -> canary.HttpResponse:
    if malformed:
        return _json_response({"object": "response", "model": model, "status": "completed"})
    request = canary.build_synthetic_live_request()
    model_output = semantic.load_json(
        ROOT / "legacy/luna_canary_v1/fixtures/profile_bio_semantic_model_output.v2.1.json"
    )
    model_output["request_id"] = request["request_id"]
    return _json_response(
        {
            "id": "resp_fixture_luna_canary",
            "object": "response",
            "status": "completed",
            "model": model,
            "output": [
                {
                    "id": "msg_fixture_luna_canary",
                    "type": "message",
                    "status": "completed",
                    "role": "assistant",
                    "content": [
                        {
                            "type": "output_text",
                            "text": semantic.canonical_json(model_output),
                            "annotations": [],
                        }
                    ],
                }
            ],
            "usage": {"input_tokens": 600, "output_tokens": 300, "total_tokens": 900},
        }
    )


class FakeHttpClient:
    def __init__(self, responses: list[canary.HttpResponse | Exception]) -> None:
        self.responses = list(responses)
        self.calls: list[dict[str, Any]] = []
        self._lock = threading.Lock()

    def request(self, **kwargs: Any) -> canary.HttpResponse:
        with self._lock:
            self.calls.append(copy.deepcopy(kwargs))
            if not self.responses:
                raise AssertionError("unexpected external call")
            value = self.responses.pop(0)
        if isinstance(value, Exception):
            raise value
        return value


class LunaLiveCanaryTest(unittest.TestCase):
    def test_v1_is_frozen_to_semantic_v21_assets(self) -> None:
        request = canary.build_synthetic_live_request()
        self.assertEqual(request["schema_version"], "x.profile.bio_semantic.request.v2.1")
        self.assertEqual(semantic.REVIEW_SCHEMA_VERSION, "x.profile.bio_semantic.review.v2.1")
        self.assertEqual(
            canary._legacy_asset_root(),
            ROOT / "legacy/luna_canary_v1",
        )

    def test_v1_public_live_execution_is_unconditionally_disabled(self) -> None:
        client = FakeHttpClient([_catalog(canary.MODEL_ID), _provider_response()])
        with self.assertRaisesRegex(PermissionError, "legacy_v1_live_execution_disabled"):
            canary.run_luna_live_canary(
                execute_live=True,
                http_client=client,
                environ={canary.KEY_ENVIRONMENT_VARIABLE: FAKE_KEY},
            )
        self.assertEqual(client.calls, [])

    def test_v1_frozen_hash_manifest_and_golden_bundle_replay(self) -> None:
        manifest = json.loads(
            (ROOT / "legacy/luna_canary_v1/frozen_asset_manifest.v1.json").read_text(encoding="utf-8")
        )
        self.assertEqual(manifest["source_commit"], "f02c72c")
        self.assertEqual(
            manifest["freeze_policy"],
            "sha256_pin_semantic_assets_runner_result_schema_and_shared_cli",
        )
        for relative, expected_sha256 in manifest["files"].items():
            path = ROOT / "legacy/luna_canary_v1" / relative
            self.assertEqual(hashlib.sha256(path.read_bytes()).hexdigest(), expected_sha256)
        for relative, expected_sha256 in manifest["project_files"].items():
            path = ROOT / relative
            self.assertEqual(hashlib.sha256(path.read_bytes()).hexdigest(), expected_sha256)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            result, bundle, approval_root, _ = self._run(
                root,
                [_catalog(canary.MODEL_ID), _provider_response()],
            )
            self.assertEqual(result["status"], "completed")
            self.assertEqual(canary.validate_artifact_directory(bundle, approval_root=approval_root), [])

    def _run(
        self,
        root: Path,
        responses: list[canary.HttpResponse | Exception],
    ) -> tuple[dict[str, Any], Path, Path, FakeHttpClient]:
        client = FakeHttpClient(responses)
        approval_root = root / "approval"
        result, bundle = canary._run_luna_live_canary_fixture_v1(
            execute_live=True,
            http_client=client,
            environ={canary.KEY_ENVIRONMENT_VARIABLE: FAKE_KEY},
            runtime_root=root / "runtime",
            approval_root=approval_root,
        )
        return result, bundle, approval_root, client

    def test_no_flag_or_key_makes_zero_calls_and_zero_writes(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            client = FakeHttpClient([])
            for execute_live, environment in ((False, {canary.KEY_ENVIRONMENT_VARIABLE: FAKE_KEY}), (True, {})):
                runtime = root / f"runtime-{execute_live}-{len(environment)}"
                approval = root / f"approval-{execute_live}-{len(environment)}"
                with self.assertRaises(PermissionError):
                    canary._run_luna_live_canary_fixture_v1(
                        execute_live=execute_live,
                        http_client=client,
                        environ=environment,
                        runtime_root=runtime,
                        approval_root=approval,
                    )
                self.assertFalse(runtime.exists())
                self.assertFalse(approval.exists())
            self.assertEqual(client.calls, [])

    def test_unsafe_runtime_owner_fails_before_approval_or_calls(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            runtime = root / "runtime"
            runtime.mkdir(mode=0o755)
            runtime.chmod(0o755)
            approval = root / "approval"
            client = FakeHttpClient([_catalog(canary.MODEL_ID), _provider_response()])
            with self.assertRaisesRegex(ValueError, "private_directory_unsafe"):
                canary._run_luna_live_canary_fixture_v1(
                    execute_live=True,
                    http_client=client,
                    environ={canary.KEY_ENVIRONMENT_VARIABLE: FAKE_KEY},
                    runtime_root=runtime,
                    approval_root=approval,
                )
            self.assertEqual(client.calls, [])
            self.assertFalse(approval.exists())

    def test_catalog_missing_is_one_call_terminal_and_approval_cannot_replay(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            result, bundle, approval_root, client = self._run(root, [_catalog("gpt-5.5")])
            self.assertEqual(result["status"], "failed")
            self.assertEqual(result["error_codes"], ["catalog_model_missing"])
            self.assertEqual(
                result["calls"],
                {
                    "catalog_external_calls": 1,
                    "model_external_calls": 0,
                    "total_external_calls": 1,
                    "semantic_model_calls": 0,
                },
            )
            self.assertEqual(len(client.calls), 1)
            self.assertEqual(
                {path.name for path in bundle.iterdir()},
                {"request.json", "approval-receipt.json", "catalog-receipt.json", "result.json"},
            )
            self.assertEqual(canary.validate_artifact_directory(bundle, approval_root=approval_root), [])
            with self.assertRaises(PermissionError):
                canary._run_luna_live_canary_fixture_v1(
                    execute_live=True,
                    http_client=client,
                    environ={canary.KEY_ENVIRONMENT_VARIABLE: FAKE_KEY},
                    runtime_root=root / "second-runtime",
                    approval_root=approval_root,
                )
            self.assertEqual(len(client.calls), 1)

    def test_success_is_two_calls_and_valid_private_bundle(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            result, bundle, approval_root, client = self._run(
                root,
                [_catalog("other-model", canary.MODEL_ID), _provider_response()],
            )
            self.assertEqual(result["status"], "completed")
            self.assertEqual(result["error_codes"], [])
            self.assertEqual(result["calls"]["total_external_calls"], 2)
            self.assertEqual(result["calls"]["semantic_model_calls"], 1)
            self.assertEqual(result["identity"]["returned_model"], canary.MODEL_ID)
            self.assertIs(result["identity"]["exact_model_match"], True)
            self.assertEqual(result["usage"]["total_tokens"], 900)
            started = datetime.fromisoformat(result["timing"]["started_at"].replace("Z", "+00:00"))
            completed = datetime.fromisoformat(result["timing"]["completed_at"].replace("Z", "+00:00"))
            self.assertEqual(int((completed - started).total_seconds() * 1000), result["timing"]["elapsed_ms"])
            self.assertEqual([call["method"] for call in client.calls], ["GET", "POST"])
            post_payload = json.loads(client.calls[1]["body"])
            self.assertEqual(post_payload["model"], canary.MODEL_ID)
            self.assertEqual(post_payload["tools"], [])
            self.assertIs(post_payload["store"], False)
            self.assertEqual(canary.validate_artifact_directory(bundle, approval_root=approval_root), [])
            self.assertEqual(bundle.stat().st_mode & 0o777, 0o700)
            self.assertTrue(all(path.stat().st_mode & 0o777 == 0o600 for path in bundle.iterdir()))

    def test_reroute_and_invalid_responses_are_terminal_and_reproducible(self) -> None:
        cases = (
            (_provider_response(model="gpt-5.6-luna-shadow"), "response_model_mismatch"),
            (_provider_response(malformed=True), "semantic_review_failed"),
        )
        for index, (response, expected_error) in enumerate(cases):
            with self.subTest(expected_error=expected_error), tempfile.TemporaryDirectory() as directory:
                root = Path(directory) / str(index)
                result, bundle, approval_root, client = self._run(
                    root,
                    [_catalog(canary.MODEL_ID), response],
                )
                self.assertEqual(result["status"], "failed")
                self.assertEqual(result["error_codes"], [expected_error])
                self.assertEqual(len(client.calls), 2)
                self.assertEqual(result["semantic"]["review_status"], "failed")
                self.assertEqual(canary.validate_artifact_directory(bundle, approval_root=approval_root), [])

    def test_transport_failures_write_terminal_bundles(self) -> None:
        cases: tuple[tuple[list[canary.HttpResponse | Exception], str, int], ...] = (
            ([RuntimeError("catalog secret detail")], "catalog_transport_failed", 1),
            ([_catalog(canary.MODEL_ID), RuntimeError("response secret detail")], "response_transport_failed", 2),
        )
        for index, (responses, expected_error, expected_calls) in enumerate(cases):
            with self.subTest(expected_error=expected_error), tempfile.TemporaryDirectory() as directory:
                root = Path(directory) / str(index)
                result, bundle, approval_root, client = self._run(root, list(responses))
                self.assertEqual(result["status"], "failed")
                self.assertEqual(result["error_codes"], [expected_error])
                self.assertEqual(len(client.calls), expected_calls)
                self.assertFalse((bundle / "raw-response.json").exists())
                self.assertFalse((bundle / "semantic-review.json").exists())
                self.assertEqual(canary.validate_artifact_directory(bundle, approval_root=approval_root), [])

    def test_key_is_absent_from_artifacts_and_cli_failure_output(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            _, bundle, approval_root, _ = self._run(
                root,
                [_catalog(canary.MODEL_ID), _provider_response()],
            )
            corpus = b"".join(path.read_bytes() for path in bundle.iterdir())
            corpus += b"".join(path.read_bytes() for path in approval_root.iterdir())
            self.assertNotIn(FAKE_KEY.encode(), corpus)

            environment = {key: value for key, value in os.environ.items() if key != canary.KEY_ENVIRONMENT_VARIABLE}
            completed = subprocess.run(
                [sys.executable, str(ROOT / "scripts/run_luna_live_canary.py"), "--execute-live-v2"],
                check=False,
                capture_output=True,
                text=True,
                env=environment,
            )
            self.assertEqual(completed.returncode, 1)
            self.assertEqual(
                json.loads(completed.stdout),
                {"error": "LUNA_LIVE_CANARY_EXECUTION_FAILED", "status": "failed"},
            )
            self.assertNotIn("CHSHAPI", completed.stdout)

    def test_validator_rejects_tamper_extra_file_permissions_and_hash_changes(self) -> None:
        mutations = ("tamper", "extra", "permissions", "hash", "result-state")
        for mutation in mutations:
            with self.subTest(mutation=mutation), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                result, bundle, approval_root, _ = self._run(
                    root,
                    [_catalog(canary.MODEL_ID), _provider_response()],
                )
                if mutation == "tamper":
                    catalog = json.loads((bundle / "catalog-receipt.json").read_text())
                    catalog["model_count"] += 1
                    canary._atomic_write_json(bundle / "catalog-receipt.json", catalog)
                elif mutation == "extra":
                    canary._atomic_write_json(bundle / "extra.json", {"unexpected": True})
                elif mutation == "permissions":
                    (bundle / "request.json").chmod(0o640)
                elif mutation == "hash":
                    changed = copy.deepcopy(result)
                    changed["artifact_sha256s"]["request.json"] = "0" * 64
                    canary._atomic_write_json(bundle / "result.json", changed)
                else:
                    changed = copy.deepcopy(result)
                    changed["status"] = "failed"
                    changed["error_codes"] = ["semantic_review_failed"]
                    canary._atomic_write_json(bundle / "result.json", changed)
                self.assertNotEqual(canary.validate_artifact_directory(bundle, approval_root=approval_root), [])

    def test_validator_rejects_coherently_rehashed_impossible_receipts_and_bool_counters(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            result, bundle, approval_root, _ = self._run(root, [_catalog("gpt-5.5")])
            catalog_path = bundle / "catalog-receipt.json"
            result_path = bundle / "result.json"
            catalog = json.loads(catalog_path.read_text())
            catalog["distinct_model_count"] = catalog["model_count"] + 1
            changed = copy.deepcopy(result)
            changed["catalog"]["distinct_model_count"] = catalog["distinct_model_count"]
            changed["artifact_sha256s"]["catalog-receipt.json"] = canary._canonical_sha256(catalog)
            canary._atomic_write_json(catalog_path, catalog)
            canary._atomic_write_json(result_path, changed)
            self.assertNotEqual(canary.validate_artifact_directory(bundle, approval_root=approval_root), [])

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            result, bundle, approval_root, _ = self._run(
                root,
                [_catalog(canary.MODEL_ID), _provider_response()],
            )
            changed = copy.deepcopy(result)
            changed["timing"]["elapsed_ms"] = False
            changed["timing"]["completed_at"] = changed["timing"]["started_at"]
            started = canary._parse_timestamp(changed["timing"]["started_at"])
            changed["retention"]["delete_after"] = canary._timestamp(started + timedelta(hours=24))
            canary._atomic_write_json(bundle / "result.json", changed)
            self.assertNotEqual(canary.validate_artifact_directory(bundle, approval_root=approval_root), [])

    def test_secret_echo_is_redacted_and_terminal(self) -> None:
        secret_bodies = (
            json.dumps({"model": canary.MODEL_ID, "echo": FAKE_KEY}).encode(),
            ('{"model":"gpt-5.6-luna","echo":"sk\\u002d' + "a" * 32 + '"}').encode(),
        )
        for body in secret_bodies:
            with self.subTest(body=body), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                secret_echo = canary.HttpResponse(200, {"content-type": "application/json"}, body)
                result, bundle, approval_root, _ = self._run(
                    root,
                    [_catalog(canary.MODEL_ID), secret_echo],
                )
                self.assertEqual(result["error_codes"], ["response_secret_detected"])
                raw = json.loads((bundle / "raw-response.json").read_text())
                self.assertIs(raw["secret_redacted"], True)
                self.assertIsNone(raw["body_base64"])
                self.assertNotIn(FAKE_KEY.encode(), b"".join(path.read_bytes() for path in bundle.iterdir()))
                self.assertEqual(canary.validate_artifact_directory(bundle, approval_root=approval_root), [])

                raw_path = bundle / "raw-response.json"
                result_path = bundle / "result.json"
                raw["body_base64"] = "cmV0YWluZWQ="
                changed = copy.deepcopy(result)
                changed["artifact_sha256s"]["raw-response.json"] = canary._canonical_sha256(raw)
                canary._atomic_write_json(raw_path, raw)
                canary._atomic_write_json(result_path, changed)
                self.assertNotEqual(canary.validate_artifact_directory(bundle, approval_root=approval_root), [])

    def test_global_approval_has_one_concurrent_winner(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            approval_root = root / "approval"
            client = FakeHttpClient([_catalog(canary.MODEL_ID), _provider_response()])
            barrier = threading.Barrier(2)
            outcomes: list[str] = []
            lock = threading.Lock()

            def worker(index: int) -> None:
                barrier.wait()
                try:
                    canary._run_luna_live_canary_fixture_v1(
                        execute_live=True,
                        http_client=client,
                        environ={canary.KEY_ENVIRONMENT_VARIABLE: FAKE_KEY},
                        runtime_root=root / f"runtime-{index}",
                        approval_root=approval_root,
                    )
                except PermissionError:
                    outcome = "blocked"
                else:
                    outcome = "completed"
                with lock:
                    outcomes.append(outcome)

            threads = [threading.Thread(target=worker, args=(index,)) for index in range(2)]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join(timeout=10)
            self.assertEqual(sorted(outcomes), ["blocked", "completed"])
            self.assertEqual(len(client.calls), 2)
            self.assertEqual(len(list(approval_root.iterdir())), 1)

    def test_cli_surface_has_only_execution_and_validation_modes(self) -> None:
        script = (ROOT / "scripts/run_luna_live_canary.py").read_text(encoding="utf-8")
        self.assertIn('"--execute-live-v2"', script)
        self.assertNotIn('"--execute-live-v1"', script)
        self.assertNotIn('"--execute-live"', script)
        self.assertIn('"--validate-directory"', script)
        self.assertNotIn("key-file", script)
        self.assertNotIn("api-key", script)
        schema = json.loads((ROOT / "contracts/x.profile.bio_semantic.live_canary.result.v1.schema.json").read_text())
        self.assertEqual(schema["properties"]["schema_version"]["const"], canary.RESULT_SCHEMA_VERSION)
        self.assertIs(schema["properties"]["authority"]["properties"]["outreach_authorized"]["const"], False)
        self.assertEqual(len(schema["properties"]["catalog"]["oneOf"]), 6)
        self.assertEqual(len(schema["allOf"]), 2)


if __name__ == "__main__":
    with contextlib.redirect_stdout(io.StringIO()):
        unittest.main()
