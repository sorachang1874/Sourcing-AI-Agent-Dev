from __future__ import annotations

import copy
import hashlib
import json
import re
import tempfile
import threading
import unittest
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]

from x_first import luna_live_canary_v2 as canary  # noqa: E402
from x_first import profile_bio_semantic_v2 as semantic  # noqa: E402


class MiniSchemaError(AssertionError):
    """No-dependency Draft 2020-12 subset used by this project's contract tests."""


def _schema_pointer(root: dict[str, Any], pointer: str) -> Any:
    current: Any = root
    for part in pointer.removeprefix("#/").split("/"):
        current = current[part.replace("~1", "/").replace("~0", "~")]
    return current


def _mini_schema_validate(
    instance: Any,
    schema: dict[str, Any],
    *,
    root: dict[str, Any] | None = None,
    externals: dict[str, dict[str, Any]] | None = None,
    path: str = "$",
) -> None:
    """Execute the JSON-Schema keywords used by the Luna v2 contracts."""

    root = schema if root is None else root
    externals = {} if externals is None else externals
    reference = schema.get("$ref")
    if isinstance(reference, str):
        if reference.startswith("#/"):
            _mini_schema_validate(
                instance,
                _schema_pointer(root, reference),
                root=root,
                externals=externals,
                path=path,
            )
        else:
            schema_id, separator, pointer = reference.partition("#")
            external = externals.get(schema_id)
            if external is None:
                raise MiniSchemaError(f"{path}: unresolved ref {reference}")
            target = _schema_pointer(external, f"#{pointer}") if separator else external
            _mini_schema_validate(instance, target, root=external, externals=externals, path=path)

    for child in schema.get("allOf", []):
        _mini_schema_validate(instance, child, root=root, externals=externals, path=path)
    if "anyOf" in schema:
        if not any(
            _mini_schema_matches(instance, child, root=root, externals=externals, path=path)
            for child in schema["anyOf"]
        ):
            raise MiniSchemaError(f"{path}: anyOf mismatch")
    if "oneOf" in schema:
        matches = sum(
            _mini_schema_matches(instance, child, root=root, externals=externals, path=path)
            for child in schema["oneOf"]
        )
        if matches != 1:
            raise MiniSchemaError(f"{path}: oneOf matched {matches}")
    if "not" in schema and _mini_schema_matches(
        instance,
        schema["not"],
        root=root,
        externals=externals,
        path=path,
    ):
        raise MiniSchemaError(f"{path}: not matched")
    if "if" in schema:
        branch = (
            "then"
            if _mini_schema_matches(instance, schema["if"], root=root, externals=externals, path=path)
            else "else"
        )
        if branch in schema:
            _mini_schema_validate(instance, schema[branch], root=root, externals=externals, path=path)

    expected_type = schema.get("type")
    expected_types = [expected_type] if isinstance(expected_type, str) else expected_type
    if expected_types is not None:
        matches_type = any(
            (name == "object" and isinstance(instance, dict))
            or (name == "array" and isinstance(instance, list))
            or (name == "string" and isinstance(instance, str))
            or (name == "integer" and type(instance) is int)
            or (name == "number" and type(instance) in {int, float})
            or (name == "boolean" and type(instance) is bool)
            or (name == "null" and instance is None)
            for name in expected_types
        )
        if not matches_type:
            raise MiniSchemaError(f"{path}: wrong type")
    if "const" in schema and not semantic.json_type_strict_equal(instance, schema["const"]):
        raise MiniSchemaError(f"{path}: const mismatch")
    if "enum" in schema and not any(
        semantic.json_type_strict_equal(instance, choice) for choice in schema["enum"]
    ):
        raise MiniSchemaError(f"{path}: enum mismatch")
    if isinstance(instance, str):
        if "pattern" in schema and re.search(schema["pattern"], instance) is None:
            raise MiniSchemaError(f"{path}: pattern mismatch")
        if len(instance) < schema.get("minLength", 0) or len(instance) > schema.get("maxLength", len(instance)):
            raise MiniSchemaError(f"{path}: string length")
    if type(instance) in {int, float}:
        if instance < schema.get("minimum", instance) or instance > schema.get("maximum", instance):
            raise MiniSchemaError(f"{path}: numeric bound")
    if isinstance(instance, dict):
        if any(key not in instance for key in schema.get("required", [])):
            raise MiniSchemaError(f"{path}: required key missing")
        properties = schema.get("properties", {})
        if schema.get("additionalProperties") is False and set(instance) - set(properties):
            raise MiniSchemaError(f"{path}: additional property")
        for key, value in instance.items():
            if key in properties:
                _mini_schema_validate(
                    value,
                    properties[key],
                    root=root,
                    externals=externals,
                    path=f"{path}.{key}",
                )
    if isinstance(instance, list):
        if len(instance) < schema.get("minItems", 0) or len(instance) > schema.get("maxItems", len(instance)):
            raise MiniSchemaError(f"{path}: item count")
        if schema.get("uniqueItems") and len({semantic.canonical_json(value) for value in instance}) != len(instance):
            raise MiniSchemaError(f"{path}: duplicate item")
        prefix = schema.get("prefixItems", [])
        for index, child_schema in enumerate(prefix):
            if index < len(instance):
                _mini_schema_validate(
                    instance[index],
                    child_schema,
                    root=root,
                    externals=externals,
                    path=f"{path}[{index}]",
                )
        items = schema.get("items")
        start = len(prefix) if prefix else 0
        if items is False and len(instance) > start:
            raise MiniSchemaError(f"{path}: extra items")
        if isinstance(items, dict):
            for index in range(start, len(instance)):
                _mini_schema_validate(
                    instance[index],
                    items,
                    root=root,
                    externals=externals,
                    path=f"{path}[{index}]",
                )


def _mini_schema_matches(
    instance: Any,
    schema: dict[str, Any],
    *,
    root: dict[str, Any],
    externals: dict[str, dict[str, Any]],
    path: str,
) -> bool:
    try:
        _mini_schema_validate(instance, schema, root=root, externals=externals, path=path)
    except MiniSchemaError:
        return False
    return True


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
    model_output = semantic.load_json(ROOT / "fixtures/profile_bio_semantic_model_output_v2.json")
    model_output["request_id"] = request["request_id"]
    return _json_response(
        {
            "id": "resp_fixture_luna_canary_v2",
            "object": "response",
            "status": "completed",
            "model": model,
            "output": [
                {
                    "id": "msg_fixture_luna_canary_v2",
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
    # Deliberately misleading metadata: v2 must ignore it and use observed
    # attempt receipts rather than infer execution from ``is_live``.
    is_live = False

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


class FalseyFakeHttpClient(FakeHttpClient):
    def __bool__(self) -> bool:
        return False


class LunaLiveCanaryV2Test(unittest.TestCase):
    def _run(
        self,
        root: Path,
        responses: list[canary.HttpResponse | Exception],
    ) -> tuple[dict[str, Any], Path, Path, FakeHttpClient]:
        client = FakeHttpClient(responses)
        approval_root = root / "approval-v2"
        result, bundle = canary.run_luna_live_canary_v2_fixture(
            http_client=client,
            runtime_root=root / "runtime-v2",
            approval_root=approval_root,
        )
        return result, bundle, approval_root, client

    def test_v2_binds_only_current_semantic_v22_assets(self) -> None:
        request = canary.build_synthetic_live_request()
        self.assertEqual(request["schema_version"], "x.profile.bio_semantic.request.v2.2")
        self.assertEqual(request["model_execution_mode"], "live_canary")
        self.assertEqual(semantic.REVIEW_SCHEMA_VERSION, "x.profile.bio_semantic.review.v2.2")
        self.assertEqual(
            request["model_policy"]["professional_experience_proxy_policy_sha256"],
            semantic.CANONICAL_PROXY_POLICY_SHA256,
        )
        self.assertEqual(semantic.validate_pure_adjudication_implementation(), [])

    def test_no_flag_or_key_makes_zero_calls_and_zero_writes(self) -> None:
        with self.assertRaises(PermissionError):
            canary.run_luna_live_canary_v2(execute_live=False)
        with mock.patch.dict(canary.os.environ, {}, clear=True), self.assertRaises(PermissionError):
            canary.run_luna_live_canary_v2(execute_live=True)
        with self.assertRaises(TypeError):
            canary.run_luna_live_canary_v2(execute_live=True, http_client=FakeHttpClient([]))  # type: ignore[call-arg]
        with tempfile.TemporaryDirectory() as directory, self.assertRaises(PermissionError):
            root = Path(directory)
            canary.run_luna_live_canary_v2_fixture(
                http_client=canary.UrllibHttpClient(),
                runtime_root=root / "runtime",
                approval_root=root / "approval",
            )

    def test_fixture_falsey_client_never_falls_back_to_production_transport(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            client = FalseyFakeHttpClient([_catalog(canary.MODEL_ID), _provider_response()])
            result, _ = canary.run_luna_live_canary_v2_fixture(
                http_client=client,
                runtime_root=root / "runtime",
                approval_root=root / "approval",
            )
            self.assertEqual(result["status"], "completed")
            self.assertEqual(len(client.calls), 2)
            self.assertEqual(result["evidence_provenance"]["execution_origin"], canary.FIXTURE_EXECUTION_ORIGIN)

    def test_internal_runner_cannot_attach_production_provenance_to_injected_dependencies(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            client = FakeHttpClient([_catalog(canary.MODEL_ID), _provider_response()])
            with self.assertRaisesRegex(PermissionError, "production_runner_dependencies_not_injectable"):
                canary._run_luna_live_canary_v2(
                    lane=canary._PRODUCTION_LANE,
                    key="sk-" + "f" * 32,
                    http_client=client,
                    runtime_root=root / "runtime",
                    approval_root=root / "approval",
                    wall_clock=lambda: datetime.now(UTC),
                    monotonic=canary.time.monotonic,
                )
            self.assertEqual(client.calls, [])
            self.assertFalse((root / "runtime").exists())
            self.assertFalse((root / "approval").exists())

    def test_success_persists_exact_observed_attempts_and_replays_offline(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            provider_response = _provider_response()
            result, bundle, approval_root, client = self._run(
                root,
                [_catalog("other-model", canary.MODEL_ID), provider_response],
            )
            self.assertEqual(result["status"], "completed")
            self.assertEqual(result["error_codes"], [])
            self.assertEqual(result["semantic"]["review_schema_version"], semantic.REVIEW_SCHEMA_VERSION)
            self.assertEqual(result["calls"]["total_external_calls"], 2)
            self.assertEqual(result["calls"]["semantic_model_calls"], 1)
            self.assertEqual(
                result["evidence_provenance"],
                {
                    "execution_origin": canary.FIXTURE_EXECUTION_ORIGIN,
                    "production_transport_observed": False,
                    "formal_live_evidence_eligible": False,
                    "caller_injection_used": True,
                },
            )
            execution = json.loads((bundle / "execution-receipt.json").read_text())
            self.assertEqual(execution["execution_origin"], canary.FIXTURE_EXECUTION_ORIGIN)
            self.assertEqual(execution["attempt_count"], 2)
            self.assertEqual([attempt["sequence"] for attempt in execution["attempts"]], [1, 2])
            self.assertEqual(
                [attempt["operation"] for attempt in execution["attempts"]],
                ["model_catalog", "semantic_response"],
            )
            self.assertTrue(all(attempt["retry_used"] is False for attempt in execution["attempts"]))
            self.assertTrue(all(attempt["fallback_used"] is False for attempt in execution["attempts"]))
            post_body = client.calls[1]["body"]
            self.assertEqual(execution["request_payload_sha256"], hashlib.sha256(post_body).hexdigest())
            self.assertEqual(
                execution["attempts"][1]["response_body_sha256"],
                hashlib.sha256(provider_response.body).hexdigest(),
            )
            self.assertEqual(canary.validate_result_contract_v2(result), [])
            self.assertEqual(
                canary.validate_execution_receipt_contract_v2(
                    execution,
                    run_id=result["run"]["run_id"],
                    execution_origin=canary.FIXTURE_EXECUTION_ORIGIN,
                    request_payload_sha256=execution["request_payload_sha256"],
                ),
                [],
            )
            approval = json.loads((bundle / "approval-receipt.json").read_text())
            self.assertEqual(
                canary.validate_approval_receipt_contract_v2(
                    approval,
                    request=json.loads((bundle / "request.json").read_text()),
                    result=result,
                ),
                [],
            )
            mutated_approval = copy.deepcopy(approval)
            mutated_approval["execution_origin"] = canary.PRODUCTION_EXECUTION_ORIGIN
            self.assertNotEqual(
                canary.validate_approval_receipt_contract_v2(
                    mutated_approval,
                    request=json.loads((bundle / "request.json").read_text()),
                    result=result,
                ),
                [],
            )
            review = json.loads((bundle / "semantic-review.json").read_text())
            self.assertEqual(review["execution"]["mode"], "live")
            self.assertEqual(review["execution"]["provider_external_calls"], 1)
            self.assertEqual(canary.validate_artifact_directory_v2_fixture(bundle, approval_root=approval_root), [])
            self.assertNotEqual(canary.validate_artifact_directory_v2(bundle), [])
            self.assertEqual(bundle.stat().st_mode & 0o777, 0o700)
            self.assertTrue(all(path.stat().st_mode & 0o777 == 0o600 for path in bundle.iterdir()))

    def test_response_transport_failure_is_one_observed_attempt_and_replayable_semantic_failure(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            result, bundle, approval_root, client = self._run(
                root,
                [_catalog(canary.MODEL_ID), RuntimeError("must not escape")],
            )
            self.assertEqual(len(client.calls), 2)
            self.assertEqual(result["error_codes"], ["response_transport_failed"])
            self.assertEqual(canary.validate_result_contract_v2(result), [])
            execution = json.loads((bundle / "execution-receipt.json").read_text())
            response_attempt = execution["attempts"][1]
            self.assertEqual(response_attempt["outcome"], "transport_failure")
            self.assertIsNone(response_attempt["http_status"])
            self.assertIsNone(response_attempt["response_body_sha256"])
            self.assertFalse((bundle / "raw-response.json").exists())
            review = json.loads((bundle / "semantic-review.json").read_text())
            self.assertEqual(review["error_codes"], ["transport_failed"])
            self.assertEqual(review["execution"]["provider_external_calls"], 1)
            self.assertEqual(canary.validate_artifact_directory_v2_fixture(bundle, approval_root=approval_root), [])

    def test_catalog_failure_stops_before_post_and_keeps_receipt(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            result, bundle, approval_root, client = self._run(root, [_catalog("not-luna")])
            self.assertEqual(result["error_codes"], ["catalog_model_missing"])
            self.assertEqual(canary.validate_result_contract_v2(result), [])
            self.assertEqual(len(client.calls), 1)
            execution = json.loads((bundle / "execution-receipt.json").read_text())
            self.assertEqual(execution["attempt_count"], 1)
            self.assertFalse((bundle / "semantic-review.json").exists())
            self.assertEqual(canary.validate_artifact_directory_v2_fixture(bundle, approval_root=approval_root), [])

    def test_catalog_requires_exact_json_media_type_before_model_handshake(self) -> None:
        for content_type in ("application/jsonp", "text/application/json", "application/json; charset=latin1"):
            with self.subTest(content_type=content_type), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                response = canary.HttpResponse(
                    status_code=200,
                    headers={"Content-Type": content_type},
                    body=_catalog(canary.MODEL_ID).body,
                )
                result, bundle, approval_root, client = self._run(root, [response])
                self.assertEqual(result["error_codes"], ["catalog_invalid"])
                self.assertEqual(len(client.calls), 1)
                execution = json.loads((bundle / "execution-receipt.json").read_text())
                self.assertEqual(execution["attempts"][0]["response_content_type"], content_type)
                self.assertEqual(canary.validate_artifact_directory_v2_fixture(bundle, approval_root=approval_root), [])

    def test_strict_content_type_and_returned_model_normalization_fail_closed(self) -> None:
        cases = (
            (
                canary.HttpResponse(
                    200,
                    {"Content-Type": "text/plain"},
                    _provider_response().body,
                ),
                "response_content_type_invalid",
            ),
            (
                _json_response(
                    {
                        "id": "resp_invalid_model_type",
                        "object": "response",
                        "status": "completed",
                        "model": {"not": "a model id"},
                        "output": [],
                        "usage": {"input_tokens": 0, "output_tokens": 0, "total_tokens": 0},
                    }
                ),
                "response_model_mismatch",
            ),
            (_provider_response(model="x" * 161), "response_model_mismatch"),
            (
                _json_response(
                    {
                        "model": canary.MODEL_ID,
                        "echo": "sk-" + "a" * 32,
                    }
                ),
                "response_secret_detected",
            ),
        )
        for response, expected_error in cases:
            with self.subTest(expected_error=expected_error), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                result, bundle, approval_root, _ = self._run(
                    root,
                    [_catalog(canary.MODEL_ID), response],
                )
                self.assertEqual(result["error_codes"], [expected_error])
                self.assertEqual(canary.validate_result_contract_v2(result), [])
                self.assertIsNone(result["identity"]["returned_model"])
                self.assertEqual(
                    canary.validate_artifact_directory_v2_fixture(bundle, approval_root=approval_root),
                    [],
                )

    def test_expired_pending_bundle_is_invalid_then_atomically_purged_to_tombstone(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            observed = datetime(2026, 7, 14, 0, 0, tzinfo=UTC)
            client = FakeHttpClient([_catalog(canary.MODEL_ID), _provider_response()])
            approval_root = root / "approval"
            result, bundle = canary.run_luna_live_canary_v2_fixture(
                http_client=client,
                runtime_root=root / "runtime",
                approval_root=approval_root,
                wall_clock=lambda: observed,
            )
            before_expiry = observed + timedelta(hours=1)
            after_expiry = observed + timedelta(hours=25)
            self.assertEqual(
                canary.validate_artifact_directory_v2_fixture(
                    bundle,
                    approval_root=approval_root,
                    now=before_expiry,
                ),
                [],
            )
            self.assertNotEqual(
                canary.validate_artifact_directory_v2_fixture(
                    bundle,
                    approval_root=approval_root,
                    now=after_expiry,
                ),
                [],
            )
            deletion_root = root / "deletions"
            receipt, receipt_path = canary.purge_expired_artifact_directory_v2_fixture(
                bundle,
                approval_root=approval_root,
                deletion_root=deletion_root,
                now=after_expiry,
            )
            self.assertFalse(bundle.exists())
            self.assertEqual(receipt["state"], "deleted")
            self.assertEqual(receipt["result_sha256"], canary.legacy._canonical_sha256(result))
            self.assertEqual(receipt_path.stat().st_mode & 0o777, 0o600)
            self.assertEqual(deletion_root.stat().st_mode & 0o777, 0o700)
            self.assertEqual(
                canary.validate_deletion_receipt_v2_fixture(
                    receipt_path,
                    runtime_root=bundle.parent,
                    approval_root=approval_root,
                    deletion_root=deletion_root,
                ),
                [],
            )
            self.assertNotEqual(canary.validate_deletion_receipt_v2(receipt_path), [])
            replayed, replayed_path = canary.purge_expired_artifact_directory_v2_fixture(
                bundle,
                approval_root=approval_root,
                deletion_root=deletion_root,
                now=after_expiry + timedelta(minutes=1),
            )
            self.assertEqual(replayed, receipt)
            self.assertEqual(replayed_path, receipt_path)

    def test_ttl_purge_recovers_after_each_durable_transition(self) -> None:
        failure_points = ("before_rename", "before_remove", "before_final_receipt")
        for failure_point in failure_points:
            with self.subTest(failure_point=failure_point), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                observed = datetime(2026, 7, 14, 0, 0, tzinfo=UTC)
                after_expiry = observed + timedelta(hours=25)
                approval_root = root / "approval"
                _, bundle = canary.run_luna_live_canary_v2_fixture(
                    http_client=FakeHttpClient([_catalog(canary.MODEL_ID), _provider_response()]),
                    runtime_root=root / "runtime",
                    approval_root=approval_root,
                    wall_clock=lambda: observed,
                )
                deletion_root = root / "deletions"
                if failure_point == "before_rename":
                    patcher = mock.patch.object(canary.os, "replace", side_effect=RuntimeError("simulated crash"))
                elif failure_point == "before_remove":
                    patcher = mock.patch.object(canary.shutil, "rmtree", side_effect=RuntimeError("simulated crash"))
                else:
                    create = canary._create_private_json_exclusive

                    def fail_final(path: Path, payload: dict[str, Any]) -> None:
                        if path.name.endswith(".deletion.json"):
                            raise RuntimeError("simulated crash")
                        create(path, payload)

                    patcher = mock.patch.object(canary, "_create_private_json_exclusive", side_effect=fail_final)
                with patcher, self.assertRaises(RuntimeError):
                    canary.purge_expired_artifact_directory_v2_fixture(
                        bundle,
                        approval_root=approval_root,
                        deletion_root=deletion_root,
                        now=after_expiry,
                    )
                journal_path = deletion_root / f"{bundle.name}.deletion.pending.json"
                self.assertTrue(journal_path.exists())
                self.assertNotEqual(canary.validate_deletion_receipt_v2(journal_path), [])
                invalid_final_root = root / "invalid-final-receipt"
                canary.legacy._ensure_private_directory(invalid_final_root)
                invalid_final = invalid_final_root / f"{bundle.name}.deletion.json"
                canary._create_private_json_exclusive(
                    invalid_final,
                    json.loads(journal_path.read_text()),
                )
                self.assertNotEqual(canary.validate_deletion_receipt_v2(invalid_final), [])
                receipt, receipt_path = canary.purge_expired_artifact_directory_v2_fixture(
                    bundle,
                    approval_root=approval_root,
                    deletion_root=deletion_root,
                    now=after_expiry + timedelta(minutes=1),
                )
                self.assertEqual(receipt["state"], "deleted")
                self.assertFalse(bundle.exists())
                self.assertFalse((bundle.parent / f".{bundle.name}.deleting").exists())
                self.assertFalse(journal_path.exists())
                self.assertEqual(
                    canary.validate_deletion_receipt_v2_fixture(
                        receipt_path,
                        runtime_root=bundle.parent,
                        approval_root=approval_root,
                        deletion_root=deletion_root,
                    ),
                    [],
                )

    def test_deletion_tombstone_requires_owner_roots_ledger_and_absent_bundle(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            observed = datetime(2026, 7, 14, 0, 0, tzinfo=UTC)
            after_expiry = observed + timedelta(hours=25)
            approval_root = root / "approval"
            _, bundle = canary.run_luna_live_canary_v2_fixture(
                http_client=FakeHttpClient([_catalog(canary.MODEL_ID), _provider_response()]),
                runtime_root=root / "runtime",
                approval_root=approval_root,
                wall_clock=lambda: observed,
            )
            deletion_root = root / "deletions"
            receipt, receipt_path = canary.purge_expired_artifact_directory_v2_fixture(
                bundle,
                approval_root=approval_root,
                deletion_root=deletion_root,
                now=after_expiry,
            )
            self.assertEqual(
                canary.validate_deletion_receipt_v2_fixture(
                    receipt_path,
                    runtime_root=bundle.parent,
                    approval_root=approval_root,
                    deletion_root=deletion_root,
                ),
                [],
            )

            arbitrary_root = root / "arbitrary"
            canary.legacy._ensure_private_directory(arbitrary_root)
            forged_path = arbitrary_root / f"{receipt['run_id']}.deletion.json"
            forged = copy.deepcopy(receipt)
            for field in (
                "bundle_digest_sha256",
                "result_sha256",
                "approval_receipt_sha256",
                "execution_receipt_sha256",
            ):
                forged[field] = "0" * 64
            canary._create_private_json_exclusive(forged_path, forged)
            self.assertNotEqual(canary.validate_deletion_receipt_v2(forged_path), [])
            self.assertNotEqual(
                canary.validate_deletion_receipt_v2_fixture(
                    forged_path,
                    runtime_root=bundle.parent,
                    approval_root=approval_root,
                    deletion_root=arbitrary_root,
                ),
                [],
            )

            restored_bundle = bundle.parent / bundle.name
            restored_bundle.mkdir(mode=0o700)
            self.assertNotEqual(
                canary.validate_deletion_receipt_v2_fixture(
                    receipt_path,
                    runtime_root=bundle.parent,
                    approval_root=approval_root,
                    deletion_root=deletion_root,
                ),
                [],
            )

    def test_coherently_rehashed_execution_tamper_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            result, bundle, approval_root, _ = self._run(
                root,
                [_catalog(canary.MODEL_ID), _provider_response()],
            )
            execution_path = bundle / "execution-receipt.json"
            result_path = bundle / "result.json"
            execution = json.loads(execution_path.read_text())
            execution["attempts"][1]["requested_model"] = "gpt-5.6-luna-shadow"
            changed = copy.deepcopy(result)
            changed["execution_receipt_sha256"] = canary.legacy._canonical_sha256(execution)
            changed["artifact_sha256s"]["execution-receipt.json"] = canary.legacy._canonical_sha256(execution)
            canary.legacy._atomic_write_json(execution_path, execution)
            canary.legacy._atomic_write_json(result_path, changed)
            self.assertNotEqual(canary.validate_artifact_directory_v2_fixture(bundle, approval_root=approval_root), [])

    def test_project_strict_schema_helpers_reject_result_and_attempt_combination_mutations(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            result, bundle, _, _ = self._run(root, [_catalog(canary.MODEL_ID), _provider_response()])
            execution = json.loads((bundle / "execution-receipt.json").read_text())
            result_mutations = []
            changed = copy.deepcopy(result)
            changed["calls"]["total_external_calls"] = True
            result_mutations.append(changed)
            changed = copy.deepcopy(result)
            changed["catalog"]["unexpected"] = "open schema"
            result_mutations.append(changed)
            changed = copy.deepcopy(result)
            changed["artifact_inventory"] = changed["artifact_inventory"][:-1]
            result_mutations.append(changed)
            changed = copy.deepcopy(result)
            changed["evidence_provenance"]["formal_live_evidence_eligible"] = True
            result_mutations.append(changed)
            for mutation in result_mutations:
                self.assertNotEqual(canary.validate_result_contract_v2(mutation), [])

            execution_mutations = []
            changed_execution = copy.deepcopy(execution)
            changed_execution["attempt_count"] = 1
            execution_mutations.append(changed_execution)
            changed_execution = copy.deepcopy(execution)
            changed_execution["attempts"][1]["started_at"] = changed_execution["attempts"][0]["started_at"]
            changed_execution["attempts"][0]["completed_at"] = "2026-07-14T23:59:59.999Z"
            execution_mutations.append(changed_execution)
            changed_execution = copy.deepcopy(execution)
            changed_execution["attempts"][1]["outcome"] = "transport_failure"
            execution_mutations.append(changed_execution)
            for mutation in execution_mutations:
                self.assertNotEqual(
                    canary.validate_execution_receipt_contract_v2(
                        mutation,
                        run_id=result["run"]["run_id"],
                        execution_origin=canary.FIXTURE_EXECUTION_ORIGIN,
                        request_payload_sha256=execution["request_payload_sha256"],
                    ),
                    [],
                )

    def test_global_v2_approval_has_one_concurrent_winner(self) -> None:
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
                    canary.run_luna_live_canary_v2_fixture(
                        http_client=client,
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

    def test_v2_contract_schemas_bind_result_execution_and_zero_authority(self) -> None:
        result_schema = json.loads(
            (ROOT / "contracts/x.profile.bio_semantic.live_canary.result.v2.schema.json").read_text()
        )
        execution_schema = json.loads(
            (ROOT / "contracts/x.profile.bio_semantic.live_canary.execution_receipt.v2.schema.json").read_text()
        )
        approval_schema = json.loads(
            (ROOT / "contracts/x.profile.bio_semantic.live_canary.approval_receipt.v2.schema.json").read_text()
        )
        deletion_schema = json.loads(
            (ROOT / "contracts/x.profile.bio_semantic.live_canary.deletion_receipt.v2.schema.json").read_text()
        )
        deletion_journal_schema = json.loads(
            (ROOT / "contracts/x.profile.bio_semantic.live_canary.deletion_journal.v2.schema.json").read_text()
        )
        self.assertEqual(
            result_schema["properties"]["schema_version"]["const"],
            canary.RESULT_SCHEMA_VERSION,
        )
        self.assertEqual(
            execution_schema["properties"]["schema_version"]["const"],
            canary.EXECUTION_RECEIPT_SCHEMA_VERSION,
        )
        self.assertIs(
            result_schema["properties"]["authority"]["properties"]["outreach_authorized"]["const"],
            False,
        )
        self.assertIs(
            execution_schema["properties"]["retry_allowed"]["const"],
            False,
        )
        self.assertIs(
            execution_schema["properties"]["fallback_allowed"]["const"],
            False,
        )
        self.assertEqual(
            approval_schema["properties"]["schema_version"]["const"],
            canary.APPROVAL_RECEIPT_SCHEMA_VERSION,
        )
        self.assertEqual(
            approval_schema["properties"]["semantic_adjudication_implementation_sha256"]["const"],
            semantic.PURE_ADJUDICATION_IMPLEMENTATION_SHA256,
        )
        self.assertEqual(
            execution_schema["properties"]["semantic_adjudication_implementation_sha256"]["const"],
            semantic.PURE_ADJUDICATION_IMPLEMENTATION_SHA256,
        )
        self.assertEqual(
            result_schema["properties"]["semantic_contract"]["properties"]
            ["pure_adjudication_implementation_sha256"]["const"],
            semantic.PURE_ADJUDICATION_IMPLEMENTATION_SHA256,
        )
        self.assertEqual(deletion_schema["properties"]["state"]["const"], "deleted")
        self.assertEqual(
            deletion_journal_schema["properties"]["schema_version"]["const"],
            canary.DELETION_JOURNAL_SCHEMA_VERSION,
        )

    def test_generated_success_failure_journal_and_tombstone_match_declared_schemas(self) -> None:
        schemas = {
            name: json.loads((ROOT / "contracts" / name).read_text())
            for name in (
                "x.profile.bio_semantic.live_canary.result.v2.schema.json",
                "x.profile.bio_semantic.live_canary.execution_receipt.v2.schema.json",
                "x.profile.bio_semantic.live_canary.approval_receipt.v2.schema.json",
                "x.profile.bio_semantic.live_canary.deletion_journal.v2.schema.json",
                "x.profile.bio_semantic.live_canary.deletion_receipt.v2.schema.json",
            )
        }
        external_schemas = {
            "x.profile.bio_semantic.live_canary.result.v1.schema.json": json.loads(
                (
                    ROOT
                    / "contracts/x.profile.bio_semantic.live_canary.result.v1.schema.json"
                ).read_text()
            )
        }
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            success, success_bundle, _, _ = self._run(
                root / "success",
                [_catalog(canary.MODEL_ID), _provider_response()],
            )
            _mini_schema_validate(
                success,
                schemas["x.profile.bio_semantic.live_canary.result.v2.schema.json"],
                externals=external_schemas,
            )
            _mini_schema_validate(
                json.loads((success_bundle / "execution-receipt.json").read_text()),
                schemas["x.profile.bio_semantic.live_canary.execution_receipt.v2.schema.json"],
            )
            _mini_schema_validate(
                json.loads((success_bundle / "approval-receipt.json").read_text()),
                schemas["x.profile.bio_semantic.live_canary.approval_receipt.v2.schema.json"],
            )

            failure, failure_bundle, _, _ = self._run(
                root / "failure",
                [_catalog("not-luna")],
            )
            self.assertEqual(failure["status"], "failed")
            _mini_schema_validate(
                failure,
                schemas["x.profile.bio_semantic.live_canary.result.v2.schema.json"],
                externals=external_schemas,
            )
            _mini_schema_validate(
                json.loads((failure_bundle / "execution-receipt.json").read_text()),
                schemas["x.profile.bio_semantic.live_canary.execution_receipt.v2.schema.json"],
            )

            deletion_case = root / "deletion"
            observed = datetime(2026, 7, 14, 0, 0, tzinfo=UTC)
            after_expiry = observed + timedelta(hours=25)
            approval_root = deletion_case / "approval"
            _, bundle = canary.run_luna_live_canary_v2_fixture(
                http_client=FakeHttpClient([_catalog(canary.MODEL_ID), _provider_response()]),
                runtime_root=deletion_case / "runtime",
                approval_root=approval_root,
                wall_clock=lambda: observed,
            )
            deletion_root = deletion_case / "deletions"
            with mock.patch.object(canary.os, "replace", side_effect=RuntimeError("schema fixture crash")):
                with self.assertRaises(RuntimeError):
                    canary.purge_expired_artifact_directory_v2_fixture(
                        bundle,
                        approval_root=approval_root,
                        deletion_root=deletion_root,
                        now=after_expiry,
                    )
            journal_path = deletion_root / f"{bundle.name}.deletion.pending.json"
            _mini_schema_validate(
                json.loads(journal_path.read_text()),
                schemas["x.profile.bio_semantic.live_canary.deletion_journal.v2.schema.json"],
            )
            receipt, receipt_path = canary.purge_expired_artifact_directory_v2_fixture(
                bundle,
                approval_root=approval_root,
                deletion_root=deletion_root,
                now=after_expiry + timedelta(minutes=1),
            )
            _mini_schema_validate(
                receipt,
                schemas["x.profile.bio_semantic.live_canary.deletion_receipt.v2.schema.json"],
            )
            zero_digest_receipt = copy.deepcopy(receipt)
            zero_digest_receipt["bundle_digest_sha256"] = "0" * 64
            with self.assertRaises(MiniSchemaError):
                _mini_schema_validate(
                    zero_digest_receipt,
                    schemas["x.profile.bio_semantic.live_canary.deletion_receipt.v2.schema.json"],
                )
            self.assertEqual(receipt, json.loads(receipt_path.read_text()))


if __name__ == "__main__":
    unittest.main()
