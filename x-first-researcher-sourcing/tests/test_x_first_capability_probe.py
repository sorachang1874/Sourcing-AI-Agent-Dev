from __future__ import annotations

import ast
import contextlib
import copy
import io
import json
import os
import re
import subprocess
import sys
import tempfile
import threading
import time
import tomllib
import unittest
from collections.abc import Callable, Iterable
from pathlib import Path
from typing import Any
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "scripts"))

import generate_capability_probe_fixtures as capability_fixture_generator  # noqa: E402
from generate_capability_probe_fixtures import (  # noqa: E402
    OWNED_TEMP_PREFIX,
    _atomic_write_many,
    _exclusive_pair_lock,
    _write_same_directory_temp,
    build_request_fixture,
    build_result_fixture,
)

from x_first.capability_probe import (  # noqa: E402
    CANONICAL_RFC3339_UTC_MILLIS_PATTERN,
    CAPABILITY_OBSERVATION_EXCERPTS,
    DIAGNOSTIC_CODES,
    ERROR_ENVELOPE_BY_VERDICT,
    MAX_CAPABILITY_OBSERVATIONS,
    REQUEST_JSON_LOAD_DIAGNOSTIC,
    REQUEST_VALIDATION_DIAGNOSTIC,
    RESULT_JSON_LOAD_DIAGNOSTIC,
    RESULT_VALIDATION_DIAGNOSTIC,
    SYNTHETIC_RAW_RESPONSE_SHA256,
    _validate_cli_payloads,
    canonical_sha256,
    validate_capability_request,
    validate_capability_result,
)
from x_first.contracts import load_json  # noqa: E402

LEGACY_PAIR_LOCK_FILENAME = ".x-first-capability-fixture.pair.lock"


def _walk(value: Any) -> Iterable[Any]:
    yield value
    if isinstance(value, dict):
        for child in value.values():
            yield from _walk(child)
    elif isinstance(value, list):
        for child in value:
            yield from _walk(child)


class XFirstCapabilityProbeFixtureTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.request = build_request_fixture()
        cls.result = build_result_fixture(cls.request)
        cls.request_schema = load_json(ROOT / "contracts/x.grok.capability_probe.request.v1.schema.json")
        cls.result_schema = load_json(ROOT / "contracts/x.grok.capability_probe.result.v1.schema.json")

    def assert_request_rejected(
        self,
        mutation: Callable[[dict[str, Any]], None],
        *,
        contains: str,
    ) -> None:
        request = copy.deepcopy(self.request)
        mutation(request)
        errors = validate_capability_request(request)
        self.assertTrue(any(contains in error for error in errors), errors)

    def assert_result_rejected(
        self,
        mutation: Callable[[dict[str, Any]], None],
        *,
        contains: str,
    ) -> None:
        result = copy.deepcopy(self.result)
        mutation(result)
        errors = validate_capability_result(result, request=self.request)
        self.assertTrue(any(contains in error for error in errors), errors)

    def build_failure_result(self, *, verdict: str = "capability_unavailable") -> dict[str, Any]:
        result = copy.deepcopy(self.result)
        result["run"]["status"] = "failed"
        result["task"].update({"status": "failed", "stop_reason": "fixture_failed"})
        result["capability"]["verdict"] = verdict
        result["observations"] = []
        result["usage"]["observations"] = 0
        code, message, retryable = ERROR_ENVELOPE_BY_VERDICT[verdict]
        result["errors"] = [{"code": code, "message": message, "retryable": retryable}]
        return result

    def test_generated_artifacts_are_current_and_deterministic(self) -> None:
        self.assertEqual(build_request_fixture(), build_request_fixture())
        self.assertEqual(build_result_fixture(self.request), build_result_fixture(self.request))
        self.assertEqual(load_json(ROOT / "fixtures/capability_probe_request_fixture_v1.json"), self.request)
        self.assertEqual(load_json(ROOT / "fixtures/capability_probe_result_fixture_v1.json"), self.result)
        result = subprocess.run(
            [sys.executable, "scripts/generate_capability_probe_fixtures.py", "--check"],
            cwd=ROOT,
            env={**os.environ, "PYTHONPATH": "src"},
            check=False,
            capture_output=True,
            text=True,
            timeout=60,
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_generator_atomic_write_rejects_symlinks_and_rolls_back_pair_failure(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            first = root / "first.json"
            second = root / "second.json"
            first.write_text("old-first", encoding="utf-8")
            second.write_text("old-second", encoding="utf-8")

            _atomic_write_many(((first, "new-first"), (second, "new-second")))
            self.assertEqual(first.read_text(encoding="utf-8"), "new-first")
            self.assertEqual(second.read_text(encoding="utf-8"), "new-second")
            self.assertFalse(list(root.glob(f"{OWNED_TEMP_PREFIX}*.tmp")))
            self.assertFalse((root / LEGACY_PAIR_LOCK_FILENAME).exists())

            stale_owned_temp = _write_same_directory_temp(first, b"hard-crash-orphan")
            unrelated_temp = root / f"{OWNED_TEMP_PREFIX}{first.name}.not-owned.tmp"
            unrelated_temp.write_text("unrelated", encoding="utf-8")
            first.write_text("hard-crash-partial-first", encoding="utf-8")
            reaped = _atomic_write_many(((first, "repaired-first"), (second, "repaired-second")))
            self.assertEqual(reaped, (stale_owned_temp,))
            self.assertFalse(stale_owned_temp.exists())
            self.assertEqual(unrelated_temp.read_text(encoding="utf-8"), "unrelated")
            self.assertEqual(first.read_text(encoding="utf-8"), "repaired-first")
            self.assertEqual(second.read_text(encoding="utf-8"), "repaired-second")

            target = root / "target.json"
            symlink = root / "symlink.json"
            target.write_text("target-original", encoding="utf-8")
            symlink.symlink_to(target)
            with self.assertRaisesRegex(ValueError, "refusing to replace symlink"):
                _atomic_write_many(((symlink, "must-not-write"), (second, "must-not-write")))
            self.assertEqual(target.read_text(encoding="utf-8"), "target-original")
            self.assertEqual(second.read_text(encoding="utf-8"), "repaired-second")

            first.write_text("rollback-first", encoding="utf-8")
            second.write_text("rollback-second", encoding="utf-8")
            real_replace = os.replace
            replace_calls = 0

            def fail_second_replace(source: str | os.PathLike[str], destination: str | os.PathLike[str]) -> None:
                nonlocal replace_calls
                replace_calls += 1
                if replace_calls == 2:
                    raise OSError("synthetic second replace failure")
                real_replace(source, destination)

            with mock.patch("generate_capability_probe_fixtures.os.replace", side_effect=fail_second_replace):
                with self.assertRaisesRegex(OSError, "synthetic second replace failure"):
                    _atomic_write_many(((first, "partial-first"), (second, "partial-second")))
            self.assertEqual(first.read_text(encoding="utf-8"), "rollback-first")
            self.assertEqual(second.read_text(encoding="utf-8"), "rollback-second")
            self.assertFalse(
                [path for path in root.glob(f"{OWNED_TEMP_PREFIX}*.tmp") if path != unrelated_temp]
            )
            self.assertEqual(unrelated_temp.read_text(encoding="utf-8"), "unrelated")

    def test_generator_directory_lock_rejects_symlink_parent_and_serializes_two_writers(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            trusted = root / "trusted"
            trusted.mkdir()
            linked = root / "linked"
            linked.symlink_to(trusted, target_is_directory=True)
            first = linked / "first.json"
            second = linked / "second.json"
            with self.assertRaisesRegex(ValueError, "requires a trusted real directory"):
                _atomic_write_many(((first, "first"), (second, "second")))
            self.assertFalse((trusted / "first.json").exists())
            self.assertFalse((trusted / "second.json").exists())

            untrusted = root / "group-or-other-writable"
            untrusted.mkdir()
            untrusted.chmod(0o777)
            with self.assertRaisesRegex(ValueError, "directory identity changed"):
                _atomic_write_many(
                    ((untrusted / "first.json", "first"), (untrusted / "second.json", "second"))
                )
            self.assertFalse((untrusted / "first.json").exists())
            self.assertFalse((untrusted / "second.json").exists())

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            first = root / "request.json"
            second = root / "result.json"
            first.write_text("initial-request", encoding="utf-8")
            second.write_text("initial-result", encoding="utf-8")

            first_writer_replaced_request = threading.Event()
            release_first_writer = threading.Event()
            second_writer_started = threading.Event()
            second_writer_entered_critical_section = threading.Event()
            writer_errors: list[BaseException] = []
            real_replace = os.replace
            real_reap = capability_fixture_generator._reap_owned_stale_temps

            def interleaved_replace(
                source: str | os.PathLike[str], destination: str | os.PathLike[str]
            ) -> None:
                real_replace(source, destination)
                if threading.current_thread().name == "writer-one" and Path(destination) == first:
                    first_writer_replaced_request.set()
                    if not release_first_writer.wait(timeout=3):
                        raise TimeoutError("test did not release first writer")

            def tracked_reap(paths: list[Path]) -> tuple[Path, ...]:
                if threading.current_thread().name == "writer-two":
                    second_writer_entered_critical_section.set()
                return real_reap(paths)

            def write_pair(name: str, values: tuple[tuple[Path, str], ...]) -> None:
                if name == "writer-two":
                    second_writer_started.set()
                try:
                    _atomic_write_many(values)
                except BaseException as error:
                    writer_errors.append(error)

            with (
                mock.patch("generate_capability_probe_fixtures.os.replace", side_effect=interleaved_replace),
                mock.patch(
                    "generate_capability_probe_fixtures._reap_owned_stale_temps",
                    side_effect=tracked_reap,
                ),
            ):
                writer_one = threading.Thread(
                    target=write_pair,
                    name="writer-one",
                    args=("writer-one", ((first, "writer-one-request"), (second, "writer-one-result"))),
                )
                writer_one.start()
                self.assertTrue(first_writer_replaced_request.wait(timeout=3))
                writer_two = threading.Thread(
                    target=write_pair,
                    name="writer-two",
                    args=("writer-two", ((first, "writer-two-request"), (second, "writer-two-result"))),
                )
                writer_two.start()
                try:
                    self.assertTrue(second_writer_started.wait(timeout=3))
                    entered_before_release = second_writer_entered_critical_section.wait(timeout=0.2)
                finally:
                    release_first_writer.set()
                writer_one.join(timeout=3)
                writer_two.join(timeout=3)

            self.assertFalse(writer_one.is_alive())
            self.assertFalse(writer_two.is_alive())
            self.assertFalse(entered_before_release, "second writer entered while the first pair was half-written")
            self.assertEqual(writer_errors, [])
            self.assertTrue(second_writer_entered_critical_section.is_set())
            self.assertEqual(first.read_text(encoding="utf-8"), "writer-two-request")
            self.assertEqual(second.read_text(encoding="utf-8"), "writer-two-result")
            self.assertFalse(list(root.glob(f"{OWNED_TEMP_PREFIX}*.tmp")))

    def test_generator_directory_lock_survives_legacy_lock_inode_swap_and_times_out_contender(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            first = root / "request.json"
            second = root / "result.json"
            first.write_text("initial-request", encoding="utf-8")
            second.write_text("initial-result", encoding="utf-8")
            legacy_lock_path = root / LEGACY_PAIR_LOCK_FILENAME
            legacy_lock_path.write_text("legacy-inode-one", encoding="utf-8")
            original_legacy_inode = legacy_lock_path.stat().st_ino

            child_program = "\n".join(
                (
                    "import sys",
                    "from pathlib import Path",
                    "import generate_capability_probe_fixtures as generator",
                    "generator.PAIR_LOCK_TIMEOUT_SECONDS = 0.2",
                    "try:",
                    "    generator._atomic_write_many(((Path(sys.argv[1]), 'child-request'), "
                    "(Path(sys.argv[2]), 'child-result')))",
                    "except TimeoutError:",
                    "    raise SystemExit(0)",
                    "raise SystemExit(3)",
                )
            )
            with _exclusive_pair_lock(root):
                replacement = root / "replacement-lock-inode"
                replacement.write_text("legacy-inode-two", encoding="utf-8")
                os.replace(replacement, legacy_lock_path)
                self.assertNotEqual(legacy_lock_path.stat().st_ino, original_legacy_inode)
                completed = subprocess.run(
                    [sys.executable, "-c", child_program, str(first), str(second)],
                    cwd=ROOT,
                    env={**os.environ, "PYTHONPATH": f"{ROOT / 'src'}:{ROOT / 'scripts'}"},
                    check=False,
                    capture_output=True,
                    text=True,
                    timeout=3,
                )
            self.assertEqual(completed.returncode, 0, completed.stdout + completed.stderr)
            self.assertEqual(first.read_text(encoding="utf-8"), "initial-request")
            self.assertEqual(second.read_text(encoding="utf-8"), "initial-result")
            self.assertEqual(legacy_lock_path.read_text(encoding="utf-8"), "legacy-inode-two")

    def test_generator_check_rejects_symlink_even_when_target_bytes_match(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            fixture_root = root / "fixtures"
            fixture_root.mkdir()
            request = build_request_fixture()
            result = build_result_fixture(request)

            def serialize(payload: dict[str, Any]) -> str:
                return json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n"

            request_target = fixture_root / "request-target.json"
            request_target.write_text(serialize(request), encoding="utf-8")
            request_path = fixture_root / "capability_probe_request_fixture_v1.json"
            request_path.symlink_to(request_target)
            (fixture_root / "capability_probe_result_fixture_v1.json").write_text(
                serialize(result), encoding="utf-8"
            )

            output = io.StringIO()
            with (
                mock.patch.object(capability_fixture_generator, "ROOT", root),
                contextlib.redirect_stdout(output),
            ):
                return_code = capability_fixture_generator.main(["--check"])
            self.assertEqual(return_code, 1)
            self.assertIn(str(request_path), output.getvalue())

    def test_positive_fixture_validates_and_binds_request(self) -> None:
        self.assertEqual(validate_capability_request(self.request), [])
        self.assertEqual(validate_capability_result(self.result, request=self.request), [])
        expected_hash = canonical_sha256(self.request)
        self.assertEqual(self.result["request_sha256"], expected_hash)
        self.assertEqual(self.result["provenance"]["request_sha256"], expected_hash)
        self.assertEqual(self.result["provenance"]["raw_response_sha256"], SYNTHETIC_RAW_RESPONSE_SHA256)
        self.assertEqual(self.result["usage"]["elapsed_ms"], 0)
        self.assertEqual(self.result["capability"]["verdict"], "fixture_contract_validated")
        self.assertFalse(self.result["capability"]["x_native_access_proven"])

    def test_declarative_schemas_are_strict_and_separate_from_stage0(self) -> None:
        self.assertIs(self.request_schema["additionalProperties"], False)
        self.assertIs(self.result_schema["additionalProperties"], False)
        for name in ("run", "task", "capability", "provenance", "usage", "observation", "error", "retention", "claims"):
            with self.subTest(name=name):
                self.assertIs(self.result_schema["$defs"][name]["additionalProperties"], False)
        self.assertEqual(self.request_schema["properties"]["execution_mode"]["const"], "fixture_only")
        self.assertEqual(self.result_schema["properties"]["execution_mode"]["const"], "fixture_only")
        self.assertIs(self.result_schema["$defs"]["capability"]["properties"]["x_native_access_proven"]["const"], False)
        self.assertEqual(
            self.result_schema["properties"]["observations"]["maxItems"],
            MAX_CAPABILITY_OBSERVATIONS,
        )
        self.assertEqual(
            self.request_schema["properties"]["hard_budgets"]["properties"]["max_observations"]["const"],
            MAX_CAPABILITY_OBSERVATIONS,
        )
        self.assertEqual(self.result_schema["$defs"]["error"]["properties"]["message"]["maxLength"], 280)
        self.assertEqual(
            set(self.result_schema["$defs"]["observation"]["properties"]["excerpt"]["enum"]),
            set(CAPABILITY_OBSERVATION_EXCERPTS.values()),
        )
        self.assertEqual(
            self.result_schema["$defs"]["provenance"]["properties"]["raw_response_sha256"]["const"],
            SYNTHETIC_RAW_RESPONSE_SHA256,
        )
        error_properties = self.result_schema["$defs"]["error"]["properties"]
        self.assertEqual(
            set(error_properties["code"]["enum"]),
            {value[0] for value in ERROR_ENVELOPE_BY_VERDICT.values()},
        )
        self.assertEqual(
            set(error_properties["message"]["enum"]),
            {value[1] for value in ERROR_ENVELOPE_BY_VERDICT.values()},
        )
        self.assertEqual(
            {error_properties["retryable"]["const"]},
            {value[2] for value in ERROR_ENVELOPE_BY_VERDICT.values()},
        )
        timestamp_properties = (
            self.result_schema["$defs"]["run"]["properties"]["started_at"],
            self.result_schema["$defs"]["run"]["properties"]["completed_at"],
            self.result_schema["$defs"]["observation"]["properties"]["authored_at"],
            self.result_schema["$defs"]["observation"]["properties"]["observed_at"],
        )
        for timestamp_property in timestamp_properties:
            self.assertEqual(timestamp_property["pattern"], CANONICAL_RFC3339_UTC_MILLIS_PATTERN)
            self.assertEqual(timestamp_property["minLength"], 24)
            self.assertEqual(timestamp_property["maxLength"], 24)
        stage0 = load_json(ROOT / "contracts/x.grok.collection.v1.schema.json")
        self.assertEqual(stage0["properties"]["schema_version"]["const"], "x.grok.collection.v1")
        self.assertNotIn("capability", stage0["properties"])
        contract_doc = (ROOT / "docs/STAGE1_CAPABILITY_FIXTURE_CONTRACT.md").read_text(encoding="utf-8")
        self.assertIn("CapabilityFixtureProfile", contract_doc)
        self.assertIn("before — not after — any second lab", contract_doc)

    def test_fixture_contains_only_synthetic_account_level_evidence(self) -> None:
        serialized = json.dumps({"request": self.request, "result": self.result}, ensure_ascii=False).casefold()
        self.assertNotIn("x.com/", serialized)
        self.assertNotIn("twitter.com/", serialized)
        self.assertNotIn("pretrain_relevance", serialized)
        self.assertNotIn("current_affiliation", serialized)
        self.assertEqual(self.request["target"]["account_kind"], "official_lab")
        self.assertFalse(self.request["claims"]["researcher_mapping_authorized"])
        for value in _walk(self.result):
            if isinstance(value, str) and value.startswith("https://"):
                self.assertTrue(value.startswith("https://posts.invalid/"), value)
        for field in ("candidate_packets", "identity_link_proposals", "assertions", "canonical_writes"):
            self.assertEqual(self.result[field], [])

    def test_request_owner_scope_budget_and_kill_switch_fail_closed(self) -> None:
        mutations: dict[str, tuple[Callable[[dict[str, Any]], None], str]] = {
            "live mode": (lambda p: p.__setitem__("execution_mode", "live"), "must be fixture_only"),
            "owner decision promoted": (
                lambda p: p["owner_decisions"].__setitem__("live_execution", "approved"),
                "must remain deferred",
            ),
            "researcher target": (
                lambda p: p["target"].__setitem__("account_kind", "researcher"),
                "synthetic official-lab account",
            ),
            "graph query": (
                lambda p: p["query"].__setitem__("query_kind", "graph_expansion"),
                "fixed synthetic official-account",
            ),
            "six observations": (
                lambda p: p["hard_budgets"].__setitem__("max_observations", 6),
                "must be exactly 1/1/1/5",
            ),
            "boolean budget": (
                lambda p: p["hard_budgets"].__setitem__("max_observations", True),
                "strict numeric types",
            ),
            "kill switch disarmed": (
                lambda p: p["kill_switch"].__setitem__("armed", False),
                "must be armed",
            ),
            "external execution authorized": (
                lambda p: p["claims"].__setitem__("external_execution_authorized", True),
                "must be false",
            ),
        }
        for name, (mutation, expected) in mutations.items():
            with self.subTest(name=name):
                self.assert_request_rejected(mutation, contains=expected)

    def test_request_live_urls_credentials_and_disallowed_signals_fail_closed(self) -> None:
        self.assert_request_rejected(
            lambda p: p["query"].__setitem__("query_template", "https://x.com/example/status/1"),
            contains="live X URL",
        )
        self.assert_request_rejected(
            lambda p: p.__setitem__("api_key", "fixture-secret"),
            contains="credential-bearing field",
        )
        self.assert_request_rejected(
            lambda p: p["target"].__setitem__("ethnicity_score", 1),
            contains="prohibited protected/proxy field",
        )

    def test_request_hash_provenance_and_access_claims_fail_closed(self) -> None:
        mutations: dict[str, tuple[Callable[[dict[str, Any]], None], str]] = {
            "request hash": (lambda p: p.__setitem__("request_sha256", "0" * 64), "bind the canonical request"),
            "provenance hash": (
                lambda p: p["provenance"].__setitem__("request_sha256", "0" * 64),
                "provenance mismatch: request_sha256",
            ),
            "generic web": (
                lambda p: p["provenance"].__setitem__("access_mode", "generic_web"),
                "provenance mismatch: access_mode",
            ),
            "model claim": (
                lambda p: p["provenance"].__setitem__("model_id", "grok-live"),
                "provenance mismatch: model_id",
            ),
            "X access claim": (
                lambda p: p["capability"].__setitem__("x_native_access_proven", True),
                "cannot prove X-native access",
            ),
            "invalid raw response hash": (
                lambda p: p["provenance"].__setitem__("raw_response_sha256", "0" * 64),
                "must bind the canonical synthetic raw response bytes",
            ),
        }
        for name, (mutation, expected) in mutations.items():
            with self.subTest(name=name):
                self.assert_result_rejected(mutation, contains=expected)

    def test_non_object_bound_requests_fail_closed_without_exception(self) -> None:
        for request in (None, [], "request", 1, True):
            with self.subTest(request=request):
                errors = validate_capability_result(copy.deepcopy(self.result), request=request)
                self.assertTrue(any("bound request invalid: request must be an object" in error for error in errors))

    def test_terminal_total_state_registry_and_failure_envelope(self) -> None:
        failure = self.build_failure_result()
        self.assertEqual(validate_capability_result(failure, request=self.request), [])

        self.assert_result_rejected(
            lambda p: p["run"].__setitem__("status", "running"),
            contains="unknown capability run status",
        )
        self.assert_result_rejected(
            lambda p: p["task"].__setitem__("status", "pending"),
            contains="unknown capability task status",
        )
        self.assert_result_rejected(
            lambda p: p["capability"].__setitem__("verdict", "x_native_proven"),
            contains="unknown capability verdict",
        )
        self.assert_result_rejected(
            lambda p: p["run"].__setitem__("status", "failed"),
            contains="terminal tuple is inconsistent",
        )

    def test_observation_and_error_content_registries_fail_closed(self) -> None:
        injected_excerpts = (
            "Synthetic capability evidence 001 plus an arbitrary individual name.",
            "Synthetic capability evidence 001 from @arbitrary_handle.",
            "Synthetic capability evidence 001 at https://x.com/example/status/1.",
            "Synthetic capability evidence 001 with api_key=fixture-value.",
            "Synthetic capability evidence 001 with a language signal and region signal.",
        )
        for excerpt in injected_excerpts:
            with self.subTest(excerpt=excerpt):
                self.assert_result_rejected(
                    lambda payload, excerpt=excerpt: payload["observations"][0].__setitem__("excerpt", excerpt),
                    contains="must match the deterministic object template",
                )
        self.assert_result_rejected(
            lambda payload: payload["observations"][0].__setitem__("observation_id", "xprobe_obs_fixture_002"),
            contains="must bind its platform object sequence",
        )
        self.assert_result_rejected(
            lambda payload: payload["observations"][0].__setitem__(
                "excerpt", CAPABILITY_OBSERVATION_EXCERPTS["xpost_fixture_002"]
            ),
            contains="must match the deterministic object template",
        )

        injected_messages = (
            "Synthetic failure involving an arbitrary individual name.",
            "Synthetic failure from @arbitrary_handle.",
            "Synthetic failure at https://x.com/example/status/1.",
            "Synthetic failure with api_key=fixture-value.",
            "Synthetic failure with a language signal and region signal.",
        )
        for message in injected_messages:
            with self.subTest(message=message):
                failure = self.build_failure_result()
                failure["errors"][0]["message"] = message
                errors = validate_capability_result(failure, request=self.request)
                self.assertTrue(
                    any("must match the deterministic verdict envelope" in error for error in errors),
                    errors,
                )

        overlong_failure = self.build_failure_result()
        overlong_failure["errors"][0]["message"] = "S" * 281
        overlong_errors = validate_capability_result(overlong_failure, request=self.request)
        self.assertTrue(any("at most 280 characters" in error for error in overlong_errors), overlong_errors)

        wrong_code_failure = self.build_failure_result()
        wrong_code_failure["errors"][0]["code"] = "synthetic_other"
        wrong_code_errors = validate_capability_result(wrong_code_failure, request=self.request)
        self.assertTrue(
            any("must match the deterministic verdict envelope" in error for error in wrong_code_errors),
            wrong_code_errors,
        )

        retryable_failure = self.build_failure_result()
        retryable_failure["errors"][0]["retryable"] = True
        retryable_errors = validate_capability_result(retryable_failure, request=self.request)
        self.assertTrue(
            any("must match the deterministic verdict envelope" in error for error in retryable_errors),
            retryable_errors,
        )

    def test_runtime_and_cli_diagnostics_never_echo_untrusted_content(self) -> None:
        sentinels = (
            "SENTINEL_UNKNOWN_NATIONALITY_SECRET_FIELD_7D3F",
            "SENTINEL_PERSON_VALUE_ALICE_7D3F",
            "SENTINEL_STATUS_ALICE_7D3F",
            "SENTINEL_VERDICT_ALICE_7D3F",
            "SENTINEL_OBSERVATION_ALICE_7D3F",
            "SENTINEL_ERROR_CODE_ALICE_7D3F",
            "SENTINEL_ERROR_MESSAGE_ALICE_SECRET_7D3F",
        )
        unknown_field, person_value, status, verdict, observation_id, error_code, error_message = sentinels
        request = copy.deepcopy(self.request)
        request[unknown_field] = person_value
        request["target"][unknown_field] = person_value
        result = copy.deepcopy(self.result)
        result["run"]["status"] = status
        result["task"]["status"] = status
        result["capability"]["verdict"] = verdict
        result["observations"][0]["observation_id"] = observation_id
        result["observations"][0][unknown_field] = person_value
        result["errors"] = [{"code": error_code, "message": error_message, "retryable": False}]

        diagnostics = [
            *validate_capability_request(request),
            *validate_capability_result(result, request=request),
        ]
        self.assertTrue(diagnostics)
        for diagnostic in diagnostics:
            self.assertIn(diagnostic.partition(":")[0], DIAGNOSTIC_CODES)
        rendered = json.dumps(diagnostics, ensure_ascii=False)
        for sentinel in sentinels:
            self.assertNotIn(sentinel, rendered)
        self.assertIn("result.observations[0]", rendered)

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            request_path = root / "request.json"
            result_path = root / "result.json"
            request_path.write_text(json.dumps(request), encoding="utf-8")
            result_path.write_text(json.dumps(result), encoding="utf-8")
            completed = subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "x_first.capability_probe",
                    "--request",
                    str(request_path),
                    "--result",
                    str(result_path),
                ],
                cwd=ROOT,
                env={**os.environ, "PYTHONPATH": "src"},
                check=False,
                capture_output=True,
                text=True,
                timeout=60,
            )
        self.assertEqual(completed.returncode, 1, completed.stdout + completed.stderr)
        for sentinel in sentinels:
            self.assertNotIn(sentinel, completed.stdout)
            self.assertNotIn(sentinel, completed.stderr)
        cli_payload = json.loads(completed.stdout)
        self.assertEqual(cli_payload["status"], "invalid")
        for diagnostic in cli_payload["errors"]:
            self.assertIn(diagnostic.partition(":")[0], DIAGNOSTIC_CODES)

    def test_cli_load_boundary_rejects_non_objects_malformed_json_and_private_paths(self) -> None:
        cases = (
            ("request", '["SENTINEL_LIST_VALUE_7D3F"]', REQUEST_JSON_LOAD_DIAGNOSTIC),
            ("result", '"SENTINEL_PRIMITIVE_VALUE_7D3F"', RESULT_JSON_LOAD_DIAGNOSTIC),
            ("request", '{"SENTINEL_MALFORMED_VALUE_7D3F":', REQUEST_JSON_LOAD_DIAGNOSTIC),
            ("result", None, RESULT_JSON_LOAD_DIAGNOSTIC),
        )
        for subject, content, expected_diagnostic in cases:
            with self.subTest(subject=subject, content=content):
                with tempfile.TemporaryDirectory() as directory:
                    root = Path(directory)
                    request_path = root / "request.json"
                    result_path = root / "result.json"
                    request_path.write_text(json.dumps(self.request), encoding="utf-8")
                    result_path.write_text(json.dumps(self.result), encoding="utf-8")
                    hostile_path = root / f"SENTINEL_PRIVATE_{subject.upper()}_PATH_7D3F.json"
                    if content is not None:
                        hostile_path.write_text(content, encoding="utf-8")
                    if subject == "request":
                        request_path = hostile_path
                    else:
                        result_path = hostile_path
                    completed = subprocess.run(
                        [
                            sys.executable,
                            "-m",
                            "x_first.capability_probe",
                            "--request",
                            str(request_path),
                            "--result",
                            str(result_path),
                        ],
                        cwd=ROOT,
                        env={**os.environ, "PYTHONPATH": "src"},
                        check=False,
                        capture_output=True,
                        text=True,
                        timeout=60,
                    )
                self.assertEqual(completed.returncode, 1, completed.stdout + completed.stderr)
                self.assertEqual(completed.stderr, "")
                self.assertNotIn("SENTINEL_", completed.stdout)
                self.assertEqual(
                    json.loads(completed.stdout),
                    {"errors": [expected_diagnostic], "status": "invalid"},
                )

    def test_cli_validation_boundary_collapses_unexpected_exceptions_without_echo(self) -> None:
        sentinel = "SENTINEL_INTERNAL_EXCEPTION_DETAIL_7D3F"
        with mock.patch(
            "x_first.capability_probe.validate_capability_request",
            side_effect=RuntimeError(sentinel),
        ):
            errors = _validate_cli_payloads(copy.deepcopy(self.request), copy.deepcopy(self.result))
        self.assertEqual(errors, [REQUEST_VALIDATION_DIAGNOSTIC])
        self.assertNotIn(sentinel, json.dumps(errors))

        with mock.patch(
            "x_first.capability_probe.validate_capability_result",
            side_effect=RuntimeError(sentinel),
        ):
            errors = _validate_cli_payloads(copy.deepcopy(self.request), copy.deepcopy(self.result))
        self.assertEqual(errors, [RESULT_VALIDATION_DIAGNOSTIC])
        self.assertNotIn(sentinel, json.dumps(errors))

    def test_run_duration_and_elapsed_ms_reconcile_exactly(self) -> None:
        one_millisecond = copy.deepcopy(self.result)
        one_millisecond["run"]["completed_at"] = "2026-07-14T00:00:00.001Z"
        one_millisecond["usage"]["elapsed_ms"] = 1
        self.assertEqual(validate_capability_result(one_millisecond, request=self.request), [])

        self.assert_result_rejected(
            lambda payload: payload["usage"].__setitem__("elapsed_ms", 1),
            contains="must exactly match the run timestamp duration",
        )
        self.assert_result_rejected(
            lambda payload: payload["run"].__setitem__("completed_at", "2026-07-14T00:00:00.001Z"),
            contains="must exactly match the run timestamp duration",
        )
        self.assert_result_rejected(
            lambda payload: payload["run"].__setitem__("completed_at", "2026-07-14T00:00:00.000001Z"),
            contains="must use canonical UTC RFC3339 millisecond form",
        )

    def test_timestamp_fields_reject_noncanonical_and_control_bearing_forms(self) -> None:
        variants = (
            "2026-07-14T00:00:00Z",
            "20260714T000000.000Z",
            "2026-W29-2T00:00:00.000Z",
            "2026-07-14T00:00:00.000+00:00",
            "2026-07-14T08:00:00.000+08:00",
            "2026-07-14 00:00:00.000Z",
            "2026-07-14T00:00:00.000z",
            " 2026-07-14T00:00:00.000Z",
            "\x002026-07-14T00:00:00.000Z",
            "2026-07-14T00:00:\n00.000Z",
            "2026-07-14T00:00:\r00.000Z",
            "2026-07-14T00:00:\t00.000Z",
        )
        for timestamp in variants:
            with self.subTest(field="run.completed_at", timestamp=repr(timestamp)):
                result = copy.deepcopy(self.result)
                result["run"]["completed_at"] = timestamp
                errors = validate_capability_result(result, request=self.request)
                self.assertTrue(any("canonical UTC RFC3339 millisecond form" in error for error in errors), errors)
            with self.subTest(field="observations[0].observed_at", timestamp=repr(timestamp)):
                result = copy.deepcopy(self.result)
                result["observations"][0]["observed_at"] = timestamp
                errors = validate_capability_result(result, request=self.request)
                self.assertTrue(any("canonical UTC RFC3339 millisecond form" in error for error in errors), errors)

        for timestamp in (
            self.result["run"]["started_at"],
            self.result["run"]["completed_at"],
            *(observation["authored_at"] for observation in self.result["observations"]),
            *(observation["observed_at"] for observation in self.result["observations"]),
        ):
            self.assertIsNotNone(re.fullmatch(CANONICAL_RFC3339_UTC_MILLIS_PATTERN, timestamp))

    def test_usage_totals_and_hard_budgets_fail_closed(self) -> None:
        mutations: dict[str, tuple[Callable[[dict[str, Any]], None], str]] = {
            "external call": (
                lambda p: p["usage"].__setitem__("external_calls", 1),
                "must have zero external execution",
            ),
            "boolean execution": (
                lambda p: p["usage"].__setitem__("executions", False),
                "must be non-negative integers",
            ),
            "observation total": (
                lambda p: p["usage"].__setitem__("observations", 2),
                "does not reconcile",
            ),
            "deadline": (
                lambda p: p["usage"].__setitem__("elapsed_ms", 1001),
                "exceeds request budget: elapsed_ms",
            ),
            "cost": (
                lambda p: p["usage"].__setitem__("cost_usd", 1),
                "must have zero external execution",
            ),
        }
        for name, (mutation, expected) in mutations.items():
            with self.subTest(name=name):
                self.assert_result_rejected(mutation, contains=expected)

    def test_observation_iteration_is_bounded_before_validating_attacker_sized_arrays(self) -> None:
        result = copy.deepcopy(self.result)
        result["observations"] = [copy.deepcopy(self.result["observations"][0]) for _ in range(1000)]
        result["usage"]["observations"] = 1000

        errors = validate_capability_result(result, request=self.request)
        overflow_errors = [error for error in errors if "exceeds the fixed maximum of five" in error]
        self.assertEqual(len(overflow_errors), 1, errors)
        rendered = json.dumps(errors, ensure_ascii=False)
        observation_indices = [
            int(match.group(1)) for match in re.finditer(r"result\.observations\[([0-9]+)\]", rendered)
        ]
        self.assertTrue(errors)
        self.assertLessEqual(len(errors), 12, errors)
        self.assertLessEqual(len(rendered.encode("utf-8")), 4096, rendered)
        self.assertTrue(all(index < MAX_CAPABILITY_OBSERVATIONS for index in observation_indices))
        self.assertNotIn("result.observations[5]", rendered)

    def test_observation_identity_url_time_and_minimization_fail_closed(self) -> None:
        mutations: dict[str, tuple[Callable[[dict[str, Any]], None], str]] = {
            "live URL": (
                lambda p: p["observations"][0].__setitem__(
                    "canonical_url", "https://x.com/fixture_openai_official/status/xpost_fixture_001"
                ),
                "live X URL",
            ),
            "URL object mismatch": (
                lambda p: p["observations"][0].__setitem__(
                    "canonical_url", "https://posts.invalid/fixture_openai_official/status/xpost_fixture_999"
                ),
                "not canonical synthetic evidence",
            ),
            "URL query ambiguity": (
                lambda p: p["observations"][0].__setitem__(
                    "canonical_url",
                    "https://posts.invalid/fixture_openai_official/status/xpost_fixture_001?source=other",
                ),
                "not canonical synthetic evidence",
            ),
            "URL userinfo ambiguity": (
                lambda p: p["observations"][0].__setitem__(
                    "canonical_url",
                    "https://fixture@posts.invalid/fixture_openai_official/status/xpost_fixture_001",
                ),
                "not canonical synthetic evidence",
            ),
            "URL parser exception": (
                lambda p: p["observations"][0].__setitem__("canonical_url", "https://[invalid"),
                "cannot be parsed safely",
            ),
            "account mismatch": (
                lambda p: p["observations"][0].__setitem__("platform_user_id", "xuid_fixture_other"),
                "account mismatch",
            ),
            "future authored": (
                lambda p: p["observations"][0].__setitem__("authored_at", "2027-01-01T00:00:00.000Z"),
                "authored_at follows observed_at",
            ),
            "naive timestamp": (
                lambda p: p["observations"][0].__setitem__("observed_at", "2026-07-14T00:00:00"),
                "must use canonical UTC RFC3339 millisecond form",
            ),
            "unbounded text": (
                lambda p: p["observations"][0].__setitem__("excerpt", "Synthetic capability evidence " + "x" * 300),
                "not bounded synthetic text",
            ),
            "full body": (
                lambda p: p["observations"][0].__setitem__("full_body_stored", True),
                "cannot store a full body",
            ),
            "duplicate URL": (
                lambda p: p["observations"][1].__setitem__(
                    "canonical_url", p["observations"][0]["canonical_url"]
                ),
                "must be unique",
            ),
        }
        for name, (mutation, expected) in mutations.items():
            with self.subTest(name=name):
                self.assert_result_rejected(mutation, contains=expected)

    def test_observation_raw_url_must_match_exactly_before_defensive_parsing(self) -> None:
        canonical = self.result["observations"][0]["canonical_url"]
        variants = (
            canonical.replace("https", "HTTPS", 1),
            canonical.replace("posts.invalid", "POSTS.INVALID", 1),
            f"{canonical}?",
            f"{canonical}#",
            f" {canonical}",
            f"\x00{canonical}",
            canonical.replace("posts.invalid", "posts.inva\nlid", 1),
            canonical.replace("posts.invalid", "posts.inva\rlid", 1),
            canonical.replace("posts.invalid", "posts.inva\tlid", 1),
        )
        for url in variants:
            with self.subTest(url=repr(url)):
                self.assert_result_rejected(
                    lambda payload, url=url: payload["observations"][0].__setitem__("canonical_url", url),
                    contains="not the exact canonical synthetic string",
                )

    def test_writers_claims_credentials_and_disallowed_signals_fail_closed(self) -> None:
        self.assert_result_rejected(
            lambda p: p["candidate_packets"].append({"id": "candidate"}),
            contains="candidate_packets must be empty",
        )
        self.assert_result_rejected(
            lambda p: p["canonical_writes"].append({"id": "write"}),
            contains="canonical_writes must be empty",
        )
        self.assert_result_rejected(
            lambda p: p["claims"].__setitem__("researcher_mapping_authorized", True),
            contains="must be false",
        )
        self.assert_result_rejected(
            lambda p: p["observations"][0].__setitem__("oauth_token", "fixture-secret"),
            contains="credential-bearing field",
        )
        self.assert_result_rejected(
            lambda p: p["observations"][0].__setitem__("nationalityEvidence", "synthetic"),
            contains="prohibited protected/proxy field",
        )

    def test_malformed_arrays_fail_closed_without_exception(self) -> None:
        for field in ("observations", "errors", "candidate_packets"):
            with self.subTest(field=field):
                result = copy.deepcopy(self.result)
                result[field] = None
                self.assertTrue(validate_capability_result(result, request=self.request))

    def test_source_boundary_and_runtime_under_sixty_seconds(self) -> None:
        forbidden_import_roots = {"sourcing_agent", "requests", "httpx", "aiohttp", "anthropic", "openai"}
        for path in [*sorted((ROOT / "src").rglob("*.py")), *sorted((ROOT / "scripts").rglob("*.py"))]:
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
            imported: set[str] = set()
            for node in ast.walk(tree):
                if isinstance(node, ast.Import):
                    imported.update(alias.name.split(".")[0] for alias in node.names)
                elif isinstance(node, ast.ImportFrom) and node.module:
                    imported.add(node.module.split(".")[0])
            self.assertFalse(imported & forbidden_import_roots, f"forbidden import in {path}: {imported}")
        pyproject = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
        self.assertEqual(pyproject["project"]["dependencies"], [])

        started = time.monotonic()
        request = build_request_fixture()
        result = build_result_fixture(request)
        self.assertEqual(validate_capability_request(request), [])
        self.assertEqual(validate_capability_result(result, request=request), [])
        self.assertLess(time.monotonic() - started, 60)


if __name__ == "__main__":
    unittest.main()
