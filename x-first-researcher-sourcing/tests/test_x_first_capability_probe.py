from __future__ import annotations

import ast
import copy
import json
import os
import subprocess
import sys
import tempfile
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

from generate_capability_probe_fixtures import (  # noqa: E402
    _atomic_write_many,
    build_request_fixture,
    build_result_fixture,
)

from x_first.capability_probe import (  # noqa: E402
    CAPABILITY_OBSERVATION_EXCERPTS,
    ERROR_ENVELOPE_BY_VERDICT,
    SYNTHETIC_RAW_RESPONSE_SHA256,
    canonical_sha256,
    validate_capability_request,
    validate_capability_result,
)
from x_first.contracts import load_json  # noqa: E402


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
            self.assertFalse(list(root.glob(".*.tmp")))

            target = root / "target.json"
            symlink = root / "symlink.json"
            target.write_text("target-original", encoding="utf-8")
            symlink.symlink_to(target)
            with self.assertRaisesRegex(ValueError, "refusing to replace symlink"):
                _atomic_write_many(((symlink, "must-not-write"), (second, "must-not-write")))
            self.assertEqual(target.read_text(encoding="utf-8"), "target-original")
            self.assertEqual(second.read_text(encoding="utf-8"), "new-second")

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
            self.assertFalse(list(root.glob(".*.tmp")))

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
        self.assertEqual(self.result_schema["properties"]["observations"]["maxItems"], 5)
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
            contains="must resolve to an exact non-negative millisecond count",
        )

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
                lambda p: p["observations"][0].__setitem__("authored_at", "2027-01-01T00:00:00Z"),
                "authored_at follows observed_at",
            ),
            "naive timestamp": (
                lambda p: p["observations"][0].__setitem__("observed_at", "2026-07-14T00:00:00"),
                "timestamps are invalid",
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
