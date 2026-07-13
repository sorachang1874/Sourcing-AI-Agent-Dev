from __future__ import annotations

import ast
import copy
import json
import os
import subprocess
import sys
import time
import tomllib
import unittest
from collections.abc import Callable, Iterable
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "scripts"))

from generate_capability_probe_fixtures import build_request_fixture, build_result_fixture  # noqa: E402

from x_first.capability_probe import (  # noqa: E402
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

    def test_positive_fixture_validates_and_binds_request(self) -> None:
        self.assertEqual(validate_capability_request(self.request), [])
        self.assertEqual(validate_capability_result(self.result, request=self.request), [])
        expected_hash = canonical_sha256(self.request)
        self.assertEqual(self.result["request_sha256"], expected_hash)
        self.assertEqual(self.result["provenance"]["request_sha256"], expected_hash)
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
        stage0 = load_json(ROOT / "contracts/x.grok.collection.v1.schema.json")
        self.assertEqual(stage0["properties"]["schema_version"]["const"], "x.grok.collection.v1")
        self.assertNotIn("capability", stage0["properties"])

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
                lambda p: p["provenance"].__setitem__("raw_response_sha256", "not-a-hash"),
                "must be a lowercase SHA-256",
            ),
        }
        for name, (mutation, expected) in mutations.items():
            with self.subTest(name=name):
                self.assert_result_rejected(mutation, contains=expected)

    def test_terminal_total_state_registry_and_failure_envelope(self) -> None:
        failure = copy.deepcopy(self.result)
        failure["run"]["status"] = "failed"
        failure["task"].update({"status": "failed", "stop_reason": "fixture_failed"})
        failure["capability"]["verdict"] = "capability_unavailable"
        failure["observations"] = []
        failure["usage"]["observations"] = 0
        failure["errors"] = [
            {"code": "synthetic_unavailable", "message": "Synthetic capability unavailable.", "retryable": False}
        ]
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
