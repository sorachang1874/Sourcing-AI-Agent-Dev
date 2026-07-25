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
from collections import Counter
from collections.abc import Callable, Iterable
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "scripts"))

from generate_openai_fixture import build_fixture, build_gold  # noqa: E402

from x_first.contracts import (  # noqa: E402
    IN_SCOPE_RELEVANCE,
    evaluate_selection,
    load_json,
    validate_acceptance,
    validate_collection,
)


def _walk(value: Any) -> Iterable[Any]:
    yield value
    if isinstance(value, dict):
        for child in value.values():
            yield from _walk(child)
    elif isinstance(value, list):
        for child in value:
            yield from _walk(child)


class XFirstFixtureContractTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.fixture = build_fixture()
        cls.gold = build_gold()
        cls.labs = load_json(ROOT / "configs/labs.v1.json")
        cls.queries = load_json(ROOT / "configs/query_families.v1.json")
        cls.schema = load_json(ROOT / "contracts/x.grok.collection.v1.schema.json")

    def validate(self, payload: dict[str, Any]) -> list[str]:
        return validate_collection(payload, labs_registry=self.labs, query_registry=self.queries)

    def assert_rejected(
        self,
        mutation: Callable[[dict[str, Any]], None],
        *,
        contains: str | None = None,
    ) -> None:
        payload = copy.deepcopy(self.fixture)
        mutation(payload)
        errors = self.validate(payload)
        self.assertTrue(errors, "mutation unexpectedly passed the executable contract")
        if contains is not None:
            self.assertTrue(
                any(contains in error for error in errors),
                f"expected {contains!r} in {errors}",
            )

    def test_generated_artifacts_are_current_and_deterministic(self) -> None:
        self.assertEqual(build_fixture(), build_fixture())
        self.assertEqual(build_gold(), build_gold())
        self.assertEqual(load_json(ROOT / "fixtures/openai_pretrain_fixture_v1.json"), self.fixture)
        self.assertEqual(load_json(ROOT / "fixtures/openai_pretrain_gold_v1.json"), self.gold)
        result = subprocess.run(
            [sys.executable, "scripts/generate_openai_fixture.py", "--check"],
            cwd=ROOT,
            env={**os.environ, "PYTHONPATH": "src"},
            check=False,
            capture_output=True,
            text=True,
            timeout=60,
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_fixture_validates_and_schema_declares_strict_shape(self) -> None:
        self.assertEqual(self.validate(self.fixture), [])
        self.assertIs(self.schema["additionalProperties"], False)
        for name in ("run", "task", "account", "observation", "candidate_packet", "coverage", "claims"):
            with self.subTest(name=name):
                self.assertIs(self.schema["$defs"][name]["additionalProperties"], False)
        exact_counts = {
            "tasks": 8,
            "accounts": 24,
            "observations": 96,
            "candidate_packets": 24,
            "coverage_ledger": 8,
        }
        for field, count in exact_counts.items():
            with self.subTest(field=field):
                node = self.schema["properties"][field]
                self.assertEqual((node["minItems"], node["maxItems"]), (count, count))
        for field in ("identity_link_proposals", "assertions", "errors"):
            self.assertEqual(self.schema["properties"][field]["maxItems"], 0)
        self.assertTrue(self.schema["$defs"]["candidate_packet"]["allOf"])

    def test_exact_counts_coverage_and_reference_closure(self) -> None:
        self.assertEqual(len(self.fixture["accounts"]), 24)
        self.assertEqual(len(self.fixture["observations"]), 96)
        self.assertEqual(len(self.fixture["tasks"]), 8)
        self.assertEqual(len(self.fixture["candidate_packets"]), 24)
        self.assertEqual(len(self.fixture["coverage_ledger"]), 8)

        task_counts = Counter(task["query_family"] for task in self.fixture["tasks"])
        coverage_counts = Counter(entry["query_family"] for entry in self.fixture["coverage_ledger"])
        observation_counts = Counter(item["query_family"] for item in self.fixture["observations"])
        self.assertEqual(set(task_counts.values()), {1})
        self.assertEqual(set(coverage_counts.values()), {1})
        self.assertEqual(set(observation_counts.values()), {12})
        self.assertEqual(len({item["platform_object_id"] for item in self.fixture["observations"]}), 96)

        tasks = {task["task_id"]: task for task in self.fixture["tasks"]}
        for task_id, task in tasks.items():
            observed = {
                item["observation_id"] for item in self.fixture["observations"] if item["source_task_id"] == task_id
            }
            self.assertEqual(set(task["observation_ids"]), observed)
        for entry in self.fixture["coverage_ledger"]:
            task = tasks[entry["task_id"]]
            self.assertEqual(entry["query_family"], task["query_family"])
            self.assertEqual(entry["lab_id"], task["lab_id"])
            self.assertEqual(entry["observation_count"], 12)
        self.assertEqual(
            self.fixture["run"]["hard_budgets"],
            {"max_tasks": 8, "max_pages": 8, "max_observations": 96, "max_candidate_packets": 24},
        )

    def test_handle_rename_preserves_external_account_identity(self) -> None:
        packets = {packet["platform_user_id"]: packet for packet in self.fixture["candidate_packets"]}
        observations = self.fixture["observations"]
        for number, account in enumerate(self.fixture["accounts"], start=1):
            account_id = account["platform_user_id"]
            expected_history_length = 2 if number <= 6 else 1
            self.assertEqual(len(account["handle_history"]), expected_history_length)
            active = [item for item in account["handle_history"] if item["observed_to"] is None]
            self.assertEqual(len(active), 1)
            self.assertEqual(active[0]["handle"], account["current_handle"])
            self.assertEqual(account["handle_history"][-1]["handle"], account["current_handle"])
            self.assertEqual(packets[account_id]["platform_user_id"], account_id)
            self.assertEqual(
                {item["platform_user_id"] for item in observations if item["platform_user_id"] == account_id},
                {account_id},
            )

    def test_population_truth_table_metrics_and_unknown_quarantine(self) -> None:
        packets = self.fixture["candidate_packets"]
        in_scope = [packet for packet in packets if packet["pretrain_relevance"] in IN_SCOPE_RELEVANCE]
        out_of_scope = [packet for packet in packets if packet["pretrain_relevance"] == "OUT_OF_SCOPE"]
        unknown = [packet for packet in packets if packet["pretrain_relevance"] == "UNKNOWN"]
        self.assertEqual((len(in_scope), len(out_of_scope), len(unknown)), (20, 2, 2))
        for packet in unknown:
            self.assertFalse(packet["selected_for_review"])
            self.assertEqual(packet["review_status"], "quarantined")
            self.assertEqual(packet["current_affiliation_proposal"]["status"], "unknown")
        metrics = evaluate_selection(self.fixture, self.gold)
        self.assertGreaterEqual(metrics.precision, self.gold["minimum_precision"])
        self.assertGreaterEqual(metrics.recall, self.gold["minimum_recall"])
        self.assertEqual(metrics.false_merge_count, 0)
        self.assertEqual(validate_acceptance(self.fixture, self.gold, metrics=metrics), [])

    def test_fixture_has_only_synthetic_data_and_no_canonical_writes(self) -> None:
        serialized = json.dumps(self.fixture, ensure_ascii=False).casefold()
        self.assertNotIn("x.com/", serialized)
        self.assertNotIn("twitter.com/", serialized)
        for value in _walk(self.fixture):
            if isinstance(value, str) and value.startswith("https://"):
                self.assertTrue(urlsplit(value).hostname.endswith(".invalid"), value)
        self.assertEqual(self.fixture["identity_link_proposals"], [])
        self.assertEqual(self.fixture["assertions"], [])
        self.assertEqual(self.fixture["errors"], [])
        self.assertTrue(all(value is False for value in self.fixture["claims"].values()))
        self.assertTrue(
            all(account["display_label"].startswith("Synthetic Account ") for account in self.fixture["accounts"])
        )
        self.assertTrue(
            all(item["excerpt"].startswith("Synthetic professional evidence ") for item in self.fixture["observations"])
        )

    def test_adversarial_contract_mutations_fail_closed(self) -> None:
        mutations: dict[str, tuple[Callable[[dict[str, Any]], None], str | None]] = {
            "unknown run status": (lambda p: p["run"].__setitem__("status", "mystery"), "unknown run status"),
            "failed task in completed run": (
                lambda p: p["tasks"][0].__setitem__("status", "failed"),
                "must be succeeded",
            ),
            "unknown not quarantined": (
                lambda p: p["candidate_packets"][-1].__setitem__("review_status", "fixture_ready"),
                "unknown relevance must be quarantined",
            ),
            "integer selection masquerades as bool": (
                lambda p: p["candidate_packets"][0].__setitem__("selected_for_review", 1),
                "selected_for_review must be boolean",
            ),
            "non ULID provisional ID": (
                lambda p: p["candidate_packets"][0].__setitem__(
                    "provisional_person_id", "pp_x_ALICE_SMITH_RESEARCHER000"
                ),
                "invalid provisional person id",
            ),
            "overflow ULID provisional ID": (
                lambda p: p["candidate_packets"][0].__setitem__(
                    "provisional_person_id", "pp_x_Z0000000000000000000000000"
                ),
                "invalid provisional person id",
            ),
            "unknown affiliation promoted": (
                lambda p: p["candidate_packets"][-1]["current_affiliation_proposal"].__setitem__(
                    "status", "current_proposed"
                ),
                "unknown relevance must be quarantined",
            ),
            "live X URL": (
                lambda p: p["observations"][0].__setitem__("canonical_url", "https://x.com/example/status/1"),
                "live X URL",
            ),
            "non-synthetic fixture URL path": (
                lambda p: p["observations"][0].__setitem__(
                    "canonical_url", "https://evidence.invalid/openai/real-person"
                ),
                "canonical_url is not synthetic",
            ),
            "assertion writer": (lambda p: p["assertions"].append({"id": "assertion"}), "must be empty"),
            "identity link writer": (
                lambda p: p["identity_link_proposals"].append({"id": "link"}),
                "must be empty",
            ),
            "cross-account evidence": (
                lambda p: p["candidate_packets"][0]["evidence_refs"].__setitem__(
                    0, p["candidate_packets"][1]["evidence_refs"][0]
                ),
                "belongs to another account",
            ),
            "duplicate packet evidence": (
                lambda p: p["candidate_packets"][0]["evidence_refs"].__setitem__(
                    1, p["candidate_packets"][0]["evidence_refs"][0]
                ),
                "four unique evidence refs",
            ),
            "missing affiliation evidence": (
                lambda p: p["candidate_packets"][0]["current_affiliation_proposal"]["evidence_refs"].__setitem__(
                    0, "missing"
                ),
                "affiliation references unknown evidence",
            ),
            "weak affiliation evidence only": (
                lambda p: p["candidate_packets"][1]["current_affiliation_proposal"].__setitem__(
                    "evidence_refs",
                    [p["candidate_packets"][1]["evidence_refs"][1], p["candidate_packets"][1]["evidence_refs"][3]],
                ),
                "lacks a bounded professional affiliation source",
            ),
            "missing task observation": (
                lambda p: p["tasks"][0]["observation_ids"].__setitem__(0, "missing"),
                "do not close over observations",
            ),
            "missing coverage task": (
                lambda p: p["coverage_ledger"][0].__setitem__("task_id", "missing"),
                "coverage task/family/lab linkage mismatch",
            ),
            "duplicate platform object": (
                lambda p: p["observations"][1].__setitem__(
                    "platform_object_id", p["observations"][0]["platform_object_id"]
                ),
                "platform_object_id values must be unique",
            ),
            "duplicate account profile URL": (
                lambda p: p["accounts"][1].__setitem__("profile_url", p["accounts"][0]["profile_url"]),
                "profile_url values must be unique",
            ),
            "duplicate observation canonical URL": (
                lambda p: p["observations"][1].__setitem__("canonical_url", p["observations"][0]["canonical_url"]),
                "canonical_url values must be unique",
            ),
            "duplicate task family": (
                lambda p: p["tasks"][1].__setitem__("query_family", p["tasks"][0]["query_family"]),
                "exactly one task",
            ),
            "duplicate coverage family": (
                lambda p: p["coverage_ledger"][1].__setitem__("query_family", p["coverage_ledger"][0]["query_family"]),
                "exactly one coverage",
            ),
            "nested extra field": (
                lambda p: p["accounts"][0].__setitem__("unowned_field", "value"),
                "unexpected fields",
            ),
            "inflated budget": (
                lambda p: p["run"]["hard_budgets"].__setitem__("max_tasks", 9),
                "must equal 8",
            ),
            "query page overflow": (lambda p: p["tasks"][0].__setitem__("pages", 100), "page budget mismatch"),
            "boolean query page": (lambda p: p["tasks"][0].__setitem__("pages", True), "page budget mismatch"),
            "malformed query page": (lambda p: p["tasks"][0].__setitem__("pages", {"bad": 1}), "page budget mismatch"),
            "boolean provider call count": (
                lambda p: p["run"]["provider"].__setitem__("external_calls", False),
                "external_calls must be integer zero",
            ),
            "boolean provider cost": (
                lambda p: p["run"]["provider"].__setitem__("cost_usd", False),
                "cost_usd must be numeric zero",
            ),
            "non timestamp evidence": (
                lambda p: p["observations"][0].__setitem__("authored_at", "Alice Example"),
                "observation timestamps are invalid",
            ),
            "evidence observed before authored": (
                lambda p: p["observations"][0].__setitem__("authored_at", "2027-01-01T00:00:00Z"),
                "authored_at must not follow observed_at",
            ),
            "run completes before start": (
                lambda p: (
                    p["run"].__setitem__("started_at", "2027-01-01T00:00:00Z"),
                    p["run"].__setitem__("completed_at", "2026-01-01T00:00:00Z"),
                ),
                "started_at must not follow completed_at",
            ),
            "account observation outside run window": (
                lambda p: p["accounts"][0].__setitem__("observed_at", "2027-01-01T00:00:00Z"),
                "account observed_at must fall within the run window",
            ),
            "observation outside run window": (
                lambda p: p["observations"][0].__setitem__("observed_at", "2027-01-01T00:00:00Z"),
                "observation observed_at must fall within the run window",
            ),
            "active handle starts after account observation": (
                lambda p: p["accounts"][0]["handle_history"][-1].__setitem__("observed_from", "2027-01-01T00:00:00Z"),
                "active handle must start on or before account observed_at",
            ),
            "non canonical stop reason": (
                lambda p: (
                    p["tasks"][0].__setitem__("stop_reason", "budget_reached"),
                    p["coverage_ledger"][0].__setitem__("stop_reason", "budget_reached"),
                ),
                "stop_reason must be fixture_complete",
            ),
            "relevance evidence conflict": (
                lambda p: p["observations"][0].__setitem__("technical_scope", "OUT_OF_SCOPE"),
                "relevance conflicts with evidence",
            ),
            "weakened canonical relevance": (
                lambda p: (
                    p["candidate_packets"][0].__setitem__("pretrain_relevance", "OUT_OF_SCOPE"),
                    p["candidate_packets"][0].__setitem__("selected_for_review", False),
                    [item.__setitem__("technical_scope", "OUT_OF_SCOPE") for item in p["observations"][:4]],
                ),
                "canonical gold population",
            ),
            "non-synthetic excerpt": (
                lambda p: p["observations"][0].__setitem__("excerpt", "Real person and real post"),
                "excerpt is not synthetic",
            ),
        }
        for name, (mutation, expected) in mutations.items():
            with self.subTest(name=name):
                self.assert_rejected(mutation, contains=expected)

    def test_protected_and_proxy_field_variants_fail_closed(self) -> None:
        for field in (
            "ethnicity",
            "ethnicity_score",
            "ethnic_score",
            "nationalityEvidence",
            "native_language",
            "race-proxy",
            "racialProxy",
        ):
            with self.subTest(field=field):
                self.assert_rejected(
                    lambda payload, field=field: payload["candidate_packets"][0].__setitem__(field, "synthetic"),
                    contains="prohibited protected/proxy field",
                )

    def test_gold_and_acceptance_metrics_fail_closed(self) -> None:
        bad_gold = copy.deepcopy(self.gold)
        bad_gold["labels"]["xuid_fixture_024"] = "PRETRAIN_CORE"
        bad_gold["labels"]["xuid_fixture_023"] = "PRETRAIN_CORE"
        bad_gold["labels"]["xuid_fixture_022"] = "PRETRAIN_CORE"
        bad_gold["labels"]["xuid_fixture_021"] = "PRETRAIN_CORE"
        bad_gold["relevant_platform_user_ids"] = list(bad_gold["labels"])
        metrics = evaluate_selection(self.fixture, bad_gold)
        errors = validate_acceptance(self.fixture, bad_gold, metrics=metrics)
        self.assertIn("selection recall is below the gold threshold", errors)

        unknown_gold = copy.deepcopy(self.gold)
        unknown_gold["relevant_platform_user_ids"][0] = "missing_account"
        self.assertTrue(validate_acceptance(self.fixture, unknown_gold))

        malformed_gold = copy.deepcopy(self.gold)
        malformed_gold["labels"]["xuid_fixture_001"] = ["PRETRAIN_CORE"]
        self.assertTrue(validate_acceptance(self.fixture, malformed_gold))

        malformed_relevant_gold = copy.deepcopy(self.gold)
        malformed_relevant_gold["relevant_platform_user_ids"] = None
        malformed_metrics = evaluate_selection(self.fixture, malformed_relevant_gold)
        self.assertEqual(malformed_metrics.relevant_count, 0)
        self.assertIn(
            "gold relevant IDs must exactly match in-scope labels",
            validate_acceptance(self.fixture, malformed_relevant_gold, metrics=malformed_metrics),
        )

        weakened_gold = copy.deepcopy(self.gold)
        weakened_gold["labels"] = {account_id: "OUT_OF_SCOPE" for account_id in weakened_gold["labels"]}
        weakened_gold["relevant_platform_user_ids"] = []
        weakened_gold["minimum_precision"] = 0
        weakened_gold["minimum_recall"] = 0
        weakened_metrics = evaluate_selection(self.fixture, weakened_gold)
        weakened_errors = validate_acceptance(self.fixture, weakened_gold, metrics=weakened_metrics)
        self.assertIn("gold labels must exactly match candidate packet relevance", weakened_errors)
        self.assertIn("gold minimum_precision must equal 0.95", weakened_errors)
        self.assertIn("gold minimum_recall must equal 0.9", weakened_errors)

    def test_malformed_collection_arrays_fail_closed_without_exception(self) -> None:
        for field in ("accounts", "candidate_packets"):
            with self.subTest(field=field):
                malformed_fixture = copy.deepcopy(self.fixture)
                malformed_fixture[field] = None
                self.assertTrue(self.validate(malformed_fixture))
                metrics = evaluate_selection(malformed_fixture, self.gold)
                self.assertTrue(validate_acceptance(malformed_fixture, self.gold, metrics=metrics))

    def test_query_registry_caps_shape_and_safety_fail_closed(self) -> None:
        mutations: dict[str, tuple[Callable[[dict[str, Any]], None], str]] = {
            "max pages": (
                lambda registry: registry["families"][0].__setitem__("max_pages", 999),
                "max_pages must equal 2",
            ),
            "max observations": (
                lambda registry: registry["families"][0].__setitem__("max_observations", 999),
                "max_observations must equal 20",
            ),
            "protected purpose": (
                lambda registry: registry["families"][0].__setitem__("purpose", "rank by nationalityEvidence"),
                "prohibited protected/proxy value",
            ),
            "race and gender purpose": (
                lambda registry: registry["families"][0].__setitem__("purpose", "Rank candidates by race/gender"),
                "prohibited protected/proxy value",
            ),
            "country-of-origin purpose": (
                lambda registry: registry["families"][0].__setitem__("purpose", "Rank candidates by country of origin"),
                "purpose does not match the canonical safety-reviewed text",
            ),
            "school-and-community purpose": (
                lambda registry: registry["families"][0].__setitem__(
                    "purpose", "Rank candidates by school and community"
                ),
                "purpose does not match the canonical safety-reviewed text",
            ),
            "private-contact purpose": (
                lambda registry: registry["families"][0].__setitem__(
                    "purpose", "Collect private emails and phone numbers"
                ),
                "purpose does not match the canonical safety-reviewed text",
            ),
        }
        for name, (mutation, expected) in mutations.items():
            with self.subTest(name=name):
                registry = copy.deepcopy(self.queries)
                mutation(registry)
                errors = validate_collection(self.fixture, labs_registry=self.labs, query_registry=registry)
                self.assertTrue(any(expected in error for error in errors), errors)

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
        payload = build_fixture()
        self.assertEqual(self.validate(payload), [])
        self.assertLess(time.monotonic() - started, 60)


if __name__ == "__main__":
    unittest.main()
