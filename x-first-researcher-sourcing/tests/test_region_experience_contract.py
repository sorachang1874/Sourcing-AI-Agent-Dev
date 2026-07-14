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
from pathlib import Path
from urllib.parse import urlsplit

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from x_first.region_experience import (  # noqa: E402
    CLASSIFICATION_SCHEMA_VERSION,
    EVIDENCE_SCHEMA_VERSION,
    LABELS,
    POLICY_SCHEMA_VERSION,
    POLICY_VERSION,
    SOURCE_KINDS,
    canonical_sha256,
    classify_region_experience,
    load_json,
    validate_classification,
    validate_evidence_bundle,
    validate_policy,
)


class RegionExperienceContractTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.policy = load_json(ROOT / "configs/region_experience_policy.v1.json")
        cls.evidence = load_json(ROOT / "fixtures/region_experience_evidence_fixture_v1.json")
        cls.result = load_json(ROOT / "fixtures/region_experience_classification_fixture_v1.json")
        cls.policy_schema = load_json(ROOT / "contracts/x.region_experience.policy.v1.schema.json")
        cls.evidence_schema = load_json(ROOT / "contracts/x.region_experience.evidence_bundle.v1.schema.json")
        cls.result_schema = load_json(ROOT / "contracts/x.region_experience.classification.v1.schema.json")

    def classify(self, payload: dict | None = None) -> dict:
        return classify_region_experience(payload or copy.deepcopy(self.evidence), policy=self.policy)

    def single_evidence_bundle(self, *, location_code: str, location_text: str) -> dict:
        item = copy.deepcopy(self.evidence["evidence"][0])
        item.update(
            {
                "evidence_id": "xregion_ev_single",
                "source_kind": "bio",
                "source_actor": "subject_self",
                "statement_scope": "subject_explicit_experience",
                "experience_kind": "based_in",
                "location_code": location_code,
                "location_text": location_text,
            }
        )
        payload = copy.deepcopy(self.evidence)
        payload["evidence"] = [item]
        return payload

    def test_policy_and_json_schemas_are_closed(self) -> None:
        self.assertEqual(validate_policy(self.policy), [])
        self.assertEqual(self.policy["schema_version"], POLICY_SCHEMA_VERSION)
        self.assertEqual(self.evidence["schema_version"], EVIDENCE_SCHEMA_VERSION)
        self.assertEqual(self.result["schema_version"], CLASSIFICATION_SCHEMA_VERSION)
        self.assertEqual(self.policy["policy_version"], POLICY_VERSION)
        for schema in (self.policy_schema, self.evidence_schema, self.result_schema):
            self.assertIs(schema["additionalProperties"], False)
        self.assertIs(self.policy_schema["$defs"]["label"]["additionalProperties"], False)
        self.assertIs(self.policy_schema["$defs"]["location"]["additionalProperties"], False)
        self.assertIs(self.policy_schema["$defs"]["source_rule"]["additionalProperties"], False)
        self.assertIs(self.evidence_schema["$defs"]["evidence"]["additionalProperties"], False)
        for name in ("claims", "evidence_decision", "label"):
            self.assertIs(self.result_schema["$defs"][name]["additionalProperties"], False)
        self.assertEqual(self.result_schema["properties"]["labels"]["minItems"], 3)
        self.assertEqual(self.result_schema["properties"]["labels"]["maxItems"], 3)

    def test_synthetic_fixture_is_deterministic_bound_and_provider_neutral(self) -> None:
        self.assertEqual(validate_evidence_bundle(self.evidence, policy=self.policy), [])
        self.assertEqual(self.classify(), self.result)
        self.assertEqual(
            validate_classification(self.result, evidence_bundle=self.evidence, policy=self.policy),
            [],
        )
        self.assertEqual(self.result["input_sha256"], canonical_sha256(self.evidence))
        self.assertEqual({item["source_kind"] for item in self.evidence["evidence"]}, SOURCE_KINDS)
        for item in self.evidence["evidence"]:
            self.assertTrue(urlsplit(item["source_url"]).hostname.endswith(".invalid"))
        self.assertTrue(all(value is False for value in self.result["claims"].values()))

    def test_location_hierarchy_is_exact(self) -> None:
        cases = (
            ("mainland_china", "Beijing", set(LABELS)),
            ("hong_kong", "Hong Kong", {"ASIA_EXPERIENCE", "GREATER_CHINA_EXPERIENCE"}),
            ("macau", "Macau", {"ASIA_EXPERIENCE", "GREATER_CHINA_EXPERIENCE"}),
            ("taiwan", "Taipei", {"ASIA_EXPERIENCE", "GREATER_CHINA_EXPERIENCE"}),
            ("singapore", "Singapore", {"ASIA_EXPERIENCE", "GREATER_CHINA_EXPERIENCE"}),
            ("japan", "Tokyo", {"ASIA_EXPERIENCE"}),
        )
        for location_code, location_text, supported in cases:
            with self.subTest(location_code=location_code):
                result = self.classify(
                    self.single_evidence_bundle(location_code=location_code, location_text=location_text)
                )
                by_label = {item["label"]: item for item in result["labels"]}
                self.assertEqual(
                    {label for label, item in by_label.items() if item["status"] == "supported"},
                    supported,
                )
                for label, item in by_label.items():
                    self.assertEqual(item["evidence_refs"], ["xregion_ev_single"] if label in supported else [])

    def test_all_supported_labels_have_exact_eligible_evidence_refs(self) -> None:
        eligible = {item["evidence_id"] for item in self.result["evidence_decisions"] if item["status"] == "eligible"}
        label_refs = {item["label"]: set(item["evidence_refs"]) for item in self.result["labels"]}
        self.assertEqual(label_refs["MAINLAND_CHINA_EXPERIENCE"], {"xregion_ev_employment_beijing"})
        self.assertEqual(
            label_refs["GREATER_CHINA_EXPERIENCE"],
            {
                "xregion_ev_bio_singapore",
                "xregion_ev_employment_beijing",
                "xregion_ev_research_hong_kong",
                "xregion_ev_post_taipei_self",
            },
        )
        self.assertEqual(label_refs["ASIA_EXPERIENCE"], eligible)
        rejected = {item["evidence_id"] for item in self.result["evidence_decisions"] if item["status"] == "rejected"}
        self.assertTrue(rejected.isdisjoint(set().union(*label_refs.values())))

    def test_topic_posts_third_party_mentions_and_wrong_actor_never_support_labels(self) -> None:
        topic_only = self.single_evidence_bundle(location_code="mainland_china", location_text="Mainland China")
        topic_only["evidence"][0]["source_kind"] = "post"
        topic_only["evidence"][0]["experience_kind"] = "researched_in"
        topic_only["evidence"][0]["statement_scope"] = "location_topic_only"
        topic_result = self.classify(topic_only)
        self.assertEqual(topic_result["evidence_decisions"][0]["reason"], "rejected_non_subject_scope")
        self.assertTrue(all(item["status"] == "not_supported" for item in topic_result["labels"]))

        mentioned = copy.deepcopy(topic_only)
        mentioned["evidence"][0]["statement_scope"] = "third_party_mention"
        mentioned["evidence"][0]["source_actor"] = "third_party"
        mention_result = self.classify(mentioned)
        self.assertEqual(mention_result["evidence_decisions"][0]["reason"], "rejected_non_subject_scope")
        self.assertTrue(all(item["evidence_refs"] == [] for item in mention_result["labels"]))

        wrong_actor = copy.deepcopy(topic_only)
        wrong_actor["evidence"][0]["statement_scope"] = "subject_explicit_experience"
        wrong_actor["evidence"][0]["source_actor"] = "verified_organization"
        wrong_actor_result = self.classify(wrong_actor)
        self.assertEqual(wrong_actor_result["evidence_decisions"][0]["reason"], "rejected_source_actor")

    def test_exact_alias_matching_prevents_substring_false_positives(self) -> None:
        for misleading_text in ("Chinatown", "Mainland-inspired", "Singaporean", "China"):
            with self.subTest(location_text=misleading_text):
                payload = self.single_evidence_bundle(
                    location_code="mainland_china",
                    location_text=misleading_text,
                )
                result = self.classify(payload)
                self.assertEqual(result["evidence_decisions"][0]["reason"], "rejected_location_alias")
                self.assertTrue(all(item["evidence_refs"] == [] for item in result["labels"]))

    def test_name_handle_language_and_graph_fields_fail_closed(self) -> None:
        for field in (
            "name",
            "display_name",
            "handle",
            "current_handle",
            "language",
            "languages",
            "social_graph",
            "nationality",
            "ethnicity",
        ):
            with self.subTest(field=field):
                payload = copy.deepcopy(self.evidence)
                payload["evidence"][0][field] = "synthetic-untrusted"
                errors = validate_evidence_bundle(payload, policy=self.policy)
                self.assertTrue(errors)
                self.assertTrue(any("protected or proxy input field is forbidden" in error for error in errors))
                with self.assertRaisesRegex(ValueError, "region evidence bundle is invalid"):
                    classify_region_experience(payload, policy=self.policy)

    def test_excerpt_content_is_not_a_classifier_feature(self) -> None:
        baseline = self.single_evidence_bundle(location_code="japan", location_text="Tokyo")
        mutated = copy.deepcopy(baseline)
        mutated["evidence"][0]["excerpt"] = (
            "Synthetic text includes a name, handle-like token, language claim, and a discussion of Mainland China; "
            "the classifier must use only the closed structured evidence fields."
        )
        baseline_result = self.classify(baseline)
        mutated_result = self.classify(mutated)
        self.assertEqual(mutated_result["labels"], baseline_result["labels"])
        self.assertEqual(mutated_result["evidence_decisions"], baseline_result["evidence_decisions"])
        self.assertNotEqual(mutated_result["input_sha256"], baseline_result["input_sha256"])

    def test_school_names_are_not_location_aliases(self) -> None:
        location_aliases = {alias.casefold() for location in self.policy["locations"] for alias in location["aliases"]}
        self.assertFalse(
            any("university" in alias or "college" in alias or "school" in alias for alias in location_aliases)
        )
        payload = self.single_evidence_bundle(location_code="mainland_china", location_text="Peking University")
        payload["evidence"][0]["source_kind"] = "education"
        payload["evidence"][0]["experience_kind"] = "studied_in"
        result = self.classify(payload)
        self.assertEqual(result["evidence_decisions"][0]["reason"], "rejected_location_alias")

    def test_result_hash_claims_and_evidence_refs_fail_closed(self) -> None:
        mutations = (
            lambda result: result.__setitem__("input_sha256", "0" * 64),
            lambda result: result["claims"].__setitem__("nationality_inferred", True),
            lambda result: result["labels"][0]["evidence_refs"].append("xregion_ev_post_topic_only"),
            lambda result: result["labels"][2].__setitem__("status", "not_supported"),
            lambda result: result["evidence_decisions"][0].__setitem__("status", "rejected"),
        )
        for mutation in mutations:
            with self.subTest(mutation=mutation):
                result = copy.deepcopy(self.result)
                mutation(result)
                self.assertTrue(validate_classification(result, evidence_bundle=self.evidence, policy=self.policy))

    def test_bundle_shape_identity_url_timestamp_and_limits_fail_closed(self) -> None:
        mutations = (
            lambda payload: payload.__setitem__("subject_ref", "person-name"),
            lambda payload: payload["evidence"][0].__setitem__("subject_ref", "xregion_subject_other"),
            lambda payload: payload["evidence"][1].__setitem__("evidence_id", payload["evidence"][0]["evidence_id"]),
            lambda payload: payload["evidence"][0].__setitem__("source_url", "http://evidence.invalid/item"),
            lambda payload: payload["evidence"][0].__setitem__("observed_at", "2026-07-14T00:00:00Z"),
            lambda payload: payload["evidence"][0].__setitem__("excerpt", "x" * 281),
        )
        for mutation in mutations:
            with self.subTest(mutation=mutation):
                payload = copy.deepcopy(self.evidence)
                mutation(payload)
                self.assertTrue(validate_evidence_bundle(payload, policy=self.policy))

    def test_policy_mutations_fail_closed(self) -> None:
        mutations = (
            lambda policy: policy["locations"][2]["aliases"].append("China"),
            lambda policy: policy["source_rules"][4]["source_actors"].append("third_party"),
            lambda policy: policy["labels"][2]["location_codes"].append("singapore"),
            lambda policy: policy["forbidden_input_fields"].remove("language"),
        )
        for mutation in mutations:
            with self.subTest(mutation=mutation):
                policy = copy.deepcopy(self.policy)
                mutation(policy)
                self.assertTrue(validate_policy(policy))

    def test_cli_source_boundary_and_runtime(self) -> None:
        completed = subprocess.run(
            [sys.executable, "-m", "x_first.region_experience"],
            cwd=ROOT,
            env={**os.environ, "PYTHONPATH": "src"},
            check=False,
            capture_output=True,
            text=True,
            timeout=60,
        )
        self.assertEqual(completed.returncode, 0, completed.stdout + completed.stderr)
        self.assertEqual(json.loads(completed.stdout), {"errors": [], "status": "valid"})

        module_path = ROOT / "src/x_first/region_experience.py"
        tree = ast.parse(module_path.read_text(encoding="utf-8"), filename=str(module_path))
        imported: set[str] = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imported.update(alias.name.split(".")[0] for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module:
                imported.add(node.module.split(".")[0])
        self.assertFalse(imported & {"sourcing_agent", "requests", "httpx", "aiohttp", "anthropic", "openai"})
        pyproject = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
        self.assertEqual(pyproject["project"]["dependencies"], [])
        started = time.monotonic()
        self.assertEqual(self.classify(), self.result)
        self.assertLess(time.monotonic() - started, 60)


if __name__ == "__main__":
    unittest.main()
