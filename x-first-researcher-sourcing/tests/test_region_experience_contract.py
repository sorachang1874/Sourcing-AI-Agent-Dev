from __future__ import annotations

import ast
import copy
import json
import os
import re
import subprocess
import sys
import time
import tomllib
import unittest
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from x_first.region_experience import (  # noqa: E402
    CLASSIFICATION_SCHEMA_VERSION,
    EVIDENCE_SCHEMA_VERSION,
    EXPECTED_LOCATION_LABELS,
    POLICY_SCHEMA_VERSION,
    POLICY_VERSION,
    SOURCE_MEDIUMS,
    actor_binding_sha256,
    adjudication_sha256,
    canonical_sha256,
    classify_region_experience,
    load_json,
    text_sha256,
    validate_classification,
    validate_evidence_bundle,
    validate_policy,
)


def _json_schema_errors(
    instance: Any, schema: Any, *, root: dict[str, Any] | None = None, path: str = "$"
) -> list[str]:
    """Validate the closed JSON-Schema subset used by the three region contracts."""

    if root is None and isinstance(schema, dict):
        root = schema
    if schema is False:
        return [f"{path}: rejected by false schema"]
    if schema is True or schema == {}:
        return []
    if not isinstance(schema, dict) or root is None:
        return [f"{path}: invalid schema"]
    if "$ref" in schema:
        ref = schema["$ref"]
        if not isinstance(ref, str) or not ref.startswith("#/"):
            return [f"{path}: unsupported ref"]
        target: Any = root
        for token in ref[2:].split("/"):
            target = target[token.replace("~1", "/").replace("~0", "~")]
        return _json_schema_errors(instance, target, root=root, path=path)

    errors: list[str] = []
    if "anyOf" in schema:
        variants = schema["anyOf"]
        if not isinstance(variants, list) or not any(
            not _json_schema_errors(instance, variant, root=root, path=path) for variant in variants
        ):
            errors.append(f"{path}: no anyOf branch matched")
    for child in schema.get("allOf", []):
        errors.extend(_json_schema_errors(instance, child, root=root, path=path))
    if "if" in schema and not _json_schema_errors(instance, schema["if"], root=root, path=path):
        errors.extend(_json_schema_errors(instance, schema.get("then", {}), root=root, path=path))
    if "const" in schema and instance != schema["const"]:
        errors.append(f"{path}: const mismatch")
    if "enum" in schema and instance not in schema["enum"]:
        errors.append(f"{path}: enum mismatch")

    expected_type = schema.get("type")
    allowed_types = [expected_type] if isinstance(expected_type, str) else expected_type
    type_checks = {
        "array": lambda value: isinstance(value, list),
        "boolean": lambda value: isinstance(value, bool),
        "null": lambda value: value is None,
        "object": lambda value: isinstance(value, dict),
        "string": lambda value: isinstance(value, str),
    }
    if isinstance(allowed_types, list) and not any(type_checks[name](instance) for name in allowed_types):
        errors.append(f"{path}: type mismatch")
        return errors

    if isinstance(instance, str):
        if len(instance) < schema.get("minLength", 0):
            errors.append(f"{path}: shorter than minLength")
        if "maxLength" in schema and len(instance) > schema["maxLength"]:
            errors.append(f"{path}: longer than maxLength")
        if "pattern" in schema and re.search(schema["pattern"], instance) is None:
            errors.append(f"{path}: pattern mismatch")
        if schema.get("format") == "date-time":
            try:
                parsed = datetime.fromisoformat(instance.removesuffix("Z") + "+00:00")
            except ValueError:
                errors.append(f"{path}: invalid date-time")
            else:
                if parsed.isoformat(timespec="milliseconds").replace("+00:00", "Z") != instance:
                    errors.append(f"{path}: noncanonical date-time")
        if schema.get("format") == "uri":
            parsed_url = urlsplit(instance)
            if not parsed_url.scheme or not parsed_url.hostname:
                errors.append(f"{path}: invalid URI")

    if isinstance(instance, dict):
        required = schema.get("required", [])
        if isinstance(required, list):
            for field in required:
                if field not in instance:
                    errors.append(f"{path}: missing {field}")
        properties = schema.get("properties", {})
        if isinstance(properties, dict):
            for field, value in instance.items():
                if field in properties:
                    errors.extend(_json_schema_errors(value, properties[field], root=root, path=f"{path}.{field}"))
                elif schema.get("additionalProperties") is False:
                    errors.append(f"{path}: additional property {field}")

    if isinstance(instance, list):
        if len(instance) < schema.get("minItems", 0):
            errors.append(f"{path}: fewer than minItems")
        if "maxItems" in schema and len(instance) > schema["maxItems"]:
            errors.append(f"{path}: more than maxItems")
        if schema.get("uniqueItems"):
            canonical = [json.dumps(value, ensure_ascii=True, sort_keys=True) for value in instance]
            if len(canonical) != len(set(canonical)):
                errors.append(f"{path}: duplicate items")
        prefix_items = schema.get("prefixItems", [])
        if isinstance(prefix_items, list):
            for index, child in enumerate(prefix_items[: len(instance)]):
                errors.extend(_json_schema_errors(instance[index], child, root=root, path=f"{path}[{index}]"))
        item_schema = schema.get("items")
        start = len(prefix_items) if isinstance(prefix_items, list) else 0
        if item_schema is not None:
            for index in range(start, len(instance)):
                errors.extend(_json_schema_errors(instance[index], item_schema, root=root, path=f"{path}[{index}]"))
    return errors


class RegionExperienceContractTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.policy = load_json(ROOT / "configs/region_experience_policy.v1.json")
        cls.evidence = load_json(ROOT / "fixtures/region_experience_evidence_fixture_v1.json")
        cls.result = load_json(ROOT / "fixtures/region_experience_classification_fixture_v1.json")
        cls.policy_schema = load_json(ROOT / "contracts/x.region_experience.policy.v1.schema.json")
        cls.evidence_schema = load_json(ROOT / "contracts/x.region_experience.evidence_bundle.v1.schema.json")
        cls.result_schema = load_json(ROOT / "contracts/x.region_experience.classification.v1.schema.json")

    def classify(self, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        candidate = copy.deepcopy(self.evidence) if payload is None else payload
        return classify_region_experience(candidate, policy=self.policy)

    @staticmethod
    def rehash(item: dict[str, Any]) -> None:
        item["excerpt_sha256"] = text_sha256(item["excerpt"])
        item["content_sha256"] = item["excerpt_sha256"]
        item["actor_binding_sha256"] = actor_binding_sha256(item)
        item["adjudication_sha256"] = adjudication_sha256(item)

    def single_evidence_bundle(self, *, location_code: str, location_text: str) -> dict[str, Any]:
        item = copy.deepcopy(self.evidence["evidence"][0])
        item["location_code"] = location_code
        item["location_text"] = location_text
        item["excerpt"] = f"Synthetic adjudicated source explicitly states experience in {location_text}."
        self.rehash(item)
        payload = copy.deepcopy(self.evidence)
        payload["evidence"] = [item]
        return payload

    def assert_evidence_rejected_by_schema_and_runtime(self, payload: dict[str, Any]) -> None:
        self.assertTrue(_json_schema_errors(payload, self.evidence_schema))
        self.assertTrue(validate_evidence_bundle(payload, policy=self.policy))

    def test_policy_schemas_and_fixtures_are_closed_and_executable(self) -> None:
        self.assertEqual(validate_policy(self.policy), [])
        self.assertEqual(self.policy["schema_version"], POLICY_SCHEMA_VERSION)
        self.assertEqual(self.evidence["schema_version"], EVIDENCE_SCHEMA_VERSION)
        self.assertEqual(self.result["schema_version"], CLASSIFICATION_SCHEMA_VERSION)
        self.assertEqual(self.policy["policy_version"], POLICY_VERSION)
        self.assertEqual(_json_schema_errors(self.policy, self.policy_schema), [])
        self.assertEqual(_json_schema_errors(self.evidence, self.evidence_schema), [])
        self.assertEqual(_json_schema_errors(self.result, self.result_schema), [])
        for schema in (self.policy_schema, self.evidence_schema, self.result_schema):
            self.assertIs(schema["additionalProperties"], False)

    def test_fixture_is_deterministic_bound_provider_neutral_and_action_closed(self) -> None:
        self.assertEqual(validate_evidence_bundle(self.evidence, policy=self.policy), [])
        self.assertEqual(self.classify(), self.result)
        self.assertEqual(validate_classification(self.result, evidence_bundle=self.evidence, policy=self.policy), [])
        self.assertEqual(self.result["input_sha256"], canonical_sha256(self.evidence))
        self.assertEqual(self.result["policy_sha256"], canonical_sha256(self.policy))
        self.assertEqual({item["source_medium"] for item in self.evidence["evidence"]}, set(SOURCE_MEDIUMS))
        for item in self.evidence["evidence"]:
            self.assertTrue(urlsplit(item["source_url"]).hostname.endswith(".invalid"))
        self.assertTrue(all(value is False for value in self.result["claims"].values()))

    def test_every_registered_location_and_alias_has_exact_hierarchy(self) -> None:
        expected_by_code = {code: set(labels) for code, labels in EXPECTED_LOCATION_LABELS.items()}
        locations = {item["location_code"]: item["aliases"] for item in self.policy["locations"]}
        self.assertEqual(set(locations), set(expected_by_code))
        label_codes = {item["label"]: set(item["location_codes"]) for item in self.policy["labels"]}
        self.assertEqual(
            label_codes["GREATER_CHINA_EXPERIENCE"],
            {"mainland_china", "hong_kong", "macau", "taiwan"},
        )
        self.assertEqual(label_codes["MAINLAND_CHINA_EXPERIENCE"], {"mainland_china"})
        self.assertIn("singapore", label_codes["ASIA_EXPERIENCE"])
        self.assertNotIn("singapore", label_codes["GREATER_CHINA_EXPERIENCE"])
        for location_code, aliases in locations.items():
            for alias in aliases:
                with self.subTest(location_code=location_code, alias=alias):
                    result = self.classify(
                        self.single_evidence_bundle(location_code=location_code, location_text=alias)
                    )
                    supported = {item["label"] for item in result["labels"] if item["status"] == "supported"}
                    self.assertEqual(supported, expected_by_code[location_code])
        singapore = self.classify(self.single_evidence_bundle(location_code="singapore", location_text="Singapore"))
        self.assertEqual(
            {item["label"] for item in singapore["labels"] if item["status"] == "supported"},
            {"ASIA_EXPERIENCE"},
        )

    def test_supported_insufficient_and_unregistered_statuses_are_distinct(self) -> None:
        empty = copy.deepcopy(self.evidence)
        empty["evidence"] = []
        empty_result = self.classify(empty)
        self.assertTrue(all(item["status"] == "insufficient_evidence" for item in empty_result["labels"]))

        unregistered = self.single_evidence_bundle(location_code="unregistered", location_text="Ho Chi Minh City")
        unregistered_result = self.classify(unregistered)
        self.assertTrue(all(item["status"] == "unregistered_location" for item in unregistered_result["labels"]))
        self.assertTrue(all(item["unregistered_evidence_refs"] for item in unregistered_result["labels"]))

        singapore = self.classify(self.single_evidence_bundle(location_code="singapore", location_text="Singapore"))
        by_label = {item["label"]: item for item in singapore["labels"]}
        self.assertEqual(by_label["ASIA_EXPERIENCE"]["status"], "supported")
        self.assertEqual(by_label["GREATER_CHINA_EXPERIENCE"]["status"], "insufficient_evidence")
        self.assertEqual(by_label["MAINLAND_CHINA_EXPERIENCE"]["status"], "insufficient_evidence")

    def test_fixture_evidence_refs_decisions_and_unregistered_refs_are_complete(self) -> None:
        decisions = {item["evidence_id"]: item for item in self.result["evidence_decisions"]}
        eligible = {key for key, item in decisions.items() if item["status"] == "eligible"}
        rejected = {key for key, item in decisions.items() if item["status"] == "rejected"}
        unregistered = {key for key, item in decisions.items() if item["status"] == "unregistered_location"}
        self.assertEqual(len(eligible), 5)
        self.assertEqual(len(rejected), 2)
        self.assertEqual(len(unregistered), 1)
        labels = {item["label"]: item for item in self.result["labels"]}
        self.assertEqual(set(labels["ASIA_EXPERIENCE"]["evidence_refs"]), eligible)
        self.assertNotIn(
            self.evidence["evidence"][0]["evidence_id"], labels["GREATER_CHINA_EXPERIENCE"]["evidence_refs"]
        )
        self.assertEqual(
            set(labels["MAINLAND_CHINA_EXPERIENCE"]["evidence_refs"]),
            {self.evidence["evidence"][1]["evidence_id"]},
        )
        for label in labels.values():
            self.assertEqual(set(label["unregistered_evidence_refs"]), unregistered)
            self.assertTrue(rejected.isdisjoint(label["evidence_refs"]))

    def test_actor_identity_and_binding_proofs_fail_closed(self) -> None:
        mutations = (
            lambda item: item.__setitem__("source_author_platform_user_id", "999999999999999999"),
            lambda item: item.__setitem__("actor_subject_relation", "unrelated_or_unknown"),
            lambda item: item.__setitem__("publisher_binding_status", "unverified"),
            lambda item: item.__setitem__("publisher_binding_ref", "xbind_01J00000000000000000000999"),
        )
        for mutation in mutations:
            with self.subTest(mutation=mutation):
                payload = self.single_evidence_bundle(location_code="singapore", location_text="Singapore")
                mutation(payload["evidence"][0])
                self.assertTrue(validate_evidence_bundle(payload, policy=self.policy))

        organization = copy.deepcopy(self.evidence)
        organization["evidence"] = [copy.deepcopy(self.evidence["evidence"][1])]
        organization["evidence"][0]["publisher_binding_ref"] = None
        self.assertTrue(validate_evidence_bundle(organization, policy=self.policy))

        third_party = copy.deepcopy(self.evidence)
        third_party["evidence"] = [copy.deepcopy(self.evidence["evidence"][6])]
        third_party["evidence"][0]["eligibility_status"] = "eligible"
        third_party["evidence"][0]["statement_scope"] = "subject_explicit_experience"
        self.assertTrue(validate_evidence_bundle(third_party, policy=self.policy))

    def test_source_medium_publisher_and_experience_are_decoupled(self) -> None:
        organization_bio = copy.deepcopy(self.evidence)
        item = copy.deepcopy(self.evidence["evidence"][1])
        item.update(
            {
                "source_medium": "profile_bio",
                "experience_kind": "worked_in",
                "source_url": "https://openai.com/research/synthetic-profile",
                "location_code": "mainland_china",
                "location_text": "北京",
                "excerpt": "Synthetic verified organization Bio states that the subject worked in 北京.",
            }
        )
        self.rehash(item)
        organization_bio["evidence"] = [item]
        self.assertEqual(validate_evidence_bundle(organization_bio, policy=self.policy), [])
        self.assertEqual(self.classify(organization_bio)["evidence_decisions"][0]["status"], "eligible")

        self_bio = self.single_evidence_bundle(location_code="japan", location_text="Tokyo")
        self_bio["evidence"][0]["source_medium"] = "profile_bio"
        self_bio["evidence"][0]["experience_kind"] = "studied_in"
        self.rehash(self_bio["evidence"][0])
        self.assertEqual(validate_evidence_bundle(self_bio, policy=self.policy), [])
        for medium in SOURCE_MEDIUMS:
            with self.subTest(medium=medium):
                candidate = copy.deepcopy(self_bio)
                candidate["evidence"][0]["source_medium"] = medium
                self.rehash(candidate["evidence"][0])
                self.assertEqual(validate_evidence_bundle(candidate, policy=self.policy), [])

    def test_lifecycle_is_terminal_total_and_only_adjudicated_eligible_supports(self) -> None:
        pending = self.single_evidence_bundle(location_code="mainland_china", location_text="Beijing")
        pending["evidence"][0].update(
            {
                "adjudication_status": "pending",
                "eligibility_status": "unresolved",
                "adjudication_ref": None,
                "adjudicated_at": None,
            }
        )
        self.rehash(pending["evidence"][0])
        self.assertEqual(validate_evidence_bundle(pending, policy=self.policy), [])
        result = self.classify(pending)
        self.assertEqual(result["evidence_decisions"][0]["status"], "insufficient_evidence")
        self.assertTrue(all(item["status"] == "insufficient_evidence" for item in result["labels"]))

        invalid_states = (
            {"adjudication_status": "pending", "eligibility_status": "eligible"},
            {"adjudication_status": "adjudicated", "eligibility_status": "unresolved"},
            {"adjudication_status": "adjudicated", "adjudication_ref": None},
            {"eligibility_status": "eligible", "statement_scope": "location_topic_only"},
        )
        for changes in invalid_states:
            with self.subTest(changes=changes):
                candidate = self.single_evidence_bundle(location_code="mainland_china", location_text="Beijing")
                candidate["evidence"][0].update(changes)
                self.assertTrue(validate_evidence_bundle(candidate, policy=self.policy))

    def test_source_record_excerpt_and_hash_closure(self) -> None:
        changed_excerpt = self.single_evidence_bundle(location_code="japan", location_text="Tokyo")
        changed_excerpt["evidence"][0]["excerpt"] = "Changed without updating the excerpt hash."
        self.assertTrue(validate_evidence_bundle(changed_excerpt, policy=self.policy))

        duplicate = self.single_evidence_bundle(location_code="japan", location_text="Tokyo")
        second = copy.deepcopy(duplicate["evidence"][0])
        second["evidence_id"] = "xev_01J00000000000000000000999"
        second["content_sha256"] = "f" * 64
        duplicate["evidence"].append(second)
        self.assertTrue(validate_evidence_bundle(duplicate, policy=self.policy))

        result = copy.deepcopy(self.result)
        for mutation in (
            lambda value: value.__setitem__("input_sha256", "0" * 64),
            lambda value: value.__setitem__("policy_sha256", "0" * 64),
            lambda value: value["labels"][0]["evidence_refs"].append(value["evidence_decisions"][-1]["evidence_id"]),
            lambda value: value["claims"].__setitem__("discovery_or_ranking_authorized", True),
        ):
            mutated = copy.deepcopy(result)
            mutation(mutated)
            self.assertTrue(validate_classification(mutated, evidence_bundle=self.evidence, policy=self.policy))

    def test_opaque_identity_and_fixed_length_ids_reject_semantic_or_handle_values(self) -> None:
        mutations = (
            lambda payload: payload.__setitem__("subject_ref", "pp_x_ALICE_CHINESE_RESEARCHER00"),
            lambda payload: payload.__setitem__("subject_platform_user_id", "@synthetic"),
            lambda payload: payload["evidence"][0].__setitem__("evidence_id", "xev_beijing_job"),
            lambda payload: payload["evidence"][0].__setitem__("source_record_id", "xsrc_profile_bio"),
            lambda payload: payload["evidence"][0].__setitem__("source_author_platform_user_id", "xuid_123"),
        )
        for mutation in mutations:
            with self.subTest(mutation=mutation):
                payload = self.single_evidence_bundle(location_code="singapore", location_text="Singapore")
                mutation(payload)
                if payload["subject_ref"] != self.evidence["subject_ref"]:
                    payload["evidence"][0]["subject_ref"] = payload["subject_ref"]
                if payload["subject_platform_user_id"] != self.evidence["subject_platform_user_id"]:
                    payload["evidence"][0]["subject_platform_user_id"] = payload["subject_platform_user_id"]
                self.assertTrue(validate_evidence_bundle(payload, policy=self.policy))

    def test_public_source_url_contract_rejects_private_noncanonical_and_credential_urls(self) -> None:
        valid_cases = (
            ("profile_bio", "https://x.com/synthetic001"),
            ("post", "https://x.com/synthetic001/status/1234567890"),
            ("profile_bio", "https://openai.com/research/synthetic-profile"),
            ("profile_bio", "https://profiles.invalid/region-fixture/profile"),
        )
        for medium, url in valid_cases:
            with self.subTest(url=url):
                payload = self.single_evidence_bundle(location_code="singapore", location_text="Singapore")
                payload["evidence"][0]["source_medium"] = medium
                payload["evidence"][0]["source_url"] = url
                self.rehash(payload["evidence"][0])
                self.assertEqual(validate_evidence_bundle(payload, policy=self.policy), [])
                self.assertEqual(_json_schema_errors(payload, self.evidence_schema), [])

        invalid_urls = (
            "http://x.com/synthetic001",
            "https://X.COM/synthetic001",
            "https://x.com/synthetic001?token=secret",
            "https://x.com/synthetic001#fragment",
            "https://user:secret@x.com/synthetic001",
            "https://localhost/profile",
            "https://127.0.0.1/profile",
            "https://unknown.example/profile",
            "https://x.com/not/a/canonical/profile/path",
            "https://profiles.invalid/" + "x" * 257,
        )
        for url in invalid_urls:
            with self.subTest(url=url):
                payload = self.single_evidence_bundle(location_code="singapore", location_text="Singapore")
                payload["evidence"][0]["source_url"] = url
                self.assert_evidence_rejected_by_schema_and_runtime(payload)

    def test_alias_nfkc_casefold_whitespace_zero_width_and_confusable_boundaries(self) -> None:
        accepted = ("ＳＩＮＧＡＰＯＲＥ", "sInGaPoRe", "  Singapore  ", "Hong  Kong", "北京")
        for alias in accepted:
            if alias == "北京":
                location_code = "mainland_china"
            elif alias == "Hong  Kong":
                location_code = "hong_kong"
            else:
                location_code = "singapore"
            payload = self.single_evidence_bundle(location_code=location_code, location_text=alias)
            self.assertEqual(validate_evidence_bundle(payload, policy=self.policy), [])

        rejected = ("Singaporean", "Chinatown", "Mainland-inspired", "Singap\u200bore", "Singapоre")
        for alias in rejected:
            with self.subTest(alias=alias):
                payload = self.single_evidence_bundle(location_code="singapore", location_text=alias)
                self.assertTrue(validate_evidence_bundle(payload, policy=self.policy))

        registered_as_unregistered = self.single_evidence_bundle(
            location_code="unregistered", location_text="Singapore"
        )
        self.assertTrue(validate_evidence_bundle(registered_as_unregistered, policy=self.policy))

    def test_protected_and_proxy_field_variants_fail_closed_but_content_is_not_identity_inference(self) -> None:
        for field in ("name", "Ｎａｍｅ", "display-name", "nationality", "language", "socialGraph"):
            with self.subTest(field=field):
                payload = self.single_evidence_bundle(location_code="japan", location_text="Tokyo")
                payload["evidence"][0][field] = "synthetic-untrusted"
                self.assertTrue(validate_evidence_bundle(payload, policy=self.policy))
                with self.assertRaisesRegex(ValueError, "region evidence bundle is invalid"):
                    self.classify(payload)

        chinese_topic = copy.deepcopy(self.evidence)
        item = copy.deepcopy(self.evidence["evidence"][5])
        item["excerpt"] = "我在中文帖子中讨论中国的研究，但没有声明本人在当地工作、学习、居住或研究。"
        self.rehash(item)
        chinese_topic["evidence"] = [item]
        result = self.classify(chinese_topic)
        self.assertEqual(result["evidence_decisions"][0]["status"], "rejected")
        self.assertTrue(all(label["status"] == "insufficient_evidence" for label in result["labels"]))

    def test_all_malformed_json_types_are_terminal_total(self) -> None:
        fields = (
            "evidence_id",
            "source_record_id",
            "subject_ref",
            "subject_platform_user_id",
            "source_author_platform_user_id",
            "source_medium",
            "publisher_actor",
            "actor_subject_relation",
            "publisher_binding_status",
            "actor_binding_sha256",
            "statement_scope",
            "experience_kind",
            "location_code",
            "location_text",
            "source_url",
            "observed_at",
            "content_sha256",
            "content_version",
            "excerpt",
            "excerpt_sha256",
            "extraction_status",
            "proposal_status",
            "adjudication_status",
            "eligibility_status",
            "adjudication_ref",
            "adjudicated_at",
            "adjudication_sha256",
        )
        for field in fields:
            with self.subTest(field=field):
                payload = self.single_evidence_bundle(location_code="singapore", location_text="Singapore")
                payload["evidence"][0][field] = []
                errors = validate_evidence_bundle(payload, policy=self.policy)
                self.assertTrue(errors)
                with self.assertRaisesRegex(ValueError, "region evidence bundle is invalid"):
                    self.classify(payload)
        for malformed in (None, [], "payload", 1):
            with self.subTest(malformed=malformed):
                self.assertTrue(validate_evidence_bundle(malformed, policy=self.policy))
                self.assertTrue(validate_policy(malformed))

    def test_schema_runtime_mutation_corpus_closes_duplicates_hierarchy_url_time_and_empty_text(self) -> None:
        evidence_mutations = (
            lambda payload: payload["evidence"].append(copy.deepcopy(payload["evidence"][0])),
            lambda payload: payload["evidence"][0].__setitem__(
                "source_url", "https://profiles.invalid/profile?token=secret"
            ),
            lambda payload: payload["evidence"][0].__setitem__("observed_at", "2026-02-31T00:00:00.000Z"),
            lambda payload: payload["evidence"][0].__setitem__("excerpt", " "),
        )
        for mutation in evidence_mutations:
            with self.subTest(mutation=mutation):
                payload = self.single_evidence_bundle(location_code="singapore", location_text="Singapore")
                mutation(payload)
                self.assert_evidence_rejected_by_schema_and_runtime(payload)

        policy_mutations = (
            lambda policy: policy["labels"].append(copy.deepcopy(policy["labels"][0])),
            lambda policy: policy["labels"][1]["location_codes"].append("singapore"),
            lambda policy: policy["locations"][0]["aliases"].append("unreviewed-alias"),
            lambda policy: policy["forbidden_input_fields"].append("name"),
        )
        for mutation in policy_mutations:
            with self.subTest(mutation=mutation):
                policy = copy.deepcopy(self.policy)
                mutation(policy)
                self.assertTrue(_json_schema_errors(policy, self.policy_schema))
                self.assertTrue(validate_policy(policy))

        result = copy.deepcopy(self.result)
        result["labels"] = [copy.deepcopy(result["labels"][0]) for _ in range(3)]
        self.assertTrue(_json_schema_errors(result, self.result_schema))
        self.assertTrue(validate_classification(result, evidence_bundle=self.evidence, policy=self.policy))

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
