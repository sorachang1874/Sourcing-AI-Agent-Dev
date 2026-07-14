from __future__ import annotations

import ast
import copy
import json
import os
import re
import subprocess
import sys
import unittest
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from x_first.profile_bio_signals import (  # noqa: E402
    ANALYSIS_SCHEMA_VERSION,
    BUNDLE_SCHEMA_VERSION,
    CANONICAL_POLICY_SHA256,
    POLICY_SCHEMA_VERSION,
    POLICY_VERSION,
    analyze_profile_bio_signals,
    canonical_sha256,
    load_json,
    text_sha256,
    validate_analysis,
    validate_evidence_bundle,
    validate_policy,
)

FORBIDDEN_RUNTIME_IMPORTS = {
    "aiohttp",
    "grok",
    "httpx",
    "openai",
    "requests",
    "socket",
    "subprocess",
    "urllib.request",
    "urllib3",
    "xai",
}


def _imported_modules(source: str) -> set[str]:
    modules: set[str] = set()
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.Import):
            modules.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            modules.add(node.module)
    return modules


def _forbidden_runtime_imports(source: str) -> set[str]:
    return {
        module
        for module in _imported_modules(source)
        if any(module == forbidden or module.startswith(f"{forbidden}.") for forbidden in FORBIDDEN_RUNTIME_IMPORTS)
    }


def _schema_errors(instance: Any, schema: Any, *, root: dict[str, Any] | None = None, path: str = "$") -> list[str]:
    """Validate the closed Draft 2020-12 subset used by this fixture lane."""

    if root is None and isinstance(schema, dict):
        root = schema
    if not isinstance(schema, dict) or root is None:
        return [f"{path}: invalid schema"]
    if "$ref" in schema:
        target: Any = root
        for token in schema["$ref"][2:].split("/"):
            target = target[token.replace("~1", "/").replace("~0", "~")]
        return _schema_errors(instance, target, root=root, path=path)

    errors: list[str] = []
    if "const" in schema and instance != schema["const"]:
        errors.append(f"{path}: const mismatch")
    if "enum" in schema and instance not in schema["enum"]:
        errors.append(f"{path}: enum mismatch")
    expected_type = schema.get("type")
    allowed = [expected_type] if isinstance(expected_type, str) else expected_type
    checks = {
        "array": lambda value: isinstance(value, list),
        "boolean": lambda value: isinstance(value, bool),
        "integer": lambda value: isinstance(value, int) and not isinstance(value, bool),
        "null": lambda value: value is None,
        "object": lambda value: isinstance(value, dict),
        "string": lambda value: isinstance(value, str),
    }
    if isinstance(allowed, list) and not any(checks[kind](instance) for kind in allowed):
        errors.append(f"{path}: type mismatch")
        return errors

    if isinstance(instance, str):
        if len(instance) < schema.get("minLength", 0):
            errors.append(f"{path}: too short")
        if "maxLength" in schema and len(instance) > schema["maxLength"]:
            errors.append(f"{path}: too long")
        if "pattern" in schema and re.search(schema["pattern"], instance) is None:
            errors.append(f"{path}: pattern mismatch")
        if schema.get("format") == "uri":
            parsed = urlsplit(instance)
            if not parsed.scheme or not parsed.hostname:
                errors.append(f"{path}: invalid URI")

    if isinstance(instance, int) and not isinstance(instance, bool):
        if "minimum" in schema and instance < schema["minimum"]:
            errors.append(f"{path}: below minimum")
        if "maximum" in schema and instance > schema["maximum"]:
            errors.append(f"{path}: above maximum")

    if isinstance(instance, dict):
        for field in schema.get("required", []):
            if field not in instance:
                errors.append(f"{path}: missing {field}")
        properties = schema.get("properties", {})
        for field, value in instance.items():
            if field in properties:
                errors.extend(_schema_errors(value, properties[field], root=root, path=f"{path}.{field}"))
            elif schema.get("additionalProperties") is False:
                errors.append(f"{path}: additional {field}")

    if isinstance(instance, list):
        if len(instance) < schema.get("minItems", 0):
            errors.append(f"{path}: too few items")
        if "maxItems" in schema and len(instance) > schema["maxItems"]:
            errors.append(f"{path}: too many items")
        if schema.get("uniqueItems"):
            canonical = [json.dumps(item, ensure_ascii=True, sort_keys=True) for item in instance]
            if len(canonical) != len(set(canonical)):
                errors.append(f"{path}: duplicate items")
        if "items" in schema:
            for index, item in enumerate(instance):
                errors.extend(_schema_errors(item, schema["items"], root=root, path=f"{path}[{index}]"))
    return errors


class ProfileBioSignalContractTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.policy = load_json(ROOT / "configs/profile_bio_signal_policy.v1.json")
        cls.bundle = load_json(ROOT / "fixtures/profile_bio_evidence_fixture_v1.json")
        cls.analysis = load_json(ROOT / "fixtures/profile_bio_signal_analysis_fixture_v1.json")
        cls.policy_schema = load_json(ROOT / "contracts/x.profile.bio_signal.policy.v1.schema.json")
        cls.bundle_schema = load_json(ROOT / "contracts/x.profile.bio_evidence.bundle.v1.schema.json")
        cls.analysis_schema = load_json(ROOT / "contracts/x.profile.bio_signal.analysis.v1.schema.json")

    def assert_bundle_rejected(self, payload: Any, *, schema_too: bool = False) -> None:
        errors = validate_evidence_bundle(payload, policy=self.policy)
        self.assertTrue(errors, "mutation unexpectedly passed runtime validation")
        if schema_too:
            self.assertTrue(_schema_errors(payload, self.bundle_schema), "mutation unexpectedly passed JSON Schema")

    @staticmethod
    def _single_proposal_bundle(
        base: dict[str, Any],
        proposal_index: int,
        *,
        bio_text: str | None = None,
        excerpt: str | None = None,
    ) -> dict[str, Any]:
        payload = copy.deepcopy(base)
        proposal = copy.deepcopy(payload["proposals"][proposal_index])
        selected_excerpt = proposal["excerpt"] if excerpt is None else excerpt
        selected_bio = selected_excerpt if bio_text is None else bio_text
        proposal["span_start"] = selected_bio.index(selected_excerpt)
        proposal["span_end"] = proposal["span_start"] + len(selected_excerpt)
        proposal["excerpt"] = selected_excerpt
        proposal["excerpt_sha256"] = text_sha256(selected_excerpt)
        payload["profile_snapshot"]["bio_text"] = selected_bio
        payload["profile_snapshot"]["content_sha256"] = text_sha256(selected_bio)
        payload["proposals"] = [proposal]
        return payload

    def test_policy_bundle_analysis_and_schemas_are_closed_and_current(self) -> None:
        self.assertEqual(self.policy["schema_version"], POLICY_SCHEMA_VERSION)
        self.assertEqual(self.policy["policy_version"], POLICY_VERSION)
        self.assertEqual(self.bundle["schema_version"], BUNDLE_SCHEMA_VERSION)
        self.assertEqual(self.analysis["schema_version"], ANALYSIS_SCHEMA_VERSION)
        self.assertEqual(validate_policy(self.policy), [])
        self.assertEqual(canonical_sha256(self.policy), CANONICAL_POLICY_SHA256)
        self.assertEqual(validate_evidence_bundle(self.bundle, policy=self.policy), [])
        self.assertEqual(validate_analysis(self.analysis, evidence_bundle=self.bundle, policy=self.policy), [])
        self.assertEqual(_schema_errors(self.policy, self.policy_schema), [])
        self.assertEqual(_schema_errors(self.bundle, self.bundle_schema), [])
        self.assertEqual(_schema_errors(self.analysis, self.analysis_schema), [])
        self.assertEqual(self.policy_schema["const"], self.policy)
        for schema in (self.policy_schema, self.bundle_schema, self.analysis_schema):
            self.assertIs(schema["additionalProperties"], False)

    def test_policy_version_is_exactly_bound_to_the_closed_registry(self) -> None:
        mutations = []
        alias = copy.deepcopy(self.policy)
        alias["china_ecosystems"][0]["aliases"].append("UnversionedNetwork")
        mutations.append(alias)
        grammar = copy.deepcopy(self.policy)
        grammar["china_ecosystems"][0]["subject_claim_templates"].append("I browse {alias}")
        mutations.append(grammar)
        relation = copy.deepcopy(self.policy)
        relation["affiliation_relations"][1]["markers"].append("前")
        mutations.append(relation)
        limits = copy.deepcopy(self.policy)
        limits["limits"]["max_proposals"] += 1
        mutations.append(limits)
        for payload in mutations:
            with self.subTest(payload=canonical_sha256(payload)):
                self.assertTrue(validate_policy(payload))
                self.assertTrue(_schema_errors(payload, self.policy_schema))

    def test_fixture_recomputes_and_separates_alias_language_ecosystem_and_affiliation(self) -> None:
        self.assertEqual(analyze_profile_bio_signals(self.bundle, policy=self.policy), self.analysis)
        self.assertEqual(
            [signal["signal_type"] for signal in self.analysis["signals"]],
            [
                "observed_chinese_content",
                "china_ecosystem_experience_lead",
                "china_ecosystem_experience_lead",
            ],
        )
        self.assertEqual(
            [item["relation"] for item in self.analysis["affiliation_proposals"]],
            ["current", "previous", "previous"],
        )
        self.assertTrue(
            all(item["resolution_status"] == "unresolved" for item in self.analysis["affiliation_proposals"])
        )
        self.assertTrue(
            all(item["confirmation_status"] == "subject_claim_only" for item in self.analysis["affiliation_proposals"])
        )
        self.assertEqual(self.analysis["experience_leads"]["china_digital_ecosystem"]["status"], "supported")
        self.assertEqual(self.analysis["experience_leads"]["physical_region_experience"]["status"], "not_evaluated")
        self.assertEqual(self.analysis["profile_capability"]["display_alias_role"], "raw_alias_only")
        self.assertEqual(self.analysis["profile_capability"]["stable_account_identity"], "fixture_only_not_live_proven")
        self.assertEqual(self.analysis["profile_capability"]["bio_text"], "fixture_present_not_live_proven")
        self.assertTrue(all(value is False for value in self.analysis["claims"].values()))

    def test_display_alias_changes_no_signal_or_relationship_semantics(self) -> None:
        changed = copy.deepcopy(self.bundle)
        changed["profile_snapshot"]["display_alias"] = "An unrelated pseudonym"
        changed_analysis = analyze_profile_bio_signals(changed, policy=self.policy)
        for field in ("signals", "affiliation_proposals", "graph_edges", "experience_leads", "claims"):
            self.assertEqual(changed_analysis[field], self.analysis[field])
        self.assertNotEqual(changed_analysis["input_sha256"], self.analysis["input_sha256"])
        self.assertNotEqual(changed_analysis["analysis_id"], self.analysis["analysis_id"])

    def test_ecosystem_requires_closed_same_statement_ownership_grammar(self) -> None:
        negatives = (
            "看到小红书行业讨论",
            "小红书用户有很多粉丝",
            "同名个人博客。小红书用户有很多粉丝",
        )
        for excerpt in negatives:
            payload = self._single_proposal_bundle(self.bundle, 4, excerpt=excerpt)
            with self.subTest(excerpt=excerpt):
                self.assert_bundle_rejected(payload)
                errors = validate_evidence_bundle(payload, policy=self.policy)
                self.assertTrue(any("closed same-statement subject-claim grammar" in error for error in errors))

        positive_xiaohongshu = self._single_proposal_bundle(self.bundle, 4, excerpt="同名小红书四万粉丝")
        positive_wechat = self._single_proposal_bundle(
            self.bundle,
            5,
            excerpt="我的公众号 SyntheticFounder（长文首发）",
        )
        self.assertEqual(validate_evidence_bundle(positive_xiaohongshu, policy=self.policy), [])
        self.assertEqual(validate_evidence_bundle(positive_wechat, policy=self.policy), [])

    def test_organization_mentions_are_handle_bound_proposals_not_confirmed_employment(self) -> None:
        missing_marker = self._single_proposal_bundle(
            self.bundle,
            1,
            excerpt="Discussing growth with @synthetic_hub",
        )
        self.assert_bundle_rejected(missing_marker)
        frontier_not_previous = self._single_proposal_bundle(
            self.bundle,
            2,
            excerpt="前沿研究讨论 @synth_listen",
        )
        self.assert_bundle_rejected(frontier_not_previous)
        cross_statement = self._single_proposal_bundle(
            self.bundle,
            2,
            excerpt="Prev role elsewhere。Discussing @synth_listen",
        )
        self.assert_bundle_rejected(cross_statement)
        ambiguous_relation = self._single_proposal_bundle(
            self.bundle,
            2,
            excerpt="Head of @synth_listen, Prev @another_org",
        )
        self.assert_bundle_rejected(ambiguous_relation)
        for excerpt in ("曾任 @synth_listen", "前任职于 @synth_listen"):
            with self.subTest(excerpt=excerpt):
                chinese_previous = self._single_proposal_bundle(self.bundle, 2, excerpt=excerpt)
                self.assertEqual(validate_evidence_bundle(chinese_previous, policy=self.policy), [])
        resolved_without_owner = copy.deepcopy(self.bundle)
        resolved_without_owner["proposals"][1]["details"]["organization_platform_user_id"] = "900000000000000002"
        self.assert_bundle_rejected(resolved_without_owner)
        tampered = copy.deepcopy(self.analysis)
        tampered["affiliation_proposals"][0]["confirmation_status"] = "confirmed"
        self.assertTrue(validate_analysis(tampered, evidence_bundle=self.bundle, policy=self.policy))

    def test_proposal_spans_hashes_source_binding_and_duplicates_fail_closed(self) -> None:
        mutations = []
        stale_span = copy.deepcopy(self.bundle)
        stale_span["proposals"][0]["span_end"] -= 1
        mutations.append(stale_span)
        stale_hash = copy.deepcopy(self.bundle)
        stale_hash["profile_snapshot"]["content_sha256"] = "0" * 64
        mutations.append(stale_hash)
        foreign_subject = copy.deepcopy(self.bundle)
        foreign_subject["profile_snapshot"]["platform_user_id"] = "900000000000000099"
        mutations.append(foreign_subject)
        duplicate = copy.deepcopy(self.bundle)
        duplicate["proposals"].append(copy.deepcopy(duplicate["proposals"][0]))
        mutations.append(duplicate)
        duplicate_affiliation = copy.deepcopy(self.bundle)
        cloned = copy.deepcopy(duplicate_affiliation["proposals"][1])
        cloned["proposal_id"] = "xbp_01J00000000000000000000008"
        duplicate_affiliation["proposals"].append(cloned)
        mutations.append(duplicate_affiliation)
        for payload in mutations:
            with self.subTest(payload=payload["profile_snapshot"].get("content_sha256")):
                self.assert_bundle_rejected(payload)

    def test_sensitive_or_name_fields_and_authority_claims_fail_closed(self) -> None:
        for field in ("real_name", "real-name", "Ｎａｔｉｏｎａｌｉｔｙ", "ethnicity"):
            payload = copy.deepcopy(self.bundle)
            payload["proposals"][0]["details"][field] = "forbidden"
            self.assert_bundle_rejected(payload, schema_too=True)
        name_kind = copy.deepcopy(self.bundle)
        name_kind["proposals"][0]["kind"] = "name_signal"
        self.assert_bundle_rejected(name_kind, schema_too=True)
        authority = copy.deepcopy(self.bundle)
        authority["claims"]["affiliation_confirmed"] = True
        self.assert_bundle_rejected(authority, schema_too=True)

    def test_v1_is_fixture_only_and_rejects_native_relabel_or_handle_mismatch(self) -> None:
        payload = copy.deepcopy(self.bundle)
        payload["profile_snapshot"]["retrieval_mode"] = "grok_x_native"
        payload["profile_snapshot"]["profile_url"] = "https://x.com/other_account"
        payload["profile_snapshot"]["native_profile_receipt_ref"] = "xcall_0123456789abcdef01234567"
        for proposal in payload["proposals"]:
            proposal["extractor"]["mode"] = "grok_x_native"
            proposal["extractor"]["tool_call_id"] = "xcall_0123456789abcdef01234567"
        self.assert_bundle_rejected(payload, schema_too=True)

        mismatched_fixture_url = copy.deepcopy(self.bundle)
        mismatched_fixture_url["profile_snapshot"]["profile_url"] = "https://profiles.invalid/x/other_account"
        self.assert_bundle_rejected(mismatched_fixture_url)

    def test_schema_runtime_mutation_corpus_and_malformed_types_are_terminal_total(self) -> None:
        structural_mutations = []
        extra = copy.deepcopy(self.bundle)
        extra["unexpected"] = True
        structural_mutations.append(extra)
        invalid_id = copy.deepcopy(self.bundle)
        invalid_id["subject"]["provisional_person_id"] = "pp_x_name_or_handle"
        structural_mutations.append(invalid_id)
        bool_span = copy.deepcopy(self.bundle)
        bool_span["proposals"][0]["span_start"] = True
        structural_mutations.append(bool_span)
        empty_proposals = copy.deepcopy(self.bundle)
        empty_proposals["proposals"] = []
        structural_mutations.append(empty_proposals)
        bad_url = copy.deepcopy(self.bundle)
        bad_url["profile_snapshot"]["profile_url"] = "https://user:pass@profiles.invalid:443/x/a?token=x"
        structural_mutations.append(bad_url)
        for payload in structural_mutations:
            self.assert_bundle_rejected(payload, schema_too=True)

        malformed_values: list[Any] = [None, [], "bundle", 1, True, {"schema_version": BUNDLE_SCHEMA_VERSION}]
        for payload in malformed_values:
            with self.subTest(payload=payload):
                errors = validate_evidence_bundle(payload, policy=self.policy)
                self.assertTrue(errors)

        nested_malformed = copy.deepcopy(self.bundle)
        nested_malformed["proposals"] = [None, [], "proposal", 1, True]
        self.assert_bundle_rejected(nested_malformed)

    def test_cli_and_source_boundary_are_offline_and_terminal(self) -> None:
        command = [
            sys.executable,
            "-m",
            "x_first.profile_bio_signals",
            "fixtures/profile_bio_evidence_fixture_v1.json",
            "--policy",
            "configs/profile_bio_signal_policy.v1.json",
        ]
        completed = subprocess.run(
            command,
            cwd=ROOT,
            env={**os.environ, "PYTHONPATH": "src"},
            check=False,
            capture_output=True,
            text=True,
            timeout=10,
        )
        self.assertEqual(completed.returncode, 0, completed.stdout + completed.stderr)
        self.assertEqual(json.loads(completed.stdout), {"errors": [], "status": "valid"})

        source = (ROOT / "src/x_first/profile_bio_signals.py").read_text(encoding="utf-8")
        self.assertEqual(_forbidden_runtime_imports(source), set())
        self.assertNotIn("sourcing_agent", source)

        synthetic_forbidden_imports = """
from httpx import Client
from requests.sessions import Session
from urllib.request import urlopen
import socket
"""
        self.assertEqual(
            _forbidden_runtime_imports(synthetic_forbidden_imports),
            {"httpx", "requests.sessions", "socket", "urllib.request"},
        )


if __name__ == "__main__":
    unittest.main()
