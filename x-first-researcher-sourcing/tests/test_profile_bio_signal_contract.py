from __future__ import annotations

import ast
import copy
import json
import os
import re
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from typing import Any
from unittest import mock
from urllib.parse import urlsplit

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from x_first import profile_bio_signals  # noqa: E402
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
            modules.update(f"{node.module}.{alias.name}" for alias in node.names if alias.name != "*")
    return modules


def _forbidden_runtime_imports(source: str) -> set[str]:
    return {
        forbidden
        for module in _imported_modules(source)
        for forbidden in FORBIDDEN_RUNTIME_IMPORTS
        if module == forbidden or module.startswith(f"{forbidden}.")
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
        form_mapping = copy.deepcopy(self.policy)
        form_mapping["china_ecosystems"][0]["claim_form_ids"].append("unversioned_form")
        mutations.append(form_mapping)
        bare_continuation = copy.deepcopy(self.policy)
        bare_continuation["china_ecosystems"][0]["bare_alias_continuation_ids"] = ["account_identifier"]
        mutations.append(bare_continuation)
        relation = copy.deepcopy(self.policy)
        relation["positive_grammar_manifest"]["affiliation"]["relations"][1]["grammars"][0]["pattern"] = ".*"
        mutations.append(relation)
        ownership_mode = copy.deepcopy(self.policy)
        ownership_mode["positive_grammar_manifest"]["ownership"]["mode"] = "prefix_match"
        mutations.append(ownership_mode)
        ownership_continuation = copy.deepcopy(self.policy)
        ownership_continuation["positive_grammar_manifest"]["ownership"]["claim_forms"][7]["continuation_ids"].append(
            "account_identifier"
        )
        mutations.append(ownership_continuation)
        parenthetical_note = copy.deepcopy(self.policy)
        parenthetical_note["positive_grammar_manifest"]["ownership"]["continuations"][0]["note_values"].append(
            "unversioned note"
        )
        mutations.append(parenthetical_note)
        unicode_category = copy.deepcopy(self.policy)
        unicode_category["positive_grammar_manifest"]["rejected_unicode_categories"].append("Co")
        mutations.append(unicode_category)
        implementation_digest = copy.deepcopy(self.policy)
        implementation_digest["grammar_runtime_binding"]["implementation_sha256"] = "0" * 64
        mutations.append(implementation_digest)
        limits = copy.deepcopy(self.policy)
        limits["limits"]["max_proposals"] += 1
        mutations.append(limits)
        traversal_budget = copy.deepcopy(self.policy)
        traversal_budget["limits"]["max_nested_validation_depth"] += 1
        mutations.append(traversal_budget)
        for payload in mutations:
            with self.subTest(payload=canonical_sha256(payload)):
                self.assertTrue(validate_policy(payload))
                self.assertTrue(_schema_errors(payload, self.policy_schema))

    def test_policy_rejects_loaded_grammar_implementation_drift(self) -> None:
        with mock.patch.object(
            profile_bio_signals,
            "_match_affiliation_positive_grammar",
            return_value=(True, None),
        ):
            policy_errors = validate_policy(self.policy)
            self.assertTrue(any("loaded grammar interpreter" in error for error in policy_errors))
            bundle_errors = validate_evidence_bundle(self.bundle, policy=self.policy)
            self.assertTrue(any("loaded grammar interpreter" in error for error in bundle_errors))

    def test_fixture_recomputes_and_separates_alias_language_ecosystem_and_affiliation(self) -> None:
        self.assertEqual(analyze_profile_bio_signals(self.bundle, policy=self.policy), self.analysis)
        self.assertEqual(
            [signal["signal_type"] for signal in self.analysis["signals"]],
            [
                "observed_chinese_content",
                "china_ecosystem_experience_lead",
                "china_ecosystem_experience_lead",
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

    def test_ecosystem_requires_full_window_positive_ownership_grammar(self) -> None:
        negatives = (
            (4, "看到小红书行业讨论"),
            (4, "小红书用户有很多粉丝"),
            (4, "同名个人博客。小红书用户有很多粉丝"),
            (4, "这不是我的小红书账号"),
            (4, "不是我的公众号"),
            (4, "并非同名小红书账号"),
            (4, "我的小红书用户画像研究"),
            (4, "推荐我的小红书好友"),
            (4, "我的小红书不是我的账号"),
            (4, "同名小红书并非本人运营"),
            (4, "我的小红书账号由朋友运营"),
            (4, "我的小红书关注列表"),
            (7, "公众号 AI research"),
            (7, "公众号"),
            (7, "公众号 SyntheticFounder 关注列表"),
            (7, "公众号 SyntheticFounder 用户列表"),
            (4, "我的小红书 account list"),
            (4, "我的小红书账号。并非本人运营"),
            (4, "我的小红书账号，由朋友运营"),
            (4, "我的小红书账号。其实不属于我"),
            (4, "我的小红书账号，并不属于本人"),
            (4, "我的小红书账号，和我无关"),
            (4, "同名小红书四万粉丝。非同一人"),
            (4, "我的小红书账号，由同事运营"),
            (4, "我的小红书账号，由朋友负责运营"),
            (4, "我的小红书账号，朋友在运营"),
            (4, "我的小红书账号，运营者是朋友"),
            (4, "我的小红书账号，已转让给朋友"),
            (4, "my Xiaohongshu account; my friend runs it"),
            (4, "my Xiaohongshu account; it doesn't belong to me"),
            (4, "my Xiaohongshu account; it isn’t mine"),
            (4, "my Xiaohongshu account list"),
            (4, "my Xiaohongshu account topic"),
            (4, "my Xiaohongshu account research"),
            (4, "my Xiaohongshu account directory"),
            (4, "my Xiaohongshu account archive"),
            (7, "公众号 SyntheticFounder（由同事运营）"),
            (7, "公众号 SyntheticFounder（朋友在运营）"),
            (7, "公众号 SyntheticFounder（并不属于我）"),
            (4, "我的小红书账号。由朋\u200b友运营"),
            (4, "我的小红书账号。并\u200b非本人运营"),
            (4, "我的小红书\t账号"),
        )
        for proposal_index, excerpt in negatives:
            payload = self._single_proposal_bundle(self.bundle, proposal_index, excerpt=excerpt)
            with self.subTest(excerpt=excerpt):
                self.assert_bundle_rejected(payload)
                errors = validate_evidence_bundle(payload, policy=self.policy)
                self.assertTrue(any("closed positive ownership grammar" in error for error in errors))

        positives = (
            (4, "同名小红书四万粉丝", "xiaohongshu"),
            (4, "小红书同名四万粉丝", "xiaohongshu"),
            (4, "我的小红书用户数四万", "xiaohongshu"),
            (7, "我的公众号 SyntheticFounder（长文首发）", "wechat_official_account"),
            (7, "公众号 SyntheticFounder（长文首发）", "wechat_official_account"),
        )
        for proposal_index, excerpt, ecosystem_id in positives:
            with self.subTest(excerpt=excerpt):
                payload = self._single_proposal_bundle(self.bundle, proposal_index, excerpt=excerpt)
                self.assertEqual(validate_evidence_bundle(payload, policy=self.policy), [])
                analysis = analyze_profile_bio_signals(payload, policy=self.policy)
                self.assertEqual(analysis["signals"][0]["value"], ecosystem_id)
                self.assertEqual(
                    analysis["experience_leads"]["physical_region_experience"],
                    {"status": "not_evaluated", "evidence_refs": []},
                )
                self.assertIs(analysis["claims"]["ethnicity_inferred"], False)

    def test_organization_mentions_are_handle_bound_proposals_not_confirmed_employment(self) -> None:
        rejected_claims = (
            (1, "Discussing growth with @synthetic_hub", None),
            (2, "前沿研究讨论 @synth_listen", None),
            (2, "Prev role elsewhere。Discussing @synth_listen", None),
            (2, "Head of @synth_listen, Prev @another_org", None),
            (1, "Not Head of @synthetic_hub", "Head of"),
            (1, "Looking for Head of @synthetic_hub", "Head of"),
            (1, "从未任职于 @synthetic_hub", None),
            (2, "Not Previously @synth_listen", None),
            (1, "Ex-Head of @synthetic_hub", "Head of"),
            (1, "Was Head of @synthetic_hub", "Head of"),
            (1, "Past Head of @synthetic_hub", "Head of"),
            (1, "Aspiring Head of @synthetic_hub", "Head of"),
            (1, "Incoming Head of @synthetic_hub", "Head of"),
            (1, "Future Head of @synthetic_hub", "Head of"),
            (1, "曾就职于 @synthetic_hub", None),
            (1, "此前就职于 @synthetic_hub", None),
            (1, "原就职于 @synthetic_hub", None),
            (1, "不再任职于 @synthetic_hub", None),
            (1, "并不任职于 @synthetic_hub", None),
            (1, "尚未任职于 @synthetic_hub", None),
            (1, "从不任职于 @synthetic_hub", None),
            (1, "将任职于 @synthetic_hub", None),
            (1, "将就职于 @synthetic_hub", None),
            (1, "准备任职于 @synthetic_hub", None),
            (1, "打算任职于 @synthetic_hub", None),
            (1, "希望任职于 @synthetic_hub", None),
            (1, "想任职于 @synthetic_hub", None),
            (1, "有意任职于 @synthetic_hub", None),
            (1, "Will be Head of @synthetic_hub", "Head of"),
            (1, "Used to be Head of @synthetic_hub", "Head of"),
            (1, "Hoping to be Engineer at @synthetic_hub", "Engineer at"),
            (1, "Planning to be Engineer at @synthetic_hub", "Engineer at"),
            (1, "之前任职于 @synthetic_hub", None),
            (1, "即将任职于 @synthetic_hub", None),
            (1, "未来任职于 @synthetic_hub", None),
            (1, "计划任职于 @synthetic_hub", None),
            (1, "过去任职于 @synthetic_hub", None),
            (2, "不曾任 @synth_listen", None),
            (2, "并不曾任 @synth_listen", None),
            (2, "未曾任 @synth_listen", None),
            (1, "即\u200b将任职于 @synthetic_hub", None),
            (1, "未\u200b曾任职于 @synthetic_hub", None),
            (1, "从\u200b未任职于 @synthetic_hub", None),
            (1, "计\u200b划任职于 @synthetic_hub", None),
            (1, "Fu\u200bture Head of @synthetic_hub", "Head of"),
            (1, "N\u200bot Head of @synthetic_hub", "Head of"),
            (1, "Head of growth\t@synthetic_hub", "Head of growth"),
            (1, "Head of growth @synthetic_hub, advisor", "advisor"),
            (1, "Head of growth @synthetic_hub @another_org", "Head of growth"),
            (1, "Head of @synthetic_hub, advisor @synthetic_hub", "advisor"),
            (
                1,
                "Head of growth. Not employed by @synthetic_hub",
                "Head of growth. Not employed by",
            ),
            (
                1,
                "Head of growth / never employed by @synthetic_hub",
                "Head of growth / never employed by",
            ),
            (
                1,
                "Head of growth - not currently at @synthetic_hub",
                "Head of growth - not currently at",
            ),
            (
                1,
                "Head of growth not anymore at @synthetic_hub",
                "Head of growth not anymore at",
            ),
            (
                1,
                "Head of growth future role at @synthetic_hub",
                "Head of growth future role at",
            ),
            (
                1,
                "Head of growth planning to join @synthetic_hub",
                "Head of growth planning to join",
            ),
            (2, "Former never employed @synth_listen", "never employed"),
            (2, "Former not employed @synth_listen", "not employed"),
            (2, "Former incoming researcher @synth_listen", "incoming researcher"),
            (2, "Former future engineer @synth_listen", "future engineer"),
            (2, "Former aspiring founder @synth_listen", "aspiring founder"),
            (2, "Former planning to join @synth_listen", "planning to join"),
        )
        for proposal_index, excerpt, role_text in rejected_claims:
            with self.subTest(excerpt=excerpt, role_text=role_text):
                payload = self._single_proposal_bundle(self.bundle, proposal_index, excerpt=excerpt)
                payload["proposals"][0]["details"]["role_text"] = role_text
                self.assert_bundle_rejected(payload)
                errors = validate_evidence_bundle(payload, policy=self.policy)
                self.assertTrue(any("positive affiliation grammar" in error for error in errors))

        invalid_role_texts = (
            ("Head of growth @synthetic_hub", "@synthetic_hub"),
            ("Head of growth @synthetic_hub", "synthetic_hub"),
            ("Head of growth @synthetic_hub", "@synthetic"),
            ("Researcher at @synthetic_hub", "at"),
        )
        for excerpt, role_text in invalid_role_texts:
            with self.subTest(excerpt=excerpt, role_text=role_text):
                payload = self._single_proposal_bundle(self.bundle, 1, excerpt=excerpt)
                payload["proposals"][0]["details"]["role_text"] = role_text
                self.assert_bundle_rejected(payload)
                errors = validate_evidence_bundle(payload, policy=self.policy)
                self.assertTrue(any("exact bounded role span" in error for error in errors))

        positive_claims = (
            (1, "Head of growth @synthetic_hub", "Head of growth", "current"),
            (1, "Researcher at @synthetic_hub", "Researcher", "current"),
            (1, "任职于 @synthetic_hub", None, "current"),
            (1, "就职于 @synthetic_hub", None, "current"),
            (2, "Prev @synth_listen @synth_int", None, "previous"),
            (2, "曾任 @synth_listen", None, "previous"),
            (2, "前任职于 @synth_listen", None, "previous"),
            (2, "此前任职于 @synth_listen", None, "previous"),
            (2, "Former Researcher @synth_listen", "Researcher", "previous"),
        )
        for proposal_index, excerpt, role_text, expected_relation in positive_claims:
            with self.subTest(excerpt=excerpt, role_text=role_text):
                payload = self._single_proposal_bundle(self.bundle, proposal_index, excerpt=excerpt)
                payload["proposals"][0]["details"]["role_text"] = role_text
                self.assertEqual(validate_evidence_bundle(payload, policy=self.policy), [])
                analysis = analyze_profile_bio_signals(payload, policy=self.policy)
                self.assertEqual(analysis["affiliation_proposals"][0]["relation"], expected_relation)
                self.assertEqual(analysis["affiliation_proposals"][0]["role_text"], role_text)
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
        cloned["proposal_id"] = "xbp_01J0000000000000000000000A"
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

        for noncanonical_url in (
            "HTTPS://profiles.invalid/x/syntheticbuild",
            "https://PROFILES.invalid/x/syntheticbuild",
        ):
            with self.subTest(profile_url=noncanonical_url):
                payload = copy.deepcopy(self.bundle)
                payload["profile_snapshot"]["profile_url"] = noncanonical_url
                self.assert_bundle_rejected(payload, schema_too=True)

    def test_extractor_version_runtime_and_schema_share_the_100_character_bound(self) -> None:
        schema_maximum = self.bundle_schema["$defs"]["extractor"]["properties"]["version"]["maxLength"]
        self.assertEqual(self.policy["limits"]["max_extractor_version_characters"], schema_maximum)
        self.assertEqual(schema_maximum, 100)

        bounded = copy.deepcopy(self.bundle)
        bounded["proposals"][0]["extractor"]["version"] = "v" * 100
        self.assertEqual(validate_evidence_bundle(bounded, policy=self.policy), [])
        self.assertEqual(_schema_errors(bounded, self.bundle_schema), [])

        oversized = copy.deepcopy(self.bundle)
        oversized["proposals"][0]["extractor"]["version"] = "v" * 101
        self.assert_bundle_rejected(oversized, schema_too=True)

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

        for location in ("bio_text", "excerpt"):
            surrogate = copy.deepcopy(self.bundle)
            if location == "bio_text":
                surrogate["profile_snapshot"]["bio_text"] = "bad-\ud800"
            else:
                surrogate["proposals"][0]["excerpt"] = "bad-\ud800"
            with self.subTest(location=location):
                self.assertEqual(
                    validate_evidence_bundle(surrogate, policy=self.policy),
                    ["$.validation: non_unicode_scalar_text"],
                )

        surrogate_analysis = copy.deepcopy(self.analysis)
        surrogate_analysis["signals"][0]["value"] = "bad-\ud800"
        self.assertEqual(
            validate_analysis(surrogate_analysis, evidence_bundle=self.bundle, policy=self.policy),
            ["$.validation: non_unicode_scalar_text"],
        )

        deep_value: Any = None
        for _ in range(1500):
            deep_value = {"child": deep_value}
        deep_root = copy.deepcopy(self.bundle)
        deep_root["subject"] = deep_value
        deep_root_errors = validate_evidence_bundle(deep_root, policy=self.policy)
        self.assertIn("$.validation: nested_depth_budget_exceeded", deep_root_errors)
        deep_valid_shape_leaf = copy.deepcopy(self.bundle)
        deep_valid_shape_leaf["proposals"][4]["details"]["ecosystem_id"] = deep_value
        deep_leaf_errors = validate_evidence_bundle(deep_valid_shape_leaf, policy=self.policy)
        self.assertIn("$.validation: nested_depth_budget_exceeded", deep_leaf_errors)
        wide = copy.deepcopy(self.bundle)
        wide["claims"] = {"items": [None] * 5000}
        wide_errors = validate_evidence_bundle(wide, policy=self.policy)
        self.assertIn("$.validation: nested_node_budget_exceeded", wide_errors)

        deep_analysis = copy.deepcopy(self.analysis)
        deep_analysis["claims"] = deep_value
        self.assertEqual(
            validate_analysis(deep_analysis, evidence_bundle=self.bundle, policy=self.policy),
            ["$.validation: nested_depth_budget_exceeded"],
        )

        def descendants(value: Any, path: tuple[str | int, ...] = ()) -> list[tuple[tuple[str | int, ...], Any]]:
            found: list[tuple[tuple[str | int, ...], Any]] = []
            if isinstance(value, dict):
                for key, child in value.items():
                    child_path = (*path, key)
                    found.append((child_path, child))
                    found.extend(descendants(child, child_path))
            elif isinstance(value, list):
                for index, child in enumerate(value):
                    child_path = (*path, index)
                    found.append((child_path, child))
                    found.extend(descendants(child, child_path))
            return found

        mutation_count = 0
        replacement_corpus: tuple[Any, ...] = (None, False, 0, 1.5, "invalid_scalar", [], {})
        for path, original in descendants(self.bundle):
            for replacement in replacement_corpus:
                if type(replacement) is type(original) and replacement == original:
                    continue
                payload = copy.deepcopy(self.bundle)
                owner: Any = payload
                for token in path[:-1]:
                    owner = owner[token]
                owner[path[-1]] = copy.deepcopy(replacement)
                schema_errors = _schema_errors(payload, self.bundle_schema)
                if not schema_errors:
                    continue
                with self.subTest(path=path, replacement=repr(replacement)):
                    self.assertTrue(validate_evidence_bundle(payload, policy=self.policy))
                mutation_count += 1
        self.assertEqual(mutation_count, 1275)

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

        deep_subject = '{"child":' * 1500 + "null" + "}" * 1500
        deep_raw = (
            '{"schema_version":"x.profile.bio_evidence.bundle.v1",'
            '"policy_version":"profile-bio-signal-v1.5","subject":'
            + deep_subject
            + ',"profile_snapshot":{},"proposals":[],"claims":{}}'
        )
        with tempfile.TemporaryDirectory() as temp_dir:
            deep_path = Path(temp_dir) / "deep.json"
            deep_path.write_text(deep_raw, encoding="utf-8")
            deep_completed = subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "x_first.profile_bio_signals",
                    str(deep_path),
                    "--policy",
                    str(ROOT / "configs/profile_bio_signal_policy.v1.json"),
                ],
                cwd=ROOT,
                env={**os.environ, "PYTHONPATH": "src"},
                check=False,
                capture_output=True,
                text=True,
                timeout=10,
            )
        self.assertEqual(deep_completed.returncode, 1)
        self.assertEqual(deep_completed.stderr, "")
        self.assertEqual(json.loads(deep_completed.stdout)["status"], "invalid")
        self.assertNotIn("Traceback", deep_completed.stdout)

        surrogate_payloads = []
        bio_surrogate = copy.deepcopy(self.bundle)
        bio_surrogate["profile_snapshot"]["bio_text"] = "bad-\ud800"
        surrogate_payloads.append(("bio", bio_surrogate))
        excerpt_surrogate = copy.deepcopy(self.bundle)
        excerpt_surrogate["proposals"][0]["excerpt"] = "bad-\ud800"
        surrogate_payloads.append(("excerpt", excerpt_surrogate))
        with tempfile.TemporaryDirectory() as temp_dir:
            for label, payload in surrogate_payloads:
                with self.subTest(label=label):
                    surrogate_path = Path(temp_dir) / f"{label}.json"
                    surrogate_path.write_text(json.dumps(payload, ensure_ascii=True), encoding="utf-8")
                    surrogate_completed = subprocess.run(
                        [
                            sys.executable,
                            "-m",
                            "x_first.profile_bio_signals",
                            str(surrogate_path),
                            "--policy",
                            str(ROOT / "configs/profile_bio_signal_policy.v1.json"),
                        ],
                        cwd=ROOT,
                        env={**os.environ, "PYTHONPATH": "src"},
                        check=False,
                        capture_output=True,
                        text=True,
                        timeout=10,
                    )
                    self.assertEqual(surrogate_completed.returncode, 1)
                    self.assertEqual(surrogate_completed.stderr, "")
                    self.assertEqual(
                        json.loads(surrogate_completed.stdout),
                        {"errors": ["$.validation: non_unicode_scalar_text"], "status": "invalid"},
                    )

        source = (ROOT / "src/x_first/profile_bio_signals.py").read_text(encoding="utf-8")
        self.assertEqual(_forbidden_runtime_imports(source), set())
        self.assertNotIn("sourcing_agent", source)

        synthetic_forbidden_imports = """
from httpx import Client
from requests.sessions import Session
from urllib.request import urlopen
from urllib import request
from urllib import request as request_alias
import socket
"""
        self.assertEqual(
            _forbidden_runtime_imports(synthetic_forbidden_imports),
            {"httpx", "requests", "socket", "urllib.request"},
        )
        self.assertEqual(_forbidden_runtime_imports("from urllib import parse"), set())


if __name__ == "__main__":
    unittest.main()
