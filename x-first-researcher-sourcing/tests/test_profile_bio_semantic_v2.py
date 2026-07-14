from __future__ import annotations

import ast
import copy
import re
import sys
import unittest
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from x_first.profile_bio_semantic_v2 import (  # noqa: E402
    BUDGETS,
    CANONICAL_OUTPUT_SCHEMA_SHA256,
    CANONICAL_PROMPT_SHA256,
    MODEL_AUTHORITY,
    MODEL_ID,
    MODEL_OUTPUT_SCHEMA_VERSION,
    REASON_SOURCE,
    OfflineFakeResponsesTransport,
    build_responses_request,
    canonical_json,
    canonical_sha256,
    load_json,
    run_semantic_review,
    structured_output_schema,
    text_sha256,
    validate_model_output,
    validate_output_schema,
    validate_prompt,
    validate_request,
    validate_review,
)


def _schema_errors(instance: Any, schema: Any, *, path: str = "$") -> list[str]:
    if not isinstance(schema, dict):
        return [f"{path}: invalid schema"]
    errors: list[str] = []
    for sub_schema in schema.get("allOf", []):
        errors.extend(_schema_errors(instance, sub_schema, path=path))
    if "oneOf" in schema:
        matches = sum(not _schema_errors(instance, option, path=path) for option in schema["oneOf"])
        if matches != 1:
            errors.append(f"{path}: oneOf mismatch")
    if "const" in schema and instance != schema["const"]:
        errors.append(f"{path}: const mismatch")
    if "enum" in schema and instance not in schema["enum"]:
        errors.append(f"{path}: enum mismatch")
    expected = schema.get("type")
    allowed = [expected] if isinstance(expected, str) else expected
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
        for key in schema.get("required", []):
            if key not in instance:
                errors.append(f"{path}: missing {key}")
        properties = schema.get("properties", {})
        for key, value in instance.items():
            if key in properties:
                errors.extend(_schema_errors(value, properties[key], path=f"{path}.{key}"))
            elif schema.get("additionalProperties") is False:
                errors.append(f"{path}: unexpected {key}")
    if isinstance(instance, list):
        if len(instance) < schema.get("minItems", 0):
            errors.append(f"{path}: too few items")
        if "maxItems" in schema and len(instance) > schema["maxItems"]:
            errors.append(f"{path}: too many items")
        if schema.get("uniqueItems") is True:
            identities = [canonical_json(value) for value in instance]
            if len(set(identities)) != len(identities):
                errors.append(f"{path}: duplicate items")
        for index, value in enumerate(instance):
            if "items" in schema:
                errors.extend(_schema_errors(value, schema["items"], path=f"{path}[{index}]"))
    return errors


def _assert_strict_object_schemas(test: unittest.TestCase, schema: Any) -> None:
    if isinstance(schema, dict):
        schema_type = schema.get("type")
        if schema_type == "object":
            test.assertIs(schema.get("additionalProperties"), False)
            test.assertEqual(set(schema.get("required", [])), set(schema.get("properties", {})))
        for value in schema.values():
            _assert_strict_object_schemas(test, value)
    elif isinstance(schema, list):
        for value in schema:
            _assert_strict_object_schemas(test, value)


def _fake_response(model_output: Any, *, usage: dict[str, int] | None = None) -> dict[str, Any]:
    return {
        "id": "resp_fixture_semantic_v2",
        "object": "response",
        "status": "completed",
        "model": MODEL_ID,
        "output": [
            {
                "id": "msg_fixture_semantic_v2",
                "type": "message",
                "status": "completed",
                "role": "assistant",
                "content": [
                    {
                        "type": "output_text",
                        "text": canonical_json(model_output),
                        "annotations": [],
                    }
                ],
            }
        ],
        "usage": usage or {"input_tokens": 600, "output_tokens": 300, "total_tokens": 900},
    }


class ProfileBioSemanticV2Test(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.prompt = load_json(ROOT / "configs/profile_bio_semantic_prompt.v2.json")
        cls.request = load_json(ROOT / "fixtures/profile_bio_semantic_request_v2.json")
        cls.model_output = load_json(ROOT / "fixtures/profile_bio_semantic_model_output_v2.json")
        cls.review = load_json(ROOT / "fixtures/profile_bio_semantic_review_v2.json")
        cls.request_schema = load_json(ROOT / "contracts/x.profile.bio_semantic.request.v2.schema.json")
        cls.model_output_schema = load_json(ROOT / "contracts/x.profile.bio_semantic.model_output.v2.schema.json")
        cls.review_schema = load_json(ROOT / "contracts/x.profile.bio_semantic.review.v2.schema.json")
        cls.raw_response = _fake_response(cls.model_output)

    def _request_for_bio(self, bio: str, *, execution_mode: str = "offline_fake") -> dict[str, Any]:
        request = copy.deepcopy(self.request)
        digest = text_sha256(bio)
        request["request_id"] = f"xbsv2r_{digest[:24]}"
        request["profile_snapshot"]["snapshot_id"] = f"xbsv2s_{digest[24:48]}"
        request["profile_snapshot"]["bio_text"] = bio
        request["profile_snapshot"]["bio_sha256"] = digest
        request["model_execution_mode"] = execution_mode
        return request

    @staticmethod
    def _output_for(
        request: dict[str, Any],
        *,
        proposal: dict[str, Any] | None,
        verdict: str = "proposals_available",
    ) -> dict[str, Any]:
        profile = request["profile_snapshot"]
        return {
            "schema_version": MODEL_OUTPUT_SCHEMA_VERSION,
            "request_id": request["request_id"],
            "profile_snapshot_id": profile["snapshot_id"],
            "platform_user_id": profile["platform_user_id"],
            "bio_sha256": profile["bio_sha256"],
            "verdict": verdict,
            "proposals": [] if proposal is None else [proposal],
            "authority": dict(MODEL_AUTHORITY),
        }

    @staticmethod
    def _proposal(
        bio: str,
        *,
        proposal_type: str,
        relation_state: str,
        reason_code: str,
        reason: str,
    ) -> dict[str, Any]:
        return {
            "proposal_type": proposal_type,
            "relation_state": relation_state,
            "span_start": 0,
            "span_end": len(bio),
            "excerpt": bio,
            "reason_codes": [reason_code],
            "reason": reason,
            "reason_source": REASON_SOURCE,
            "confidence": "high",
            "evidence_basis": "profile_bio_only",
            "requires_independent_verification": True,
        }

    def test_contract_fixtures_strict_payload_and_deterministic_review(self) -> None:
        self.assertEqual(validate_prompt(self.prompt), [])
        self.assertEqual(canonical_sha256(self.prompt), CANONICAL_PROMPT_SHA256)
        self.assertEqual(validate_output_schema(self.model_output_schema), [])
        self.assertEqual(canonical_sha256(self.model_output_schema), CANONICAL_OUTPUT_SCHEMA_SHA256)
        self.assertEqual(validate_request(self.request, prompt=self.prompt, output_schema=self.model_output_schema), [])
        self.assertEqual(validate_model_output(self.model_output, request=self.request), [])
        self.assertEqual(_schema_errors(self.request, self.request_schema), [])
        self.assertEqual(_schema_errors(self.model_output, self.model_output_schema), [])
        self.assertEqual(_schema_errors(self.review, self.review_schema), [])
        _assert_strict_object_schemas(self, structured_output_schema(self.model_output_schema))

        payload = build_responses_request(
            self.request,
            prompt=self.prompt,
            output_schema=self.model_output_schema,
        )
        self.assertEqual(payload["model"], "gpt-5.6-luna")
        self.assertEqual(payload["reasoning"], {"effort": "low"})
        self.assertEqual(payload["tools"], [])
        self.assertIs(payload["store"], False)
        self.assertEqual(payload["truncation"], "disabled")
        self.assertEqual(payload["text"]["format"]["type"], "json_schema")
        self.assertIs(payload["text"]["format"]["strict"], True)
        self.assertEqual(payload["text"]["format"]["schema"], structured_output_schema(self.model_output_schema))
        self.assertIsNone(self.request["model_policy"]["fallback_model"])
        self.assertNotIn("previous_response_id", payload)

        transport = OfflineFakeResponsesTransport(self.raw_response)
        observed = run_semantic_review(
            self.request,
            prompt=self.prompt,
            output_schema=self.model_output_schema,
            transport=transport,
        )
        self.assertEqual(transport.calls, 1)
        self.assertEqual(transport.last_timeout_ms, BUDGETS["timeout_ms"])
        self.assertEqual(observed, self.review)
        self.assertEqual(
            validate_review(
                observed,
                request=self.request,
                prompt=self.prompt,
                output_schema=self.model_output_schema,
                raw_response=self.raw_response,
            ),
            [],
        )

        reasoning_response = copy.deepcopy(self.raw_response)
        reasoning_response["output"].insert(
            0,
            {
                "id": "rs_fixture_semantic_v2",
                "type": "reasoning",
                "status": "completed",
                "summary": [{"type": "summary_text", "text": "Bounded provider summary."}],
            },
        )
        reasoning_result = run_semantic_review(
            self.request,
            prompt=self.prompt,
            output_schema=self.model_output_schema,
            transport=OfflineFakeResponsesTransport(reasoning_response),
        )
        self.assertEqual(reasoning_result["status"], "completed")

        parser_mutations = []
        message_incomplete = copy.deepcopy(self.raw_response)
        message_incomplete["output"][0]["status"] = "incomplete"
        parser_mutations.append(message_incomplete)
        message_without_id = copy.deepcopy(self.raw_response)
        del message_without_id["output"][0]["id"]
        parser_mutations.append(message_without_id)
        response_incomplete = copy.deepcopy(self.raw_response)
        response_incomplete["status"] = "incomplete"
        parser_mutations.append(response_incomplete)
        incomplete_details = copy.deepcopy(self.raw_response)
        incomplete_details["incomplete_details"] = {"reason": "max_output_tokens"}
        parser_mutations.append(incomplete_details)
        reasoning_without_status = copy.deepcopy(reasoning_response)
        del reasoning_without_status["output"][0]["status"]
        parser_mutations.append(reasoning_without_status)
        reasoning_without_id = copy.deepcopy(reasoning_response)
        del reasoning_without_id["output"][0]["id"]
        parser_mutations.append(reasoning_without_id)
        malformed_summary = copy.deepcopy(reasoning_response)
        malformed_summary["output"][0]["summary"] = ["not-a-summary-object"]
        parser_mutations.append(malformed_summary)
        refusal = copy.deepcopy(self.raw_response)
        refusal["output"][0]["content"][0] = {"type": "refusal", "refusal": "No."}
        parser_mutations.append(refusal)
        rerouted = copy.deepcopy(self.raw_response)
        rerouted["model"] = "gpt-5.6-sol"
        parser_mutations.append(rerouted)
        for raw_response in parser_mutations:
            with self.subTest(parser_case=canonical_sha256(raw_response)):
                result = run_semantic_review(
                    self.request,
                    prompt=self.prompt,
                    output_schema=self.model_output_schema,
                    transport=OfflineFakeResponsesTransport(raw_response),
                )
                self.assertEqual(result["error_codes"], ["response_invalid"])
                self.assertEqual(
                    validate_review(
                        result,
                        request=self.request,
                        prompt=self.prompt,
                        output_schema=self.model_output_schema,
                        raw_response=raw_response,
                    ),
                    [],
                )

        tool_response = copy.deepcopy(reasoning_response)
        tool_response["output"][0] = {"type": "function_call", "name": "forbidden_tool"}
        tool_result = run_semantic_review(
            self.request,
            prompt=self.prompt,
            output_schema=self.model_output_schema,
            transport=OfflineFakeResponsesTransport(tool_response),
        )
        self.assertEqual(tool_result["error_codes"], ["response_invalid"])

        duplicate_key_response = copy.deepcopy(self.raw_response)
        duplicate_key_response["output"][0]["content"][0]["text"] = canonical_json(self.model_output).replace(
            '"verdict":"proposals_available"',
            '"verdict":"abstained","verdict":"proposals_available"',
            1,
        )
        duplicate_key_result = run_semantic_review(
            self.request,
            prompt=self.prompt,
            output_schema=self.model_output_schema,
            transport=OfflineFakeResponsesTransport(duplicate_key_response),
        )
        self.assertEqual(duplicate_key_result["error_codes"], ["model_output_invalid"])

        non_finite_text_response = copy.deepcopy(self.raw_response)
        non_finite_text_response["output"][0]["content"][0]["text"] = canonical_json(self.model_output).replace(
            '"span_start":0',
            '"span_start":NaN',
            1,
        )
        non_finite_text_result = run_semantic_review(
            self.request,
            prompt=self.prompt,
            output_schema=self.model_output_schema,
            transport=OfflineFakeResponsesTransport(non_finite_text_response),
        )
        self.assertEqual(non_finite_text_result["error_codes"], ["model_output_invalid"])

    def test_eight_synthetic_bios_cover_strong_weak_and_empty_semantics(self) -> None:
        cases = (
            (
                "在新加坡从事模型训练研究。",
                "professional_region_experience",
                "not_applicable",
                "explicit_professional_region_experience",
                "Bio states professional work in 新加坡.",
            ),
            (
                "小红书同名四万粉丝",
                "professional_china_digital_ecosystem",
                "not_applicable",
                "explicit_china_digital_ecosystem_activity",
                "Bio names 小红书 as professional ecosystem activity.",
            ),
            (
                "公众号 SyntheticFounder（长文首发）",
                "professional_china_digital_ecosystem",
                "not_applicable",
                "explicit_china_digital_ecosystem_activity",
                "Bio names SyntheticFounder as a professional account.",
            ),
            (
                "Head of growth @synthetic_hub",
                "professional_affiliation",
                "current_claimed",
                "explicit_current_organization_claim",
                "Bio states a current role at @synthetic_hub.",
            ),
            (
                "Prev @synth_listen",
                "professional_affiliation",
                "previous_claimed",
                "explicit_previous_organization_claim",
                "Bio marks @synth_listen as a previous organization.",
            ),
            (
                "Future Researcher at @synthetic_lab",
                "professional_affiliation",
                "future_or_aspirational",
                "ambiguous_professional_context_requires_review",
                "Bio states a future role at @synthetic_lab.",
            ),
            (
                "分享中文 AI 工程实践。",
                "observed_chinese_professional_content",
                "not_applicable",
                "observed_chinese_professional_content",
                "Bio contains observed Chinese language and AI engineering content.",
            ),
            ("AI systems and data.", None, None, None, None),
        )
        for bio, proposal_type, relation_state, reason_code, reason in cases:
            with self.subTest(bio=bio):
                request = self._request_for_bio(bio)
                proposal = (
                    None
                    if proposal_type is None
                    else self._proposal(
                        bio,
                        proposal_type=proposal_type,
                        relation_state=relation_state,
                        reason_code=reason_code,
                        reason=reason,
                    )
                )
                output = self._output_for(
                    request,
                    proposal=proposal,
                    verdict="no_supported_professional_context" if proposal is None else "proposals_available",
                )
                self.assertEqual(validate_model_output(output, request=request), [])
                result = run_semantic_review(
                    request,
                    prompt=self.prompt,
                    output_schema=self.model_output_schema,
                    transport=OfflineFakeResponsesTransport(_fake_response(output)),
                )
                self.assertEqual(result["status"], "completed")
                self.assertEqual(result["platform_user_id"], request["profile_snapshot"]["platform_user_id"])
                self.assertTrue(all(value is False for value in result["authority"].values()))
                if proposal is None:
                    self.assertEqual(result["proposals"], [])
                else:
                    observed = result["proposals"][0]
                    self.assertEqual(observed["proposal_type"], proposal_type)
                    self.assertEqual(observed["excerpt_sha256"], text_sha256(bio))
                    self.assertEqual(observed["reason_source"], REASON_SOURCE)
                    self.assertEqual(observed["verification_status"], "unverified_professional_context_proposal")

    def test_model_output_rejects_unbound_spans_reasons_authority_and_verdicts(self) -> None:
        mutations: list[dict[str, Any]] = []
        stale_span = copy.deepcopy(self.model_output)
        stale_span["proposals"][0]["span_end"] -= 1
        mutations.append(stale_span)
        foreign_account = copy.deepcopy(self.model_output)
        foreign_account["platform_user_id"] = "900000000000000099"
        mutations.append(foreign_account)
        external_url = copy.deepcopy(self.model_output)
        external_url["proposals"][0]["reason"] = "According to https://example.com, this is true."
        mutations.append(external_url)
        unsupported_handle = copy.deepcopy(self.model_output)
        unsupported_handle["proposals"][1]["reason"] = "Bio states a role at @outside_org."
        mutations.append(unsupported_handle)
        unsupported_number = copy.deepcopy(self.model_output)
        unsupported_number["proposals"][3]["reason"] = "Bio states 100K professional activity."
        mutations.append(unsupported_number)
        unsupported_chinese_number = copy.deepcopy(self.model_output)
        unsupported_chinese_number["proposals"][3]["reason"] = "Bio states 五万 professional followers."
        mutations.append(unsupported_chinese_number)
        protected_identity = copy.deepcopy(self.model_output)
        protected_identity["proposals"][5]["reason"] = "Bio proves Chinese researcher ethnicity."
        mutations.append(protected_identity)
        authority = copy.deepcopy(self.model_output)
        authority["authority"]["discovery_or_ranking_recommended"] = True
        mutations.append(authority)
        bad_relation = copy.deepcopy(self.model_output)
        bad_relation["proposals"][1]["relation_state"] = "not_applicable"
        mutations.append(bad_relation)
        bad_verdict = copy.deepcopy(self.model_output)
        bad_verdict["verdict"] = "no_supported_professional_context"
        mutations.append(bad_verdict)
        duplicate = copy.deepcopy(self.model_output)
        duplicate["proposals"].append(copy.deepcopy(duplicate["proposals"][0]))
        mutations.append(duplicate)
        semantic_duplicate = copy.deepcopy(self.model_output)
        duplicate_with_new_prose = copy.deepcopy(semantic_duplicate["proposals"][0])
        duplicate_with_new_prose["reason"] = "Different untrusted display narrative."
        duplicate_with_new_prose["confidence"] = "low"
        semantic_duplicate["proposals"].append(duplicate_with_new_prose)
        mutations.append(semantic_duplicate)
        contradictory_relation_codes = copy.deepcopy(self.model_output)
        contradictory_relation_codes["proposals"][1]["reason_codes"] = [
            "explicit_current_organization_claim",
            "explicit_previous_organization_claim",
        ]
        self.assertTrue(_schema_errors(contradictory_relation_codes, self.model_output_schema))
        mutations.append(contradictory_relation_codes)
        wrong_relation_code = copy.deepcopy(self.model_output)
        wrong_relation_code["proposals"][1]["reason_codes"] = ["explicit_previous_organization_claim"]
        mutations.append(wrong_relation_code)

        for model_output in mutations:
            with self.subTest(digest=canonical_sha256(model_output)):
                self.assertTrue(validate_model_output(model_output, request=self.request))
                result = run_semantic_review(
                    self.request,
                    prompt=self.prompt,
                    output_schema=self.model_output_schema,
                    transport=OfflineFakeResponsesTransport(_fake_response(model_output)),
                )
                self.assertEqual(result["status"], "failed")
                self.assertEqual(result["error_codes"], ["model_output_invalid"])
                self.assertEqual(result["proposals"], [])
                self.assertEqual(
                    validate_review(
                        result,
                        request=self.request,
                        prompt=self.prompt,
                        output_schema=self.model_output_schema,
                        raw_response=_fake_response(model_output),
                    ),
                    [],
                )

        natural_paraphrase = copy.deepcopy(self.model_output)
        natural_paraphrase["proposals"][0]["reason"] = (
            "The cited Bio passage directly supports this professional-context proposal; verify it independently."
        )
        self.assertEqual(validate_model_output(natural_paraphrase, request=self.request), [])

        invented_narrative = copy.deepcopy(self.model_output)
        invented_narrative["proposals"][0]["reason"] = "Anthropic in Canada."
        self.assertEqual(validate_model_output(invented_narrative, request=self.request), [])
        invented_raw_response = _fake_response(invented_narrative)
        invented_review = run_semantic_review(
            self.request,
            prompt=self.prompt,
            output_schema=self.model_output_schema,
            transport=OfflineFakeResponsesTransport(invented_raw_response),
        )
        self.assertEqual(invented_review["status"], "completed")
        self.assertEqual(invented_review["proposals"][0]["proposal_id"], self.review["proposals"][0]["proposal_id"])
        self.assertEqual(invented_review["proposals"][0]["reason_source"], REASON_SOURCE)
        self.assertTrue(all(value is False for value in invented_review["authority"].values()))
        self.assertEqual(
            validate_review(
                invented_review,
                request=self.request,
                prompt=self.prompt,
                output_schema=self.model_output_schema,
                raw_response=invented_raw_response,
            ),
            [],
        )

    def test_live_gate_splits_fixture_source_from_model_execution_and_never_calls(self) -> None:
        class NeverLiveTransport:
            is_live = True
            transport_id = "openai_compatible_responses"

            def __init__(self) -> None:
                self.calls = 0

            def create_response(self, payload: Any, *, timeout_ms: int) -> dict[str, Any]:
                self.calls += 1
                raise AssertionError("live transport must not be invoked in this slice")

        transport = NeverLiveTransport()
        live_request = copy.deepcopy(self.request)
        live_request["model_execution_mode"] = "live_canary"
        blocked = run_semantic_review(
            live_request,
            prompt=self.prompt,
            output_schema=self.model_output_schema,
            transport=transport,
        )
        self.assertEqual(blocked["status"], "blocked")
        self.assertEqual(blocked["error_codes"], ["execute_live_required"])
        self.assertEqual(transport.calls, 0)
        self.assertEqual(
            validate_review(
                blocked,
                request=live_request,
                prompt=self.prompt,
                output_schema=self.model_output_schema,
                raw_response=None,
            ),
            [],
        )
        self.assertEqual(live_request["profile_source_mode"], "offline_fixture")
        self.assertTrue(live_request["profile_snapshot"]["profile_url"].startswith("https://profiles.invalid/"))

        mismatch = run_semantic_review(
            self.request,
            prompt=self.prompt,
            output_schema=self.model_output_schema,
            transport=transport,
            execute_live=True,
        )
        self.assertEqual(mismatch["error_codes"], ["request_mode_mismatch"])
        self.assertEqual(transport.calls, 0)
        forged_transport_failure = copy.deepcopy(mismatch)
        forged_transport_failure["error_codes"] = ["transport_failed"]
        self.assertTrue(
            validate_review(
                forged_transport_failure,
                request=self.request,
                prompt=self.prompt,
                output_schema=self.model_output_schema,
                raw_response=None,
            )
        )

        offline_execute_transport = OfflineFakeResponsesTransport(self.raw_response)
        offline_execute = run_semantic_review(
            self.request,
            prompt=self.prompt,
            output_schema=self.model_output_schema,
            transport=offline_execute_transport,
            execute_live=True,
        )
        self.assertEqual(offline_execute["error_codes"], ["execution_contract_invalid"])
        self.assertEqual(_schema_errors(offline_execute, self.review_schema), [])
        self.assertEqual(offline_execute_transport.calls, 0)

        class InvalidPairTransport:
            def __init__(self, *, is_live: bool, transport_id: str) -> None:
                self.is_live = is_live
                self.transport_id = transport_id
                self.calls = 0

            def create_response(self, payload: Any, *, timeout_ms: int) -> dict[str, Any]:
                self.calls += 1
                return self.raw_response  # type: ignore[attr-defined]

        invalid_pairs = (
            (self.request, InvalidPairTransport(is_live=False, transport_id="openai_compatible_responses"), False),
            (live_request, InvalidPairTransport(is_live=True, transport_id="offline_fake_responses"), True),
        )
        for request, invalid_transport, execute_live in invalid_pairs:
            result = run_semantic_review(
                request,
                prompt=self.prompt,
                output_schema=self.model_output_schema,
                transport=invalid_transport,
                execute_live=execute_live,
            )
            self.assertEqual(result["error_codes"], ["execution_contract_invalid"])
            self.assertEqual(_schema_errors(result, self.review_schema), [])
            self.assertEqual(invalid_transport.calls, 0)
            self.assertEqual(
                validate_review(
                    result,
                    request=request,
                    prompt=self.prompt,
                    output_schema=self.model_output_schema,
                    raw_response=None,
                ),
                [],
            )
        native_source = copy.deepcopy(live_request)
        native_source["profile_source_mode"] = "native_x"
        native_source["profile_snapshot"]["profile_url"] = "https://x.com/semanticfixture"
        self.assertTrue(validate_request(native_source, prompt=self.prompt, output_schema=self.model_output_schema))

        source = (ROOT / "src/x_first/profile_bio_semantic_v2.py").read_text(encoding="utf-8")
        imported = {
            alias.name for node in ast.walk(ast.parse(source)) if isinstance(node, ast.Import) for alias in node.names
        }
        imported.update(
            node.module for node in ast.walk(ast.parse(source)) if isinstance(node, ast.ImportFrom) and node.module
        )
        self.assertFalse({"httpx", "openai", "requests", "socket", "urllib.request"} & imported)

    def test_terminal_total_depth_node_utf8_and_usage_budgets(self) -> None:
        unpaired_bio = copy.deepcopy(self.request)
        unpaired_bio["profile_snapshot"]["bio_text"] = "\ud800"
        self.assertTrue(validate_request(unpaired_bio, prompt=self.prompt, output_schema=self.model_output_schema))
        with self.assertRaisesRegex(ValueError, "request_binding_invalid"):
            run_semantic_review(
                unpaired_bio,
                prompt=self.prompt,
                output_schema=self.model_output_schema,
                transport=OfflineFakeResponsesTransport(self.raw_response),
            )
        for field_name in ("profile_url", "content_version"):
            invalid_scalar = copy.deepcopy(self.request)
            invalid_scalar["profile_snapshot"][field_name] = "prefix\ud800suffix"
            self.assertTrue(
                validate_request(invalid_scalar, prompt=self.prompt, output_schema=self.model_output_schema)
            )
            with self.assertRaisesRegex(ValueError, "request_binding_invalid"):
                run_semantic_review(
                    invalid_scalar,
                    prompt=self.prompt,
                    output_schema=self.model_output_schema,
                    transport=OfflineFakeResponsesTransport(self.raw_response),
                )
        invalid_key = copy.deepcopy(self.request)
        invalid_key["profile_snapshot"]["bad\ud800key"] = "value"
        with self.assertRaisesRegex(ValueError, "request_binding_invalid"):
            run_semantic_review(
                invalid_key,
                prompt=self.prompt,
                output_schema=self.model_output_schema,
                transport=OfflineFakeResponsesTransport(self.raw_response),
            )

        unpaired_reason = copy.deepcopy(self.model_output)
        unpaired_reason["proposals"][0]["reason"] = "\ud800"
        reason_result = run_semantic_review(
            self.request,
            prompt=self.prompt,
            output_schema=self.model_output_schema,
            transport=OfflineFakeResponsesTransport(_fake_response(unpaired_reason)),
        )
        self.assertEqual(reason_result["error_codes"], ["model_output_invalid"])

        raw_unpaired = copy.deepcopy(self.raw_response)
        raw_unpaired["output"][0]["content"][0]["text"] = "\ud800"
        raw_unpaired_result = run_semantic_review(
            self.request,
            prompt=self.prompt,
            output_schema=self.model_output_schema,
            transport=OfflineFakeResponsesTransport(raw_unpaired),
        )
        self.assertEqual(raw_unpaired_result["error_codes"], ["response_invalid"])
        raw_unpaired_key = copy.deepcopy(self.raw_response)
        raw_unpaired_key["bad\ud800key"] = "value"
        raw_unpaired_key_result = run_semantic_review(
            self.request,
            prompt=self.prompt,
            output_schema=self.model_output_schema,
            transport=OfflineFakeResponsesTransport(raw_unpaired_key),
        )
        self.assertEqual(raw_unpaired_key_result["error_codes"], ["response_invalid"])

        oversized_reasoning = copy.deepcopy(self.raw_response)
        oversized_reasoning["output"].insert(
            0,
            {
                "id": "rs_fixture_semantic_v2",
                "type": "reasoning",
                "status": "completed",
                "summary": [{"type": "summary_text", "text": "x" * 100_000}],
            },
        )
        oversized_reasoning_result = run_semantic_review(
            self.request,
            prompt=self.prompt,
            output_schema=self.model_output_schema,
            transport=OfflineFakeResponsesTransport(oversized_reasoning),
        )
        self.assertEqual(oversized_reasoning_result["error_codes"], ["budget_exceeded"])

        oversized_diagnostic = copy.deepcopy(self.raw_response)
        oversized_diagnostic["diagnostic"] = "x" * 100_000
        oversized_diagnostic_result = run_semantic_review(
            self.request,
            prompt=self.prompt,
            output_schema=self.model_output_schema,
            transport=OfflineFakeResponsesTransport(oversized_diagnostic),
        )
        self.assertEqual(oversized_diagnostic_result["error_codes"], ["budget_exceeded"])

        oversized_canonical = copy.deepcopy(self.raw_response)
        oversized_canonical["diagnostic"] = ["x" * 10_000 for _ in range(30)]
        oversized_canonical_result = run_semantic_review(
            self.request,
            prompt=self.prompt,
            output_schema=self.model_output_schema,
            transport=OfflineFakeResponsesTransport(oversized_canonical),
        )
        self.assertEqual(oversized_canonical_result["error_codes"], ["budget_exceeded"])

        deep: Any = None
        for _ in range(1500):
            deep = {"child": deep}
        deep_response = copy.deepcopy(self.raw_response)
        deep_response["diagnostic"] = deep
        deep_result = run_semantic_review(
            self.request,
            prompt=self.prompt,
            output_schema=self.model_output_schema,
            transport=OfflineFakeResponsesTransport(deep_response),
        )
        self.assertEqual(deep_result["error_codes"], ["budget_exceeded"])

        too_many_nodes = copy.deepcopy(self.raw_response)
        too_many_nodes["diagnostic"] = [None] * 5000
        node_result = run_semantic_review(
            self.request,
            prompt=self.prompt,
            output_schema=self.model_output_schema,
            transport=OfflineFakeResponsesTransport(too_many_nodes),
        )
        self.assertEqual(node_result["error_codes"], ["budget_exceeded"])

        over_usage = _fake_response(
            self.model_output,
            usage={"input_tokens": 4000, "output_tokens": 1601, "total_tokens": 5601},
        )
        usage_result = run_semantic_review(
            self.request,
            prompt=self.prompt,
            output_schema=self.model_output_schema,
            transport=OfflineFakeResponsesTransport(over_usage),
        )
        self.assertEqual(usage_result["error_codes"], ["budget_exceeded"])

        with self.assertRaises(ValueError):
            canonical_json({"non_finite": float("nan")})
        non_finite_response = copy.deepcopy(self.raw_response)
        non_finite_response["diagnostic"] = float("nan")
        non_finite_result = run_semantic_review(
            self.request,
            prompt=self.prompt,
            output_schema=self.model_output_schema,
            transport=OfflineFakeResponsesTransport(non_finite_response),
        )
        self.assertEqual(non_finite_result["error_codes"], ["response_invalid"])

    def test_request_without_exact_binding_produces_no_review_artifact(self) -> None:
        transport = OfflineFakeResponsesTransport(self.raw_response)
        subject_conflict = copy.deepcopy(self.request)
        subject_conflict["subject"]["platform_user_id"] = "900000000000000099"
        bio_hash_conflict = copy.deepcopy(self.request)
        bio_hash_conflict["profile_snapshot"]["bio_sha256"] = "0" * 64
        for request in (None, {}, {"request_id": "bad"}, subject_conflict, bio_hash_conflict):
            with self.subTest(request=request), self.assertRaisesRegex(ValueError, "request_binding_invalid"):
                run_semantic_review(
                    request,
                    prompt=self.prompt,
                    output_schema=self.model_output_schema,
                    transport=transport,
                )
        self.assertEqual(transport.calls, 0)

    def test_review_recomputation_rejects_identity_usage_prompt_schema_and_authority_tampering(self) -> None:
        mutations = []
        for path, value in (
            (("platform_user_id",), "900000000000000099"),
            (("bio_sha256",), "0" * 64),
            (("model_receipt", "prompt_sha256"), "0" * 64),
            (("model_receipt", "output_schema_sha256"), "0" * 64),
            (("usage", "total_tokens"), 901),
            (("authority", "discovery_or_ranking_authorized"), True),
            (("proposals", 0, "excerpt_sha256"), "0" * 64),
        ):
            payload = copy.deepcopy(self.review)
            owner: Any = payload
            for token in path[:-1]:
                owner = owner[token]
            owner[path[-1]] = value
            mutations.append(payload)
        for review in mutations:
            with self.subTest(review=canonical_sha256(review)):
                self.assertTrue(
                    validate_review(
                        review,
                        request=self.request,
                        prompt=self.prompt,
                        output_schema=self.model_output_schema,
                        raw_response=self.raw_response,
                    )
                )

    def test_terminal_status_schema_and_recomputation_are_total(self) -> None:
        live_request = copy.deepcopy(self.request)
        live_request["model_execution_mode"] = "live_canary"

        class NeverLiveTransport:
            is_live = True
            transport_id = "openai_compatible_responses"

            def create_response(self, payload: Any, *, timeout_ms: int) -> dict[str, Any]:
                raise AssertionError("blocked before call")

        blocked = run_semantic_review(
            live_request,
            prompt=self.prompt,
            output_schema=self.model_output_schema,
            transport=NeverLiveTransport(),
        )
        self.assertEqual(_schema_errors(blocked, self.review_schema), [])
        self.assertEqual(
            validate_review(
                blocked,
                request=live_request,
                prompt=self.prompt,
                output_schema=self.model_output_schema,
                raw_response=None,
            ),
            [],
        )

        bad_raw_response = copy.deepcopy(self.raw_response)
        bad_raw_response["output"][0]["status"] = "incomplete"
        failed = run_semantic_review(
            self.request,
            prompt=self.prompt,
            output_schema=self.model_output_schema,
            transport=OfflineFakeResponsesTransport(bad_raw_response),
        )
        self.assertEqual(_schema_errors(failed, self.review_schema), [])
        self.assertEqual(
            validate_review(
                failed,
                request=self.request,
                prompt=self.prompt,
                output_schema=self.model_output_schema,
                raw_response=bad_raw_response,
            ),
            [],
        )

        incoherent_failed = copy.deepcopy(self.review)
        incoherent_failed["status"] = "failed"
        incoherent_failed["error_codes"] = []
        self.assertTrue(_schema_errors(incoherent_failed, self.review_schema))
        self.assertTrue(
            validate_review(
                incoherent_failed,
                request=self.request,
                prompt=self.prompt,
                output_schema=self.model_output_schema,
                raw_response=self.raw_response,
            )
        )

    def test_schema_and_runtime_close_extra_fields_and_policy_mutations(self) -> None:
        request_extra = copy.deepcopy(self.request)
        request_extra["rank_candidate"] = True
        self.assertTrue(_schema_errors(request_extra, self.request_schema))
        self.assertTrue(validate_request(request_extra, prompt=self.prompt, output_schema=self.model_output_schema))

        output_extra = copy.deepcopy(self.model_output)
        output_extra["proposals"][0]["external_fact"] = "unsupported"
        self.assertTrue(_schema_errors(output_extra, self.model_output_schema))
        self.assertTrue(validate_model_output(output_extra, request=self.request))

        policy_mutation = copy.deepcopy(self.request)
        policy_mutation["model_policy"]["model_id"] = "gpt-5.6-sol"
        self.assertTrue(validate_request(policy_mutation, prompt=self.prompt, output_schema=self.model_output_schema))
        effort_mutation = copy.deepcopy(self.request)
        effort_mutation["model_policy"]["reasoning_effort"] = "high"
        self.assertTrue(validate_request(effort_mutation, prompt=self.prompt, output_schema=self.model_output_schema))
        budget_mutation = copy.deepcopy(self.request)
        budget_mutation["budgets"]["max_output_tokens"] += 1
        self.assertTrue(validate_request(budget_mutation, prompt=self.prompt, output_schema=self.model_output_schema))


if __name__ == "__main__":
    unittest.main()
