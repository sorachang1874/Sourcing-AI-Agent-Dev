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

from x_first import profile_bio_semantic_v2 as semantic  # noqa: E402
from x_first.profile_bio_semantic_v2 import (  # noqa: E402
    BUDGETS,
    CANONICAL_OUTPUT_SCHEMA_SHA256,
    CANONICAL_PROMPT_SHA256,
    CANONICAL_PROXY_POLICY_SHA256,
    EXPLANATION_TEMPLATES_BY_SEMANTIC_STATE,
    MODEL_AUTHORITY,
    MODEL_ID,
    MODEL_OUTPUT_SCHEMA_VERSION,
    PURE_ADJUDICATION_API_VERSION,
    PURE_ADJUDICATION_IMPLEMENTATION_SHA256,
    REASON_SOURCE,
    OfflineFakeResponsesTransport,
    adjudicate_observed_response,
    build_responses_request,
    canonical_json,
    canonical_sha256,
    json_type_strict_equal,
    load_json,
    pure_adjudication_implementation_manifest,
    run_semantic_review,
    structured_output_schema,
    text_sha256,
    validate_model_output,
    validate_output_schema,
    validate_prompt,
    validate_proxy_policy,
    validate_pure_adjudication_implementation,
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
    if "not" in schema and not _schema_errors(instance, schema["not"], path=path):
        errors.append(f"{path}: prohibited schema match")
    if "const" in schema and not json_type_strict_equal(instance, schema["const"]):
        errors.append(f"{path}: const mismatch")
    if "enum" in schema and not any(json_type_strict_equal(instance, option) for option in schema["enum"]):
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


def _offline_completed_execution_receipt() -> dict[str, Any]:
    return {
        "mode": "offline_fake",
        "transport_id": "offline_fake_responses",
        "execute_live": False,
        "transport_invocations": 1,
        "provider_external_calls": 0,
        "response_received": True,
        "fallback_used": False,
        "phase": "completed",
        "contract_error": "none",
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
        cls.proxy_policy = load_json(ROOT / "configs/profile_bio_professional_experience_proxy_policy.v1.json")
        cls.proxy_policy_schema = load_json(
            ROOT / "contracts/x.profile.bio_professional_experience_proxy_policy.v1.schema.json"
        )
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
        reason: str | None = None,
    ) -> dict[str, Any]:
        return {
            "proposal_type": proposal_type,
            "relation_state": relation_state,
            "span_start": 0,
            "span_end": len(bio),
            "excerpt": bio,
            "reason_codes": [reason_code],
            "reason": reason or EXPLANATION_TEMPLATES_BY_SEMANTIC_STATE[(proposal_type, relation_state)],
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
        self.assertEqual(validate_proxy_policy(self.proxy_policy), [])
        self.assertEqual(canonical_sha256(self.proxy_policy), CANONICAL_PROXY_POLICY_SHA256)
        self.assertEqual(_schema_errors(self.proxy_policy, self.proxy_policy_schema), [])
        self.assertEqual(
            validate_request(
                self.request,
                prompt=self.prompt,
                output_schema=self.model_output_schema,
                proxy_policy=self.proxy_policy,
            ),
            [],
        )
        self.assertEqual(validate_model_output(self.model_output, request=self.request), [])
        self.assertEqual(_schema_errors(self.request, self.request_schema), [])
        self.assertEqual(_schema_errors(self.model_output, self.model_output_schema), [])
        self.assertEqual(_schema_errors(self.review, self.review_schema), [])
        _assert_strict_object_schemas(self, self.proxy_policy_schema)
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
        self.assertEqual(
            payload["metadata"]["professional_experience_proxy_policy_sha256"],
            CANONICAL_PROXY_POLICY_SHA256,
        )
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
        self.assertEqual(
            validate_review(
                observed,
                request=self.request,
                prompt=self.prompt,
                output_schema=self.model_output_schema,
                raw_response=self.raw_response,
                execution_attempt=observed["execution"],
            ),
            [
                "execution_attempt: semantic replay rejects caller-supplied execution authority; "
                "use the outer Luna receipt/ledger validator"
            ],
        )
        self.assertEqual(
            validate_review(
                observed,
                request=self.request,
                prompt=self.prompt,
                output_schema=self.model_output_schema,
                raw_response=self.raw_response,
                execution_attempt=copy.deepcopy(observed["execution"]),
            ),
            [
                "execution_attempt: semantic replay rejects caller-supplied execution authority; "
                "use the outer Luna receipt/ledger validator"
            ],
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

        accepted_wire_variants = []
        reasoning_without_status = copy.deepcopy(reasoning_response)
        del reasoning_without_status["output"][0]["status"]
        accepted_wire_variants.append(reasoning_without_status)
        reasoning_without_summary = copy.deepcopy(reasoning_response)
        del reasoning_without_summary["output"][0]["summary"]
        accepted_wire_variants.append(reasoning_without_summary)
        empty_logprobs = copy.deepcopy(self.raw_response)
        empty_logprobs["output"][0]["content"][0]["logprobs"] = []
        accepted_wire_variants.append(empty_logprobs)
        null_logprobs = copy.deepcopy(self.raw_response)
        null_logprobs["output"][0]["content"][0]["logprobs"] = None
        accepted_wire_variants.append(null_logprobs)
        final_phase = copy.deepcopy(self.raw_response)
        final_phase["output"][0]["phase"] = "final_answer"
        accepted_wire_variants.append(final_phase)
        for raw_response in accepted_wire_variants:
            with self.subTest(accepted_wire_case=canonical_sha256(raw_response)):
                result = run_semantic_review(
                    self.request,
                    prompt=self.prompt,
                    output_schema=self.model_output_schema,
                    transport=OfflineFakeResponsesTransport(raw_response),
                )
                self.assertEqual(result["status"], "completed")

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
                        transport=OfflineFakeResponsesTransport(raw_response),
                        execute_live=False,
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

    def test_public_pure_adjudication_api_is_digest_bound_and_transport_free(self) -> None:
        self.assertEqual(PURE_ADJUDICATION_API_VERSION, "x.profile.bio_semantic.pure_adjudication.v1")
        self.assertEqual(validate_pure_adjudication_implementation(), [])
        implementation_manifest = pure_adjudication_implementation_manifest()
        self.assertEqual(canonical_sha256(implementation_manifest), PURE_ADJUDICATION_IMPLEMENTATION_SHA256)
        self.assertEqual(
            implementation_manifest["assurance_scope"],
            "trusted_runtime_drift_detection_only",
        )
        self.assertIs(implementation_manifest["independent_integrity_claimed"], False)
        implementation_bundle = implementation_manifest["implementation_bundle"]
        self.assertEqual(
            implementation_bundle["format"],
            "cpython-transitive-code-and-dependency-manifest-v1",
        )
        self.assertIn("adjudicate_observed_response", implementation_bundle["controlled_symbol_sha256"])
        self.assertIn("_execution_record_errors", implementation_bundle["controlled_symbol_sha256"])
        self.assertIn("REVIEW_AUTHORITY", implementation_bundle["dependency_manifest"])

        original_execution_validator = semantic._execution_record_errors

        def tampered_execution_validator(execution: Any) -> list[str]:
            return original_execution_validator(execution)

        semantic._execution_record_errors = tampered_execution_validator
        try:
            self.assertEqual(
                validate_pure_adjudication_implementation(),
                ["pure_adjudication_implementation_digest_mismatch"],
            )
        finally:
            semantic._execution_record_errors = original_execution_validator
        self.assertEqual(validate_pure_adjudication_implementation(), [])
        response_attempt = {
            "mode": "offline_fake",
            "transport_id": "offline_fake_responses",
            "execute_live": False,
            "transport_invocations": 1,
            "provider_external_calls": 0,
            "response_received": True,
            "fallback_used": False,
            "phase": "response",
            "contract_error": "none",
        }
        self.assertEqual(
            adjudicate_observed_response(
                request=self.request,
                prompt=self.prompt,
                output_schema=self.model_output_schema,
                proxy_policy=self.proxy_policy,
                execution_attempt=response_attempt,
                raw_response=self.raw_response,
            ),
            self.review,
        )
        transport_attempt = {
            **response_attempt,
            "response_received": False,
            "phase": "transport",
        }
        transport_failed = adjudicate_observed_response(
            request=self.request,
            prompt=self.prompt,
            output_schema=self.model_output_schema,
            proxy_policy=self.proxy_policy,
            execution_attempt=transport_attempt,
            raw_response=None,
        )
        self.assertEqual(transport_failed["error_codes"], ["transport_failed"])
        self.assertEqual(transport_failed["execution"], transport_attempt)
        invalid_attempt = {**response_attempt, "transport_invocations": True}
        with self.assertRaisesRegex(ValueError, "execution_attempt_invalid"):
            adjudicate_observed_response(
                request=self.request,
                prompt=self.prompt,
                output_schema=self.model_output_schema,
                proxy_policy=self.proxy_policy,
                execution_attempt=invalid_attempt,
                raw_response=self.raw_response,
            )

        live_request = copy.deepcopy(self.request)
        live_request["model_execution_mode"] = "live_canary"
        live_attempt = {
            **response_attempt,
            "mode": "live",
            "transport_id": "openai_compatible_responses",
            "execute_live": True,
            "provider_external_calls": 1,
        }
        live_review = adjudicate_observed_response(
            request=live_request,
            prompt=self.prompt,
            output_schema=self.model_output_schema,
            proxy_policy=self.proxy_policy,
            execution_attempt=live_attempt,
            raw_response=self.raw_response,
        )
        self.assertIs(live_review["usage"]["billable"], True)
        self.assertEqual(
            validate_review(
                live_review,
                request=live_request,
                prompt=self.prompt,
                output_schema=self.model_output_schema,
                proxy_policy=self.proxy_policy,
                raw_response=self.raw_response,
            ),
            [
                "live_execution_authority: semantic replay cannot validate provider calls or billing; "
                "use the outer Luna receipt/ledger validator"
            ],
        )
        self.assertEqual(
            validate_review(
                live_review,
                request=live_request,
                prompt=self.prompt,
                output_schema=self.model_output_schema,
                proxy_policy=self.proxy_policy,
                raw_response=self.raw_response,
                execution_attempt=live_attempt,
            ),
            [
                "execution_attempt: semantic replay rejects caller-supplied execution authority; "
                "use the outer Luna receipt/ledger validator"
            ],
        )

    def test_nine_synthetic_bios_cover_explicit_proxy_and_empty_semantics(self) -> None:
        cases = (
            (
                "在新加坡从事模型训练研究。",
                "professional_region_experience",
                "not_applicable",
                "explicit_professional_region_experience",
                None,
            ),
            (
                "小红书同名四万粉丝",
                "professional_china_digital_ecosystem",
                "not_applicable",
                "explicit_china_digital_ecosystem_activity",
                None,
            ),
            (
                "公众号 SyntheticFounder（长文首发）",
                "professional_china_digital_ecosystem",
                "not_applicable",
                "explicit_china_digital_ecosystem_activity",
                None,
            ),
            (
                "Head of growth @synthetic_hub",
                "professional_affiliation",
                "current_claimed",
                "explicit_current_organization_claim",
                None,
            ),
            (
                "Prev @synth_listen",
                "professional_affiliation",
                "previous_claimed",
                "explicit_previous_organization_claim",
                None,
            ),
            (
                "Future Researcher at @synthetic_lab",
                "professional_affiliation",
                "future_or_aspirational",
                "ambiguous_professional_context_requires_review",
                None,
            ),
            (
                "分享中文 AI 工程实践。",
                "observed_chinese_professional_content",
                "not_applicable",
                "observed_chinese_professional_content",
                None,
            ),
            ("喜欢中文电影。", None, None, None, None),
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
                expected_proxy = (
                    "strong_proxy"
                    if proposal_type == "professional_china_digital_ecosystem"
                    else "weak_proxy"
                    if proposal_type == "observed_chinese_professional_content"
                    else "none"
                )
                rollup = result["professional_experience_proxy_rollup"]
                expected_status = "unverified_model_derived" if expected_proxy != "none" else "none"
                self.assertEqual(rollup["status"], expected_status)
                self.assertEqual(
                    {region: value["strength"] for region, value in rollup["regions"].items()},
                    {"china": expected_proxy, "asia": expected_proxy},
                )
                if proposal is None:
                    self.assertEqual(result["proposals"], [])
                else:
                    observed = result["proposals"][0]
                    self.assertEqual(observed["proposal_type"], proposal_type)
                    self.assertEqual(observed["excerpt_sha256"], text_sha256(bio))
                    self.assertEqual(observed["reason_source"], REASON_SOURCE)
                    self.assertEqual(observed["verification_status"], "unverified_professional_context_proposal")
                for region_rollup in rollup["regions"].values():
                    if expected_proxy == "none":
                        self.assertEqual(region_rollup["contributing_proposals"], [])
                    else:
                        self.assertEqual(
                            region_rollup["contributing_proposals"],
                            [
                                {
                                    "proposal_id": result["proposals"][0]["proposal_id"],
                                    "proposal_type": proposal_type,
                                    "confidence": "high",
                                    "mapped_strength": expected_proxy,
                                }
                            ],
                        )

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
                        transport=OfflineFakeResponsesTransport(_fake_response(model_output)),
                        execute_live=False,
                    ),
                    [],
                )

        natural_paraphrase = copy.deepcopy(self.model_output)
        natural_paraphrase["proposals"][0]["reason"] = (
            "The cited Bio passage directly supports this professional-context proposal; verify it independently."
        )
        self.assertTrue(validate_model_output(natural_paraphrase, request=self.request))

        confidence_only = copy.deepcopy(self.model_output)
        confidence_only["proposals"][0]["confidence"] = "low"
        confidence_review = run_semantic_review(
            self.request,
            prompt=self.prompt,
            output_schema=self.model_output_schema,
            transport=OfflineFakeResponsesTransport(_fake_response(confidence_only)),
        )
        self.assertEqual(confidence_review["status"], "completed")
        self.assertEqual(confidence_review["proposals"][0]["proposal_id"], self.review["proposals"][0]["proposal_id"])

        for forbidden_reason in ("Anthropic in Canada.", "该用户是汉族并具有韩国国籍。"):
            invented_narrative = copy.deepcopy(self.model_output)
            invented_narrative["proposals"][0]["reason"] = forbidden_reason
            self.assertTrue(validate_model_output(invented_narrative, request=self.request))
            self.assertTrue(_schema_errors(invented_narrative, self.model_output_schema))
            invented_raw_response = _fake_response(invented_narrative)
            invented_transport = OfflineFakeResponsesTransport(invented_raw_response)
            invented_review = run_semantic_review(
                self.request,
                prompt=self.prompt,
                output_schema=self.model_output_schema,
                transport=invented_transport,
            )
            self.assertEqual(invented_review["error_codes"], ["model_output_invalid"])
            self.assertEqual(
                validate_review(
                    invented_review,
                    request=self.request,
                    prompt=self.prompt,
                    output_schema=self.model_output_schema,
                    raw_response=invented_raw_response,
                    transport=invented_transport,
                    execute_live=False,
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
                transport=transport,
                execute_live=False,
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
                transport=transport,
                execute_live=True,
            )
        )

        for invalid_execute_live in ("false", 1, None, []):
            invalid_flag_transport = NeverLiveTransport()
            result = run_semantic_review(
                live_request,
                prompt=self.prompt,
                output_schema=self.model_output_schema,
                transport=invalid_flag_transport,
                execute_live=invalid_execute_live,
            )
            self.assertEqual(result["error_codes"], ["execution_contract_invalid"])
            self.assertEqual(result["execution"]["contract_error"], "invalid_execute_live")
            self.assertEqual(invalid_flag_transport.calls, 0)
            self.assertEqual(_schema_errors(result, self.review_schema), [])
            self.assertEqual(
                validate_review(
                    result,
                    request=live_request,
                    prompt=self.prompt,
                    output_schema=self.model_output_schema,
                    raw_response=None,
                    transport=invalid_flag_transport,
                    execute_live=invalid_execute_live,
                ),
                [],
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
                    transport=invalid_transport,
                    execute_live=execute_live,
                ),
                [],
            )
        native_source = copy.deepcopy(live_request)
        native_source["profile_source_mode"] = "native_x"
        native_source["profile_snapshot"]["profile_url"] = "https://x.com/semanticfixture"
        self.assertTrue(validate_request(native_source, prompt=self.prompt, output_schema=self.model_output_schema))
        self.assertTrue(_schema_errors(native_source, self.request_schema))

        source = (ROOT / "src/x_first/profile_bio_semantic_v2.py").read_text(encoding="utf-8")
        imported = {
            alias.name for node in ast.walk(ast.parse(source)) if isinstance(node, ast.Import) for alias in node.names
        }
        imported.update(
            node.module for node in ast.walk(ast.parse(source)) if isinstance(node, ast.ImportFrom) and node.module
        )
        self.assertFalse({"httpx", "openai", "requests", "socket", "urllib.request"} & imported)

    def test_failed_state_recomputation_rejects_coherent_forgery(self) -> None:
        class LiveTransport:
            is_live = True
            transport_id = "openai_compatible_responses"

            def __init__(self, response: Any) -> None:
                self.response = response
                self.calls = 0

            def create_response(self, payload: Any, *, timeout_ms: int) -> Any:
                self.calls += 1
                return self.response

        live_request = copy.deepcopy(self.request)
        live_request["model_execution_mode"] = "live_canary"
        live_transport = LiveTransport(self.raw_response)
        blocked = run_semantic_review(
            live_request,
            prompt=self.prompt,
            output_schema=self.model_output_schema,
            transport=live_transport,
            execute_live=False,
        )
        forged_blocked = copy.deepcopy(blocked)
        forged_blocked["status"] = "failed"
        forged_blocked["error_codes"] = ["transport_failed"]
        self.assertTrue(_schema_errors(forged_blocked, self.review_schema))
        self.assertTrue(
            validate_review(
                forged_blocked,
                request=live_request,
                prompt=self.prompt,
                output_schema=self.model_output_schema,
                raw_response=None,
                transport=live_transport,
                execute_live=False,
            )
        )

        mismatch = run_semantic_review(
            self.request,
            prompt=self.prompt,
            output_schema=self.model_output_schema,
            transport=live_transport,
            execute_live=True,
        )
        fully_relabelled = copy.deepcopy(mismatch)
        fully_relabelled["error_codes"] = ["transport_failed"]
        fully_relabelled["execution"] = {
            "mode": "offline_fake",
            "transport_id": "offline_fake_responses",
            "execute_live": False,
            "transport_invocations": 1,
            "provider_external_calls": 0,
            "response_received": False,
            "fallback_used": False,
            "phase": "transport",
            "contract_error": "none",
        }
        self.assertEqual(_schema_errors(fully_relabelled, self.review_schema), [])
        self.assertTrue(
            validate_review(
                fully_relabelled,
                request=self.request,
                prompt=self.prompt,
                output_schema=self.model_output_schema,
                raw_response=None,
                transport=live_transport,
                execute_live=True,
            )
        )

        mismatch_with_response = copy.deepcopy(mismatch)
        mismatch_with_response["execution"]["transport_invocations"] = 1
        mismatch_with_response["execution"]["provider_external_calls"] = 1
        mismatch_with_response["execution"]["response_received"] = True
        mismatch_with_response["execution"]["phase"] = "response"
        self.assertTrue(
            validate_review(
                mismatch_with_response,
                request=self.request,
                prompt=self.prompt,
                output_schema=self.model_output_schema,
                raw_response=self.raw_response,
                transport=live_transport,
                execute_live=True,
            )
        )

        bad_raw = copy.deepcopy(self.raw_response)
        bad_raw["output"][0]["status"] = "incomplete"
        offline_transport = OfflineFakeResponsesTransport(bad_raw)
        failed = run_semantic_review(
            self.request,
            prompt=self.prompt,
            output_schema=self.model_output_schema,
            transport=offline_transport,
        )
        forged_calls = copy.deepcopy(failed)
        forged_calls["execution"]["provider_external_calls"] = 1
        self.assertTrue(_schema_errors(forged_calls, self.review_schema))
        self.assertTrue(
            validate_review(
                forged_calls,
                request=self.request,
                prompt=self.prompt,
                output_schema=self.model_output_schema,
                raw_response=bad_raw,
                transport=offline_transport,
                execute_live=False,
            )
        )

    def test_live_and_offline_transport_failures_bind_exact_call_evidence(self) -> None:
        class FailingTransport:
            def __init__(self, *, is_live: bool, transport_id: str) -> None:
                self.is_live = is_live
                self.transport_id = transport_id
                self.calls = 0

            def create_response(self, payload: Any, *, timeout_ms: int) -> Any:
                self.calls += 1
                raise RuntimeError("synthetic failure")

        live_request = copy.deepcopy(self.request)
        live_request["model_execution_mode"] = "live_canary"
        cases = (
            (self.request, FailingTransport(is_live=False, transport_id="offline_fake_responses"), False, 0),
            (live_request, FailingTransport(is_live=True, transport_id="openai_compatible_responses"), True, 1),
        )
        for request, transport, execute_live, external_calls in cases:
            with self.subTest(live=transport.is_live):
                result = run_semantic_review(
                    request,
                    prompt=self.prompt,
                    output_schema=self.model_output_schema,
                    transport=transport,
                    execute_live=execute_live,
                )
                self.assertEqual(transport.calls, 1)
                self.assertEqual(result["error_codes"], ["transport_failed"])
                self.assertEqual(result["execution"]["phase"], "transport")
                self.assertEqual(result["execution"]["transport_invocations"], 1)
                self.assertEqual(result["execution"]["provider_external_calls"], external_calls)
                self.assertEqual(_schema_errors(result, self.review_schema), [])
                self.assertEqual(
                    validate_review(
                        result,
                        request=request,
                        prompt=self.prompt,
                        output_schema=self.model_output_schema,
                        raw_response=None,
                        transport=transport,
                        execute_live=execute_live,
                    ),
                    [],
                )

    def test_malformed_json_types_and_transport_metadata_are_terminal_total(self) -> None:
        class MetadataTransport:
            def __init__(self, *, is_live: Any, transport_id: Any) -> None:
                self.is_live = is_live
                self.transport_id = transport_id
                self.calls = 0

            def create_response(self, payload: Any, *, timeout_ms: int) -> Any:
                self.calls += 1
                return self.raw_response  # type: ignore[attr-defined]

        for is_live, transport_id, contract_error in (
            (False, [], "invalid_transport_id"),
            (False, "unknown_transport", "invalid_transport_id"),
            (None, "offline_fake_responses", "invalid_is_live"),
        ):
            transport = MetadataTransport(is_live=is_live, transport_id=transport_id)
            result = run_semantic_review(
                self.request,
                prompt=self.prompt,
                output_schema=self.model_output_schema,
                transport=transport,
            )
            self.assertEqual(result["error_codes"], ["execution_contract_invalid"])
            self.assertEqual(result["execution"]["contract_error"], contract_error)
            self.assertEqual(transport.calls, 0)
            self.assertEqual(_schema_errors(result, self.review_schema), [])
            self.assertEqual(
                validate_review(
                    result,
                    request=self.request,
                    prompt=self.prompt,
                    output_schema=self.model_output_schema,
                    raw_response=None,
                    transport=transport,
                    execute_live=False,
                ),
                [],
            )

        malformed_request = copy.deepcopy(self.request)
        malformed_request["model_execution_mode"] = []
        request_transport = OfflineFakeResponsesTransport(self.raw_response)
        request_result = run_semantic_review(
            malformed_request,
            prompt=self.prompt,
            output_schema=self.model_output_schema,
            transport=request_transport,
        )
        self.assertEqual(request_result["error_codes"], ["request_invalid"])
        self.assertEqual(request_transport.calls, 0)

        for path, value in (
            (("verdict",), []),
            (("proposals", 0, "proposal_type"), []),
            (("proposals", 0, "relation_state"), {}),
            (("proposals", 0, "confidence"), []),
        ):
            malformed_output = copy.deepcopy(self.model_output)
            owner: Any = malformed_output
            for token in path[:-1]:
                owner = owner[token]
            owner[path[-1]] = value
            result = run_semantic_review(
                self.request,
                prompt=self.prompt,
                output_schema=self.model_output_schema,
                transport=OfflineFakeResponsesTransport(_fake_response(malformed_output)),
            )
            self.assertEqual(result["error_codes"], ["model_output_invalid"])

        malformed_schema = copy.deepcopy(self.model_output_schema)
        malformed_schema["properties"] = []
        self.assertTrue(validate_output_schema(malformed_schema))
        malformed_review = copy.deepcopy(self.review)
        malformed_review["execution"]["mode"] = []
        self.assertTrue(
            validate_review(
                malformed_review,
                request=self.request,
                prompt=self.prompt,
                output_schema=self.model_output_schema,
                raw_response=self.raw_response,
                execution_attempt=_offline_completed_execution_receipt(),
            )
        )

    def test_breadth_and_request_byte_budgets_reject_before_unbounded_work(self) -> None:
        class CountingList(list[Any]):
            def __init__(self, values: list[Any]) -> None:
                super().__init__(values)
                self.iterated = 0

            def __iter__(self):  # type: ignore[no-untyped-def]
                for value in super().__iter__():
                    self.iterated += 1
                    yield value

        wide = CountingList([None] * 50_000)
        self.assertEqual(
            semantic._scan_json(wide, max_depth=64, max_nodes=4096),  # noqa: SLF001
            ["nested_node_budget_exceeded"],
        )
        self.assertEqual(wide.iterated, 0)

        oversized_request = copy.deepcopy(self.request)
        for index in range(6):
            oversized_request[f"padding_{index}"] = "x" * 12_000
        self.assertEqual(
            validate_request(oversized_request, prompt=self.prompt, output_schema=self.model_output_schema),
            ["$.validation: request_canonical_byte_budget_exceeded"],
        )
        self.assertEqual(BUDGETS["max_request_canonical_bytes"], 65_536)

        contract = (ROOT / "docs/PROFILE_BIO_SEMANTIC_V2_CONTRACT.md").read_text(encoding="utf-8")
        self.assertNotIn("frozen v1.4 baseline", contract)
        self.assertIn("evaluation manifest", contract)

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
            (("professional_experience_proxy_policy", "observed", "policy_sha256"), "0" * 64),
            (("professional_experience_proxy_policy", "validation_status"), "invalid"),
            (("professional_experience_proxy_rollup", "status"), "none"),
            (("professional_experience_proxy_rollup", "regions", "china", "strength"), "weak_proxy"),
            (
                (
                    "professional_experience_proxy_rollup",
                    "regions",
                    "china",
                    "contributing_proposals",
                    0,
                    "proposal_id",
                ),
                "xbsv2p_000000000000000000000000",
            ),
            (
                (
                    "professional_experience_proxy_rollup",
                    "regions",
                    "asia",
                    "contributing_proposals",
                    0,
                    "mapped_strength",
                ),
                "strong_proxy",
            ),
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
                        execution_attempt=_offline_completed_execution_receipt(),
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

        blocked_transport = NeverLiveTransport()
        blocked = run_semantic_review(
            live_request,
            prompt=self.prompt,
            output_schema=self.model_output_schema,
            transport=blocked_transport,
        )
        self.assertEqual(_schema_errors(blocked, self.review_schema), [])
        self.assertEqual(
            validate_review(
                blocked,
                request=live_request,
                prompt=self.prompt,
                output_schema=self.model_output_schema,
                raw_response=None,
                transport=blocked_transport,
                execute_live=False,
            ),
            [],
        )

        bad_raw_response = copy.deepcopy(self.raw_response)
        bad_raw_response["output"][0]["status"] = "incomplete"
        failed_transport = OfflineFakeResponsesTransport(bad_raw_response)
        failed = run_semantic_review(
            self.request,
            prompt=self.prompt,
            output_schema=self.model_output_schema,
            transport=failed_transport,
        )
        self.assertEqual(_schema_errors(failed, self.review_schema), [])
        self.assertEqual(
            validate_review(
                failed,
                request=self.request,
                prompt=self.prompt,
                output_schema=self.model_output_schema,
                raw_response=bad_raw_response,
                transport=failed_transport,
                execute_live=False,
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
                execution_attempt=_offline_completed_execution_receipt(),
            )
        )

    def test_proxy_policy_is_strict_pinned_and_fails_before_transport(self) -> None:
        self.assertEqual(validate_proxy_policy(self.proxy_policy), [])
        self.assertEqual(_schema_errors(self.proxy_policy, self.proxy_policy_schema), [])
        mutations = []
        changed_strength = copy.deepcopy(self.proxy_policy)
        changed_strength["proposal_mappings"]["observed_chinese_professional_content"]["strength"] = "strong_proxy"
        mutations.append(changed_strength)
        changed_region = copy.deepcopy(self.proxy_policy)
        changed_region["proposal_mappings"]["professional_china_digital_ecosystem"]["regions"] = ["china"]
        mutations.append(changed_region)
        malformed_region = copy.deepcopy(self.proxy_policy)
        malformed_region["proposal_mappings"]["professional_china_digital_ecosystem"]["regions"] = [{}]
        mutations.append(malformed_region)
        changed_boundary = copy.deepcopy(self.proxy_policy)
        changed_boundary["usage_boundary"]["final_eligibility_or_ranking"] = True
        mutations.append(changed_boundary)
        extra_mapping = copy.deepcopy(self.proxy_policy)
        extra_mapping["proposal_mappings"]["professional_affiliation"] = {
            "strength": "weak_proxy",
            "regions": ["china", "asia"],
        }
        mutations.append(extra_mapping)
        for proxy_policy in mutations:
            with self.subTest(proxy_policy=canonical_sha256(proxy_policy)):
                self.assertTrue(validate_proxy_policy(proxy_policy))
                self.assertTrue(_schema_errors(proxy_policy, self.proxy_policy_schema))

        transport = OfflineFakeResponsesTransport(self.raw_response)
        failed = run_semantic_review(
            self.request,
            prompt=self.prompt,
            output_schema=self.model_output_schema,
            proxy_policy=changed_strength,
            transport=transport,
        )
        self.assertEqual(failed["error_codes"], ["proxy_policy_invalid"])
        self.assertEqual(
            failed["execution"],
            {
                "mode": "not_inspected",
                "transport_id": "not_inspected",
                "execute_live": None,
                "transport_invocations": 0,
                "provider_external_calls": 0,
                "response_received": False,
                "fallback_used": False,
                "phase": "preflight",
                "contract_error": "not_evaluated",
            },
        )
        self.assertEqual(
            failed["professional_experience_proxy_policy"],
            {
                "expected": {
                    "policy_version": "profile-bio-professional-experience-proxy-policy-v1",
                    "policy_sha256": CANONICAL_PROXY_POLICY_SHA256,
                },
                "observed": {
                    "policy_version": "profile-bio-professional-experience-proxy-policy-v1",
                    "policy_sha256": canonical_sha256(changed_strength),
                },
                "validation_status": "invalid",
            },
        )
        self.assertEqual(failed["professional_experience_proxy_rollup"]["status"], "none")
        self.assertEqual(transport.calls, 0)
        self.assertEqual(_schema_errors(failed, self.review_schema), [])

        false_canonical_provenance = copy.deepcopy(failed)
        false_canonical_provenance["professional_experience_proxy_policy"]["observed"] = copy.deepcopy(
            false_canonical_provenance["professional_experience_proxy_policy"]["expected"]
        )
        self.assertTrue(_schema_errors(false_canonical_provenance, self.review_schema))

        false_invalid_status = copy.deepcopy(self.review)
        false_invalid_status["professional_experience_proxy_policy"]["validation_status"] = "invalid"
        self.assertTrue(_schema_errors(false_invalid_status, self.review_schema))
        self.assertEqual(
            validate_review(
                failed,
                request=self.request,
                prompt=self.prompt,
                output_schema=self.model_output_schema,
                proxy_policy=changed_strength,
                raw_response=None,
                transport=transport,
                execute_live=False,
            ),
            [],
        )

        for field_name, value in (
            ("professional_experience_proxy_policy_version", "unreviewed-policy"),
            ("professional_experience_proxy_policy_sha256", "0" * 64),
        ):
            request = copy.deepcopy(self.request)
            request["model_policy"][field_name] = value
            self.assertTrue(_schema_errors(request, self.request_schema))
            self.assertTrue(
                validate_request(
                    request,
                    prompt=self.prompt,
                    output_schema=self.model_output_schema,
                    proxy_policy=self.proxy_policy,
                )
            )

    def test_invalid_policy_precedes_transport_metadata_and_validator_never_gets_it(self) -> None:
        changed_policy = copy.deepcopy(self.proxy_policy)
        changed_policy["proposal_mappings"]["observed_chinese_professional_content"]["strength"] = "strong_proxy"

        class ExplosiveMetadataTransport:
            def __init__(self) -> None:
                self.metadata_getters = 0

            @property
            def is_live(self) -> bool:
                self.metadata_getters += 1
                raise AssertionError("policy preflight must precede transport metadata")

            @property
            def transport_id(self) -> str:
                self.metadata_getters += 1
                raise AssertionError("policy preflight must precede transport metadata")

            def create_response(self, payload: Any, *, timeout_ms: int) -> dict[str, Any]:
                raise AssertionError("invalid policy must not call transport")

        transport = ExplosiveMetadataTransport()
        observed = run_semantic_review(
            self.request,
            prompt=self.prompt,
            output_schema=self.model_output_schema,
            proxy_policy=changed_policy,
            transport=transport,
        )
        self.assertEqual(observed["error_codes"], ["proxy_policy_invalid"])
        self.assertEqual(observed["professional_experience_proxy_policy"]["validation_status"], "invalid")
        self.assertEqual(
            observed["professional_experience_proxy_policy"]["observed"]["policy_sha256"],
            canonical_sha256(changed_policy),
        )
        self.assertEqual(transport.metadata_getters, 0)
        self.assertEqual(_schema_errors(observed, self.review_schema), [])
        self.assertEqual(
            validate_review(
                observed,
                request=self.request,
                prompt=self.prompt,
                output_schema=self.model_output_schema,
                proxy_policy=changed_policy,
                raw_response=None,
                transport=transport,
                execute_live=False,
            ),
            [],
        )
        self.assertEqual(transport.metadata_getters, 0)

    def test_transport_cannot_mutate_validated_inputs_after_preflight(self) -> None:
        mutable_policy = copy.deepcopy(self.proxy_policy)
        mutable_request = copy.deepcopy(self.request)
        raw_response = copy.deepcopy(self.raw_response)

        class MutatingTransport:
            is_live = False
            transport_id = "offline_fake_responses"

            def create_response(self, payload: Any, *, timeout_ms: int) -> dict[str, Any]:
                del payload, timeout_ms
                mutable_policy["proposal_mappings"]["professional_china_digital_ecosystem"]["strength"] = "weak_proxy"
                mutable_request["profile_snapshot"]["bio_text"] = "mutated after preflight"
                return copy.deepcopy(raw_response)

        observed = run_semantic_review(
            mutable_request,
            prompt=self.prompt,
            output_schema=self.model_output_schema,
            proxy_policy=mutable_policy,
            transport=MutatingTransport(),
        )

        self.assertEqual(observed["status"], "completed")
        self.assertEqual(observed["request_sha256"], canonical_sha256(self.request))
        self.assertEqual(
            observed["professional_experience_proxy_rollup"]["regions"]["china"]["strength"],
            "strong_proxy",
        )
        self.assertEqual(
            validate_review(
                observed,
                request=self.request,
                prompt=self.prompt,
                output_schema=self.model_output_schema,
                proxy_policy=self.proxy_policy,
                raw_response=self.raw_response,
            ),
            [],
        )

    def test_metadata_getters_and_response_subclass_cannot_mutate_snapshotted_sources(self) -> None:
        mutable_request = copy.deepcopy(self.request)
        mutable_prompt = copy.deepcopy(self.prompt)
        mutable_schema = copy.deepcopy(self.model_output_schema)
        mutable_policy = copy.deepcopy(self.proxy_policy)

        class GetterBombResponse(dict[str, Any]):
            def get(self, key: str, default: Any = None) -> Any:
                raise AssertionError(f"detached response must not call get({key!r})")

            def __getitem__(self, key: str) -> Any:
                raise AssertionError(f"detached response must not call __getitem__({key!r})")

        class MutatingMetadataTransport:
            @property
            def is_live(self) -> bool:
                mutable_request["profile_snapshot"]["bio_text"] = "getter mutation"
                mutable_prompt["model_id"] = "getter mutation"
                return False

            @property
            def transport_id(self) -> str:
                mutable_schema["title"] = "getter mutation"
                mutable_policy["source_status"] = "getter mutation"
                return "offline_fake_responses"

            def create_response(self, payload: Any, *, timeout_ms: int) -> GetterBombResponse:
                del payload, timeout_ms
                return GetterBombResponse(copy.deepcopy(self.raw_response))  # type: ignore[attr-defined]

        transport = MutatingMetadataTransport()
        transport.raw_response = self.raw_response  # type: ignore[attr-defined]
        observed = run_semantic_review(
            mutable_request,
            prompt=mutable_prompt,
            output_schema=mutable_schema,
            proxy_policy=mutable_policy,
            transport=transport,
        )
        self.assertEqual(observed, self.review)
        self.assertEqual(
            validate_review(
                self.review,
                request=self.request,
                prompt=self.prompt,
                output_schema=self.model_output_schema,
                proxy_policy=self.proxy_policy,
                raw_response=GetterBombResponse(copy.deepcopy(self.raw_response)),
            ),
            [],
        )

    def test_invalid_transport_metadata_cannot_rebind_snapshotted_request(self) -> None:
        class MutatingInvalidMetadataTransport:
            def __init__(self, request: dict[str, Any]) -> None:
                self.request = request

            @property
            def is_live(self) -> None:
                self.request["profile_snapshot"]["bio_text"] = "mutated by invalid metadata getter"
                return None

            @property
            def transport_id(self) -> str:
                return "offline_fake_responses"

            def create_response(self, payload: Any, *, timeout_ms: int) -> dict[str, Any]:
                raise AssertionError("invalid transport metadata must terminate before invocation")

        mutable_request = copy.deepcopy(self.request)
        result = run_semantic_review(
            mutable_request,
            prompt=self.prompt,
            output_schema=self.model_output_schema,
            proxy_policy=self.proxy_policy,
            transport=MutatingInvalidMetadataTransport(mutable_request),
        )
        self.assertEqual(result["error_codes"], ["execution_contract_invalid"])
        self.assertEqual(result["request_sha256"], canonical_sha256(self.request))

        validator_request = copy.deepcopy(self.request)
        self.assertEqual(
            validate_review(
                result,
                request=validator_request,
                prompt=self.prompt,
                output_schema=self.model_output_schema,
                proxy_policy=self.proxy_policy,
                raw_response=None,
                transport=MutatingInvalidMetadataTransport(validator_request),
                execute_live=False,
            ),
            [],
        )

        mutable_review = copy.deepcopy(self.review)
        mutable_request = copy.deepcopy(self.request)
        mutable_prompt = copy.deepcopy(self.prompt)
        mutable_schema = copy.deepcopy(self.model_output_schema)
        mutable_policy = copy.deepcopy(self.proxy_policy)
        mutable_response = copy.deepcopy(self.raw_response)

        class ValidatorMutatingMetadataTransport:
            @property
            def is_live(self) -> bool:
                mutable_review["status"] = "failed"
                mutable_request["profile_snapshot"]["bio_text"] = "validator getter mutation"
                mutable_response["status"] = "failed"
                return False

            @property
            def transport_id(self) -> str:
                mutable_prompt["model_id"] = "validator getter mutation"
                mutable_schema["title"] = "validator getter mutation"
                mutable_policy["source_status"] = "validator getter mutation"
                return "offline_fake_responses"

        self.assertEqual(
            validate_review(
                mutable_review,
                request=mutable_request,
                prompt=mutable_prompt,
                output_schema=mutable_schema,
                proxy_policy=mutable_policy,
                raw_response=mutable_response,
            ),
            [],
        )

    def test_json_equality_schema_and_runtime_are_bool_int_type_strict(self) -> None:
        self.assertFalse(json_type_strict_equal(False, 0))
        self.assertFalse(json_type_strict_equal(True, 1))
        self.assertFalse(json_type_strict_equal(1, 1.0))
        self.assertTrue(json_type_strict_equal({"value": False}, {"value": False}))
        self.assertTrue(_schema_errors(0, {"const": False}))
        self.assertTrue(_schema_errors(1, {"enum": [True]}))
        self.assertEqual(_schema_errors(False, {"const": False}), [])

        request_mutations = []
        strict_output = copy.deepcopy(self.request)
        strict_output["model_policy"]["strict_structured_output"] = 1
        request_mutations.append(strict_output)
        bool_budget = copy.deepcopy(self.request)
        bool_budget["budgets"]["max_external_calls"] = True
        request_mutations.append(bool_budget)
        numeric_authority = copy.deepcopy(self.request)
        numeric_authority["authority"]["external_facts_allowed"] = 0
        request_mutations.append(numeric_authority)
        for request in request_mutations:
            self.assertTrue(
                validate_request(
                    request,
                    prompt=self.prompt,
                    output_schema=self.model_output_schema,
                    proxy_policy=self.proxy_policy,
                )
            )
            self.assertTrue(_schema_errors(request, self.request_schema))

        model_mutations = []
        bool_span = copy.deepcopy(self.model_output)
        bool_span["proposals"][0]["span_start"] = False
        model_mutations.append(bool_span)
        numeric_verification = copy.deepcopy(self.model_output)
        numeric_verification["proposals"][0]["requires_independent_verification"] = 1
        model_mutations.append(numeric_verification)
        numeric_model_authority = copy.deepcopy(self.model_output)
        numeric_model_authority["authority"]["external_facts_used"] = 0
        model_mutations.append(numeric_model_authority)
        for model_output in model_mutations:
            self.assertTrue(validate_model_output(model_output, request=self.request))
            self.assertTrue(_schema_errors(model_output, self.model_output_schema))

        review_mutations = []
        numeric_billable = copy.deepcopy(self.review)
        numeric_billable["usage"]["billable"] = 0
        review_mutations.append(numeric_billable)
        numeric_review_authority = copy.deepcopy(self.review)
        numeric_review_authority["authority"]["external_facts_accepted"] = 0
        review_mutations.append(numeric_review_authority)
        numeric_review_verification = copy.deepcopy(self.review)
        numeric_review_verification["proposals"][0]["requires_independent_verification"] = 1
        review_mutations.append(numeric_review_verification)
        for review in review_mutations:
            self.assertTrue(_schema_errors(review, self.review_schema))
            self.assertTrue(
                validate_review(
                    review,
                    request=self.request,
                    prompt=self.prompt,
                    output_schema=self.model_output_schema,
                    proxy_policy=self.proxy_policy,
                    raw_response=self.raw_response,
                    execution_attempt=_offline_completed_execution_receipt(),
                )
            )

    def test_fixture_profile_url_schema_and_runtime_share_canonical_rule(self) -> None:
        self.assertEqual(_schema_errors(self.request, self.request_schema), [])
        self.assertEqual(
            validate_request(
                self.request,
                prompt=self.prompt,
                output_schema=self.model_output_schema,
                proxy_policy=self.proxy_policy,
            ),
            [],
        )
        invalid_urls = (
            "https://invalid/x/semanticfixture",
            "https://Profiles.invalid/x/semanticfixture",
            "https://profiles.invalid/x/Semanticfixture",
            "https://profiles.invalid/x/semanticfixture/",
            "http://profiles.invalid/x/semanticfixture",
        )
        for profile_url in invalid_urls:
            request = copy.deepcopy(self.request)
            request["profile_snapshot"]["profile_url"] = profile_url
            with self.subTest(profile_url=profile_url):
                self.assertTrue(_schema_errors(request, self.request_schema))
                self.assertTrue(
                    validate_request(
                        request,
                        prompt=self.prompt,
                        output_schema=self.model_output_schema,
                        proxy_policy=self.proxy_policy,
                    )
                )

        cross_field_mismatch = copy.deepcopy(self.request)
        cross_field_mismatch["profile_snapshot"]["profile_url"] = "https://profiles.invalid/x/otherhandle"
        self.assertEqual(_schema_errors(cross_field_mismatch, self.request_schema), [])
        self.assertTrue(
            validate_request(
                cross_field_mismatch,
                prompt=self.prompt,
                output_schema=self.model_output_schema,
                proxy_policy=self.proxy_policy,
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
