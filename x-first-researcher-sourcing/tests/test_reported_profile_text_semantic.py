from __future__ import annotations

import copy
import inspect
import re
import sys
import unittest
from pathlib import Path
from typing import Any
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from x_first import profile_bio_semantic_v2 as semantic  # noqa: E402
from x_first import reported_profile_text_semantic as reported  # noqa: E402


def _resolve_ref(schema: dict[str, Any], root: dict[str, Any]) -> dict[str, Any]:
    reference = schema.get("$ref")
    if reference is None:
        return schema
    if not isinstance(reference, str) or not reference.startswith("#/"):
        raise AssertionError(f"unsupported schema reference: {reference!r}")
    value: Any = root
    for token in reference[2:].split("/"):
        token = token.replace("~1", "/").replace("~0", "~")
        value = value[token]
    if not isinstance(value, dict):
        raise AssertionError(f"schema reference does not resolve to an object: {reference}")
    return value


def _schema_errors(
    instance: Any,
    schema: Any,
    *,
    root: dict[str, Any] | None = None,
    path: str = "$",
) -> list[str]:
    if not isinstance(schema, dict):
        return [f"{path}: invalid schema"]
    root = schema if root is None else root
    if "$ref" in schema:
        return _schema_errors(instance, _resolve_ref(schema, root), root=root, path=path)
    errors: list[str] = []
    for sub_schema in schema.get("allOf", []):
        errors.extend(_schema_errors(instance, sub_schema, root=root, path=path))
    if "oneOf" in schema:
        matches = sum(not _schema_errors(instance, option, root=root, path=path) for option in schema["oneOf"])
        if matches != 1:
            errors.append(f"{path}: oneOf mismatch")
    if "const" in schema and not semantic.json_type_strict_equal(instance, schema["const"]):
        errors.append(f"{path}: const mismatch")
    if "enum" in schema and not any(
        semantic.json_type_strict_equal(instance, option) for option in schema["enum"]
    ):
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
                errors.extend(_schema_errors(value, properties[key], root=root, path=f"{path}.{key}"))
            elif schema.get("additionalProperties") is False:
                errors.append(f"{path}: unexpected {key}")
    if isinstance(instance, list):
        if len(instance) < schema.get("minItems", 0):
            errors.append(f"{path}: too few items")
        if "maxItems" in schema and len(instance) > schema["maxItems"]:
            errors.append(f"{path}: too many items")
        for index, value in enumerate(instance):
            if "items" in schema:
                errors.extend(_schema_errors(value, schema["items"], root=root, path=f"{path}[{index}]"))
    return errors


def _assert_strict_objects(test: unittest.TestCase, value: Any) -> None:
    if isinstance(value, dict):
        if value.get("type") == "object":
            test.assertIs(value.get("additionalProperties"), False)
            test.assertEqual(set(value.get("required", [])), set(value.get("properties", {})))
        for child in value.values():
            _assert_strict_objects(test, child)
    elif isinstance(value, list):
        for child in value:
            _assert_strict_objects(test, child)


def _proposal(text: str, proposal_type: str, relation_state: str) -> dict[str, Any]:
    reason_codes = semantic.REASON_CODES_BY_SEMANTIC_STATE[(proposal_type, relation_state)]
    return {
        "proposal_type": proposal_type,
        "relation_state": relation_state,
        "span_start": 0,
        "span_end": len(text),
        "excerpt": text,
        "reason_codes": sorted(reason_codes),
        "reason": semantic.EXPLANATION_TEMPLATES_BY_SEMANTIC_STATE[(proposal_type, relation_state)],
        "reason_source": semantic.REASON_SOURCE,
        "confidence": "high",
        "evidence_basis": reported.EVIDENCE_BASIS,
        "requires_independent_verification": True,
    }


class ReportedProfileTextSemanticTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.batch = reported.load_json(ROOT / "fixtures/reported_profile_text_semantic_batch_request_v1.json")
        output_fixture = reported.load_json(
            ROOT / "fixtures/reported_profile_text_semantic_model_outputs_v1.json"
        )
        review_fixture = reported.load_json(
            ROOT / "fixtures/reported_profile_text_semantic_item_reviews_v1.json"
        )
        cls.outputs = {
            value["observation_id"]: value
            for value in output_fixture["items"]
        }
        cls.reviews = review_fixture["items"]
        cls.proxy_policy = reported.load_json(
            ROOT / "configs/profile_bio_professional_experience_proxy_policy.v1.json"
        )
        cls.observation_schema = reported.load_json(
            ROOT / "contracts/x.reported_profile_text.observation.v1.schema.json"
        )
        cls.batch_schema = reported.load_json(
            ROOT / "contracts/x.reported_profile_text.semantic.batch_request.v1.schema.json"
        )
        cls.model_output_schema = reported.load_json(
            ROOT / "contracts/x.reported_profile_text.semantic.model_output.v1.schema.json"
        )
        cls.item_review_schema = reported.load_json(
            ROOT / "contracts/x.reported_profile_text.semantic.item_review.v1.schema.json"
        )

    def test_synthetic_fixtures_validate_and_recompute(self) -> None:
        self.assertEqual(reported.validate_batch_request(self.batch), [])
        self.assertEqual(_schema_errors(self.batch, self.batch_schema), [])
        closure = reported.adjudicate_batch(
            batch_request=self.batch,
            model_outputs_by_observation_id=self.outputs,
            proxy_policy=self.proxy_policy,
        )
        self.assertEqual(closure["reviews"], self.reviews)
        self.assertEqual(closure["denominator"], 5)
        self.assertEqual(closure["terminal_reviews"], 5)
        self.assertEqual(closure["completed"], 5)
        self.assertEqual(closure["failed"], 0)
        self.assertEqual(
            reported.validate_batch_closure(
                closure,
                batch_request=self.batch,
                model_outputs_by_observation_id=self.outputs,
                proxy_policy=self.proxy_policy,
            ),
            [],
        )
        for observation, review in zip(self.batch["observations"], self.reviews, strict=True):
            output = self.outputs[observation["observation_id"]]
            self.assertEqual(reported.validate_observation(observation), [])
            self.assertEqual(_schema_errors(observation, self.observation_schema), [])
            self.assertEqual(
                reported.validate_model_output(output, batch_request=self.batch, observation=observation),
                [],
            )
            self.assertEqual(_schema_errors(output, self.model_output_schema), [])
            self.assertEqual(
                reported.validate_item_review(
                    review,
                    batch_request=self.batch,
                    model_output=output,
                    proxy_policy=self.proxy_policy,
                ),
                [],
            )
            self.assertEqual(_schema_errors(review, self.item_review_schema), [])

    def test_source_and_reported_identity_remain_unverified(self) -> None:
        observation = self.batch["observations"][0]
        self.assertEqual(observation["reported_platform_user_id_status"], "model_mediated_unverified")
        self.assertFalse(observation["authority"]["stable_platform_identity_verified"])
        no_id = reported.build_observation(
            campaign_result_sha256=observation["campaign_result_sha256"],
            candidate_row_sha256=observation["candidate_row_sha256"],
            text=observation["text"],
            reported_platform_user_id=None,
        )
        self.assertEqual(no_id["observation_id"], observation["observation_id"])
        self.assertEqual(no_id["candidate_ref"], observation["candidate_ref"])
        self.assertEqual(no_id["reported_platform_user_id_status"], "absent")
        source_upgrade = copy.deepcopy(observation)
        source_upgrade["source_kind"] = "source_bound_x_profile"
        self.assertTrue(reported.validate_observation(source_upgrade))
        identity_upgrade = copy.deepcopy(observation)
        identity_upgrade["reported_platform_user_id_status"] = "verified"
        self.assertTrue(reported.validate_observation(identity_upgrade))
        extra_profile_field = copy.deepcopy(observation)
        extra_profile_field["profile_url"] = "https://profiles.invalid/x/not-accepted"
        self.assertTrue(reported.validate_observation(extra_profile_field))
        forged_candidate = copy.deepcopy(observation)
        forged_candidate["candidate_ref"] = "xrpt_candidate_ffffffffffffffffffffffff"
        self.assertTrue(reported.validate_observation(forged_candidate))
        forged_observation = copy.deepcopy(observation)
        forged_observation["observation_id"] = "xrpt_obs_ffffffffffffffffffffffff"
        self.assertTrue(reported.validate_observation(forged_observation))

    def test_model_output_requires_unique_strict_batch_observation(self) -> None:
        member = self.batch["observations"][0]
        outsider = reported.build_observation(
            campaign_result_sha256=member["campaign_result_sha256"],
            candidate_row_sha256=member["candidate_row_sha256"],
            text=member["text"],
            reported_platform_user_id=None,
        )
        self.assertEqual(outsider["observation_id"], member["observation_id"])
        self.assertNotEqual(outsider, member)
        output = self.outputs[member["observation_id"]]
        errors = reported.validate_model_output(
            output,
            batch_request=self.batch,
            observation=outsider,
        )
        self.assertTrue(any("unique strict-equal member" in error for error in errors))
        with self.assertRaisesRegex(ValueError, "model_output_invalid"):
            reported.build_model_output(
                batch_request=self.batch,
                observation=outsider,
                verdict="no_supported_professional_context",
                proposals=[],
            )

    def test_strong_weak_physical_affiliation_and_none_remain_separate(self) -> None:
        strengths = [
            review["professional_experience_proxy_rollup"]["regions"]["china"]["strength"]
            for review in self.reviews
        ]
        self.assertEqual(strengths, ["strong_proxy", "weak_proxy", "none", "none", "none"])
        asia_strengths = [
            review["professional_experience_proxy_rollup"]["regions"]["asia"]["strength"]
            for review in self.reviews
        ]
        self.assertEqual(asia_strengths, strengths)
        physical = self.reviews[2]
        self.assertEqual(physical["proposals"][0]["proposal_type"], "professional_region_experience")
        self.assertFalse(physical["authority"]["physical_region_inferred"])
        affiliation = self.reviews[3]["proposals"][0]
        self.assertEqual(affiliation["proposal_type"], "professional_affiliation")
        self.assertEqual(affiliation["relation_state"], "previous_claimed")
        for review in self.reviews:
            self.assertTrue(review["authority"]["verification_queue_only"])
            self.assertFalse(review["authority"]["protected_identity_inferred"])
            self.assertFalse(review["authority"]["discovery_or_ranking_authorized"])
            self.assertFalse(review["authority"]["eligibility_decided"])
            self.assertFalse(review["authority"]["outreach_authorized"])

    def test_span_reason_hash_and_review_recomputation_fail_closed(self) -> None:
        observation = self.batch["observations"][0]
        original = self.outputs[observation["observation_id"]]
        bad_excerpt = copy.deepcopy(original)
        bad_excerpt["proposals"][0]["excerpt"] += "x"
        self.assertTrue(
            reported.validate_model_output(
                bad_excerpt,
                batch_request=self.batch,
                observation=observation,
            )
        )
        failed = reported.adjudicate_item(
            batch_request=self.batch,
            observation_id=observation["observation_id"],
            model_output=bad_excerpt,
            proxy_policy=self.proxy_policy,
        )
        self.assertEqual(failed["status"], "failed")
        self.assertEqual(failed["error_codes"], ["model_output_invalid"])
        self.assertIsNone(failed["model_output_sha256"])
        bad_reason = copy.deepcopy(original)
        bad_reason["proposals"][0]["reason"] = "Open model prose is forbidden."
        self.assertTrue(
            reported.validate_model_output(
                bad_reason,
                batch_request=self.batch,
                observation=observation,
            )
        )

    def test_invalid_or_over_ceiling_output_is_not_rehashed_on_failure(self) -> None:
        observation = self.batch["observations"][0]
        original = self.outputs[observation["observation_id"]]
        oversized = copy.deepcopy(original)
        oversized["proposals"][0]["excerpt"] = "x" * (
            reported.OPERATIONAL_LIMITS["max_model_output_canonical_bytes"] + 1
        )
        original_hash = reported.canonical_sha256

        def reject_unbounded_output_hash(value: Any) -> str:
            if value is oversized:
                raise AssertionError("invalid model output was hashed without a bound")
            return original_hash(value)

        with patch.object(reported, "canonical_sha256", side_effect=reject_unbounded_output_hash):
            review = reported.adjudicate_item(
                batch_request=self.batch,
                observation_id=observation["observation_id"],
                model_output=oversized,
                proxy_policy=self.proxy_policy,
            )
        self.assertEqual(review["status"], "failed")
        self.assertEqual(review["error_codes"], ["model_output_invalid"])
        self.assertIsNone(review["model_output_sha256"])
        corrupted_review = copy.deepcopy(self.reviews[0])
        corrupted_review["proposals"][0]["excerpt_sha256"] = "f" * 64
        self.assertTrue(
            reported.validate_item_review(
                corrupted_review,
                batch_request=self.batch,
                model_output=original,
                proxy_policy=self.proxy_policy,
            )
        )

    def test_hostile_nested_types_return_errors_without_traceback(self) -> None:
        observation = self.batch["observations"][0]
        original = self.outputs[observation["observation_id"]]
        mutations = [
            ("verdict", ["proposals_available"]),
            ("proposal_type", ["professional_china_digital_ecosystem"]),
            ("relation_state", ["not_applicable"]),
            ("reason_codes", [["explicit_china_digital_ecosystem_activity"]]),
            ("confidence", {"value": "high"}),
        ]
        for field, replacement in mutations:
            with self.subTest(field=field):
                mutated = copy.deepcopy(original)
                if field == "verdict":
                    mutated[field] = replacement
                else:
                    mutated["proposals"][0][field] = replacement
                self.assertTrue(
                    reported.validate_model_output(
                        mutated,
                        batch_request=self.batch,
                        observation=observation,
                    )
                )
        review = copy.deepcopy(self.reviews[0])
        review["status"] = ["completed"]
        self.assertTrue(
            reported.validate_item_review(
                review,
                batch_request=self.batch,
                model_output=original,
                proxy_policy=self.proxy_policy,
            )
        )

    def test_batch_denominator_is_terminal_total(self) -> None:
        self.assertNotIn("_recompute", inspect.signature(reported.validate_batch_closure).parameters)
        missing = dict(self.outputs)
        missing.pop(self.batch["observations"][1]["observation_id"])
        closure = reported.adjudicate_batch(
            batch_request=self.batch,
            model_outputs_by_observation_id=missing,
            proxy_policy=self.proxy_policy,
        )
        self.assertEqual(closure["denominator"], 5)
        self.assertEqual(closure["terminal_reviews"], 5)
        self.assertEqual(closure["completed"], 4)
        self.assertEqual(closure["failed"], 1)
        self.assertEqual(
            [review["error_codes"] for review in closure["reviews"] if review["status"] == "failed"],
            [["model_output_missing"]],
        )
        extra = dict(self.outputs)
        extra["xrpt_obs_ffffffffffffffffffffffff"] = next(iter(self.outputs.values()))
        with self.assertRaisesRegex(ValueError, "unknown_observation"):
            reported.adjudicate_batch(
                batch_request=self.batch,
                model_outputs_by_observation_id=extra,
                proxy_policy=self.proxy_policy,
            )
        corrupted = copy.deepcopy(closure)
        corrupted["terminal_reviews"] = 4
        self.assertTrue(
            reported.validate_batch_closure(
                corrupted,
                batch_request=self.batch,
                model_outputs_by_observation_id=missing,
                proxy_policy=self.proxy_policy,
            )
        )

    def test_item_review_schema_closes_completed_verdict_proposal_cardinality(self) -> None:
        proposals_missing = copy.deepcopy(self.reviews[0])
        self.assertEqual(proposals_missing["verdict"], "proposals_available")
        proposals_missing["proposals"] = []
        self.assertTrue(_schema_errors(proposals_missing, self.item_review_schema))

        unexpected_proposal = copy.deepcopy(self.reviews[4])
        self.assertEqual(unexpected_proposal["verdict"], "no_supported_professional_context")
        unexpected_proposal["proposals"] = copy.deepcopy(self.reviews[0]["proposals"])
        self.assertTrue(_schema_errors(unexpected_proposal, self.item_review_schema))

        observation = self.batch["observations"][0]
        invalid_output = copy.deepcopy(self.outputs[observation["observation_id"]])
        invalid_output["proposals"][0]["excerpt"] += " invalid"
        failed = reported.adjudicate_item(
            batch_request=self.batch,
            observation_id=observation["observation_id"],
            model_output=invalid_output,
            proxy_policy=self.proxy_policy,
        )
        self.assertIsNone(failed["model_output_sha256"])
        self.assertEqual(_schema_errors(failed, self.item_review_schema), [])
        forged_failed_digest = copy.deepcopy(failed)
        forged_failed_digest["model_output_sha256"] = "f" * 64
        self.assertTrue(_schema_errors(forged_failed_digest, self.item_review_schema))

    def test_150_synthetic_items_have_no_business_candidate_cap(self) -> None:
        campaign = "2" * 64
        observations = [
            reported.build_observation(
                campaign_result_sha256=campaign,
                candidate_row_sha256=f"{index + 1:064x}",
                text=f"Synthetic AI systems note {index:03d}.",
            )
            for index in range(150)
        ]
        batch = reported.build_batch_request(observations)
        outputs = {
            observation["observation_id"]: reported.build_model_output(
                batch_request=batch,
                observation=observation,
                verdict="no_supported_professional_context",
                proposals=[],
            )
            for observation in observations
        }
        with (
            patch.object(
                reported,
                "validate_batch_request",
                wraps=reported.validate_batch_request,
            ) as batch_validation,
            patch.object(
                reported,
                "_build_observation_index_validated",
                wraps=reported._build_observation_index_validated,
            ) as index_build,
            patch.object(
                reported,
                "_is_strict_index_member",
                wraps=reported._is_strict_index_member,
            ) as membership_lookup,
        ):
            closure = reported.adjudicate_batch(
                batch_request=batch,
                model_outputs_by_observation_id=outputs,
                proxy_policy=self.proxy_policy,
            )
        self.assertEqual(closure["denominator"], 150)
        self.assertEqual(closure["terminal_reviews"], 150)
        self.assertEqual(closure["completed"], 150)
        self.assertEqual(closure["failed"], 0)
        self.assertNotIn("business_candidate_limit", batch["operational_limits"])
        self.assertEqual(batch_validation.call_count, 1)
        self.assertEqual(index_build.call_count, 1)
        self.assertEqual(membership_lookup.call_count, 150)

    def test_technical_ceiling_is_failure_not_success(self) -> None:
        with self.assertRaisesRegex(ValueError, "technical ceiling exceeded"):
            reported.build_observation(
                campaign_result_sha256="3" * 64,
                candidate_row_sha256="4" * 64,
                text="x" * (reported.OPERATIONAL_LIMITS["max_text_characters_per_item"] + 1),
            )
        drifted = copy.deepcopy(self.batch)
        drifted["operational_limits"]["max_items"] = 5
        self.assertTrue(reported.validate_batch_request(drifted))
        with self.assertRaisesRegex(ValueError, "batch_request_invalid"):
            reported.adjudicate_batch(
                batch_request=drifted,
                model_outputs_by_observation_id=self.outputs,
                proxy_policy=self.proxy_policy,
            )

    def test_semantic_vocabulary_and_proxy_policy_are_reused_exactly(self) -> None:
        proposal_schema = self.model_output_schema["$defs"]["proposal"]["properties"]
        self.assertEqual(set(proposal_schema["proposal_type"]["enum"]), set(semantic.PROPOSAL_TYPES))
        self.assertEqual(set(proposal_schema["relation_state"]["enum"]), set(semantic.RELATION_STATES))
        self.assertEqual(set(proposal_schema["reason_codes"]["items"]["enum"]), set(semantic.REASON_CODES))
        self.assertEqual(
            set(proposal_schema["reason"]["enum"]),
            set(semantic.EXPLANATION_TEMPLATES_BY_SEMANTIC_STATE.values()),
        )
        self.assertEqual(proposal_schema["reason_source"]["const"], semantic.REASON_SOURCE)
        self.assertEqual(
            self.batch["semantic_policy"]["professional_experience_proxy_policy_sha256"],
            semantic.CANONICAL_PROXY_POLICY_SHA256,
        )
        self.assertEqual(semantic.validate_proxy_policy(self.proxy_policy), [])
        self.assertEqual(reported.canonical_sha256(self.proxy_policy), semantic.CANONICAL_PROXY_POLICY_SHA256)

    def test_four_schemas_are_closed_and_offline_module_has_no_transport(self) -> None:
        for schema in (
            self.observation_schema,
            self.batch_schema,
            self.model_output_schema,
            self.item_review_schema,
        ):
            _assert_strict_objects(self, schema)
        module_source = (ROOT / "src/x_first/reported_profile_text_semantic.py").read_text(encoding="utf-8")
        for prohibited in ("urllib", "requests", "subprocess", "socket", "CHSHAPI_API_KEY", "create_response"):
            self.assertNotIn(prohibited, module_source)
        fixture_text = "".join(
            path.read_text(encoding="utf-8")
            for path in (
                ROOT / "fixtures/reported_profile_text_semantic_batch_request_v1.json",
                ROOT / "fixtures/reported_profile_text_semantic_model_outputs_v1.json",
                ROOT / "fixtures/reported_profile_text_semantic_item_reviews_v1.json",
            )
        )
        self.assertNotIn("x.com/", fixture_text)
        self.assertNotIn("profiles.invalid", fixture_text)


if __name__ == "__main__":
    unittest.main()
