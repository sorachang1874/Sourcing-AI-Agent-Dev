from __future__ import annotations

import copy
import hashlib
import json
import sys
import threading
import time
import unittest
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from x_first import luna_batch_runner as lbr  # noqa: E402
from x_first.profile_bio_semantic_v2 import canonical_sha256  # noqa: E402
from x_first.recall_pool_schema import MiniDraft202012Error, assert_schema_valid  # noqa: E402
from x_first.source_neutral_mapping import (  # noqa: E402
    LUNA_AXIS_REDUCTION_SCHEMA_FILE,
    LUNA_AXIS_REDUCTION_SCHEMA_VERSION,
)


def _assert_closed_objects(test: unittest.TestCase, value: Any) -> None:
    if isinstance(value, dict):
        if value.get("type") == "object":
            test.assertIs(value.get("additionalProperties"), False)
            test.assertEqual(set(value.get("required", [])), set(value.get("properties", {})))
        for child in value.values():
            _assert_closed_objects(test, child)
    elif isinstance(value, list):
        for child in value:
            _assert_closed_objects(test, child)


def _find_refs(value: Any) -> list[str]:
    found: list[str] = []
    if isinstance(value, dict):
        for key, child in value.items():
            if key == "$ref":
                found.append(child)
            else:
                found.extend(_find_refs(child))
    elif isinstance(value, list):
        for child in value:
            found.extend(_find_refs(child))
    return found


class LunaBatchRunnerTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.fixture = lbr.load_json(ROOT / "fixtures/luna_candidate_review_fixture_v1.json")
        cls.seeds = cls.fixture["seeds"]
        cls.bundles = cls.fixture["bundles"]
        cls.outputs = cls.fixture["model_outputs"]
        cls.refs = [seed["seed_ref"] for seed in cls.seeds]
        cls.seed_by_ref = {seed["seed_ref"]: seed for seed in cls.seeds}
        cls.contract = lbr.load_json(
            ROOT / "contracts/x.source_neutral.mapping.luna_candidate_review.v1.schema.json"
        )

    def _approval(self, refs: list[str] | None = None) -> dict[str, Any]:
        return {
            "schema_version": lbr.APPROVAL_RECEIPT_SCHEMA_VERSION,
            "approval_id": self.fixture["approval_id"],
            "approved_at": "2026-07-19T00:00:00.000Z",
            "candidate_refs_sha256": canonical_sha256(list(refs if refs is not None else self.refs)),
        }

    def _run_batch(self, **overrides: Any) -> tuple[dict[str, Any], lbr.OfflineFakeLunaTransport]:
        transport = overrides.pop("transport", None) or lbr.OfflineFakeLunaTransport(outputs=self.outputs)
        result = lbr.run_luna_batch(
            seeds=overrides.pop("seeds", self.seeds),
            bundles=overrides.pop("bundles", self.bundles),
            transport=transport,
            approval=overrides.pop("approval", self._approval()),
            **overrides,
        )
        return result, transport

    def _review(self, ref: str) -> dict[str, Any]:
        return lbr.build_candidate_review(
            self.seed_by_ref[ref],
            self.bundles[ref],
            self.outputs[ref],
        )

    # ------------------------------------------------------------------
    # Contract schema.
    # ------------------------------------------------------------------

    def test_review_contract_is_closed_and_validates_built_review(self) -> None:
        _assert_closed_objects(self, self.contract)
        for ref in self.refs:
            review = self._review(ref)
            assert_schema_valid(review, lbr.REVIEW_SCHEMA_FILE)
            self.assertEqual(review["schema_version"], lbr.REVIEW_SCHEMA_VERSION)
            self.assertEqual(review["authority_status"], "diagnostic_only_unattested")
            self.assertEqual(review["model_claim_scope"], "state_proposal_only")
            self.assertEqual(review["judged_bundle_manifest"][0]["source_kind"], "seed_fact")

    def test_review_contract_rejects_tampered_reviews(self) -> None:
        review = self._review(self.refs[0])
        for mutate in (
            lambda value: value.update({"authority_status": "authoritative"}),
            lambda value: value.update({"model_claim_scope": "ranking_signal"}),
            lambda value: value.update({"unexpected_field": True}),
            lambda value: value.update({"proposed_lab_affiliation_state": "verified"}),
            lambda value: value["judged_bundle_manifest"][0].update({"source_status": "attested"}),
            lambda value: value["lab_affiliation_evidence_citations"].append("bio:freeform"),
        ):
            tampered = copy.deepcopy(review)
            mutate(tampered)
            with self.assertRaises(MiniDraft202012Error):
                assert_schema_valid(tampered, lbr.REVIEW_SCHEMA_FILE)

    # ------------------------------------------------------------------
    # Prompt asset pinning.
    # ------------------------------------------------------------------

    def test_prompt_is_pinned_to_canonical_sha(self) -> None:
        prompt = lbr.load_prompt()
        self.assertEqual(lbr.validate_prompt(prompt), [])
        self.assertEqual(canonical_sha256(prompt), lbr.CANONICAL_PROMPT_SHA256)
        self.assertEqual(prompt["prompt_version"], lbr.PROMPT_VERSION)
        self.assertEqual(prompt["model_id"], lbr.MODEL_ID)

    def test_prompt_tampering_breaks_pin(self) -> None:
        prompt = lbr.load_prompt()
        tampered = copy.deepcopy(prompt)
        tampered["developer_instructions"] += " Rank candidates by region."
        errors = lbr.validate_prompt(tampered)
        self.assertTrue(any("bound candidate-review prompt" in error for error in errors))
        wrong_model = copy.deepcopy(prompt)
        wrong_model["model_id"] = "gpt-other"
        self.assertTrue(any("gpt-5.6-luna" in error for error in lbr.validate_prompt(wrong_model)))

    # ------------------------------------------------------------------
    # Seed / bundle validation.
    # ------------------------------------------------------------------

    def test_seed_and_bundle_validation_fail_closed(self) -> None:
        seed = copy.deepcopy(self.seeds[0])
        seed["professional_facts"][0]["fact_type"] = "ethnicity"
        with self.assertRaisesRegex(lbr.LunaBatchRunnerError, "seed_fact_type_invalid"):
            lbr.validate_seed_input(seed)
        duplicate = copy.deepcopy(self.seeds[0])
        duplicate["professional_facts"].append(dict(duplicate["professional_facts"][0]))
        with self.assertRaisesRegex(lbr.LunaBatchRunnerError, "seed_fact_evidence_ref_duplicate"):
            lbr.validate_seed_input(duplicate)
        bundle = copy.deepcopy(self.bundles[self.refs[0]])
        bundle["account_resolution"]["handle"] = "way_too_long_handle_16"
        with self.assertRaisesRegex(lbr.LunaBatchRunnerError, "account_resolution_handle_invalid"):
            lbr.validate_candidate_bundle(bundle, seed=self.seeds[0])
        wrong_person = copy.deepcopy(self.bundles[self.refs[0]])
        wrong_person["candidate_ref"] = "fixture_luna_candidate_999"
        with self.assertRaisesRegex(lbr.LunaBatchRunnerError, "candidate_bundle_seed_mismatch"):
            lbr.validate_candidate_bundle(wrong_person, seed=self.seeds[0])

    # ------------------------------------------------------------------
    # Grok collection runner.
    # ------------------------------------------------------------------

    def test_grok_prompt_anchors_identity_and_argv_is_headless(self) -> None:
        seed = self.seeds[0]
        prompt = lbr.build_grok_identity_prompt(seed)
        self.assertIn(seed["name_text"], prompt)
        self.assertIn("Fixture Lab", prompt)
        self.assertIn("Example Institute", prompt)
        self.assertIn(f"candidate_ref: {seed['seed_ref']}", prompt)
        argv = lbr.build_grok_argv(prompt)
        self.assertEqual(argv[0], "grok")
        self.assertEqual(argv[1], "-p")
        self.assertEqual(argv[2], prompt)
        self.assertEqual(argv[3:], ["--output-format", "json"])

    def test_grok_collection_receipts_bundles_and_ordering(self) -> None:
        transport = lbr.OfflineFakeGrokTransport(bundles=self.bundles, cost_usd=0.01)
        result = lbr.run_grok_collection(seeds=self.seeds, transport=transport)
        self.assertEqual(result["completed_count"], 2)
        self.assertEqual(result["failed_count"], 0)
        self.assertEqual([row["candidate_ref"] for row in result["results"]], self.refs)
        first = result["results"][0]
        self.assertEqual(first["bundle"], lbr.validate_candidate_bundle(self.bundles[self.refs[0]], seed=self.seeds[0]))
        receipt = first["operator_receipt"]
        self.assertEqual(receipt["schema_version"], lbr.GROK_OPERATOR_RECEIPT_SCHEMA_VERSION)
        self.assertEqual(receipt["cost_usd"], 0.01)
        call = next(c for c in transport.calls if f"candidate_ref: {self.refs[0]}" in c["prompt"])
        self.assertEqual(
            receipt["prompt_sha256"],
            hashlib.sha256(call["prompt"].encode("utf-8")).hexdigest(),
        )
        self.assertEqual(receipt["session_id"], call["session_id"])
        self.assertEqual(receipt["outcome"], "completed")

    def test_grok_collection_failure_isolation(self) -> None:
        transport = lbr.OfflineFakeGrokTransport(
            bundles=self.bundles,
            failures={self.refs[1]: "grok_cli_exit_nonzero"},
        )
        result = lbr.run_grok_collection(seeds=self.seeds, transport=transport)
        self.assertEqual(result["completed_count"], 1)
        self.assertEqual(result["failed_count"], 1)
        failed = result["results"][1]
        self.assertEqual(failed["status"], "failed")
        self.assertEqual(failed["error_code"], "grok_cli_exit_nonzero")
        self.assertEqual(failed["operator_receipt"]["outcome"], "failed")

    # ------------------------------------------------------------------
    # Luna request building.
    # ------------------------------------------------------------------

    def test_luna_request_contains_identity_anchor_and_full_bundle(self) -> None:
        prompt = lbr.load_prompt()
        seed = self.seeds[0]
        bundle = self.bundles[self.refs[0]]
        payload = lbr.build_luna_responses_payload(seed, bundle, prompt=prompt)
        self.assertEqual(payload["model"], lbr.MODEL_ID)
        self.assertEqual(payload["tools"], [])
        self.assertIs(payload["store"], False)
        self.assertEqual(payload["instructions"], prompt["developer_instructions"])
        strict = payload["text"]["format"]
        self.assertEqual(strict["type"], "json_schema")
        self.assertIs(strict["strict"], True)
        self.assertIs(strict["schema"]["additionalProperties"], False)
        self.assertEqual(set(strict["schema"]["required"]), lbr._JUDGED_OUTPUT_KEYS)
        self.assertEqual(_find_refs(strict["schema"]), [])
        text = payload["input"][0]["content"][0]["text"]
        source = json.loads(text)
        self.assertEqual(source["task"], "luna_candidate_review")
        self.assertEqual(source["candidate_ref"], seed["seed_ref"])
        self.assertEqual(source["identity_context"]["name_text"], seed["name_text"])
        self.assertEqual(source["identity_context"]["current_labs"], ["Fixture Lab"])
        self.assertEqual(source["identity_context"]["past_labs"], ["Example Institute"])
        judged = source["bundle"]
        self.assertEqual(judged["x_bio"]["text"], bundle["x_bio"]["text"])
        self.assertEqual([item["text"] for item in judged["items"]], [item["text"] for item in bundle["items"]])
        self.assertEqual(judged["seed_facts"], seed["professional_facts"])
        self.assertEqual(len(source["judged_bundle_manifest"]), 6)
        _, expected_sha = lbr.build_judged_bundle_manifest(bundle, seed=seed)
        self.assertEqual(source["judged_bundle_sha256"], expected_sha)
        self.assertEqual(payload["metadata"]["judged_bundle_sha256"], expected_sha)
        self.assertEqual(payload["metadata"]["prompt_sha256"], lbr.CANONICAL_PROMPT_SHA256)

    # ------------------------------------------------------------------
    # Luna batch + binding.
    # ------------------------------------------------------------------

    def test_batch_reviews_bind_recomputed_digest_and_citations(self) -> None:
        result, transport = self._run_batch()
        self.assertEqual(result["completed_count"], 2)
        self.assertEqual(result["failed_count"], 0)
        self.assertEqual(result["approval_receipt"], self._approval())
        self.assertEqual(result["prompt_sha256"], lbr.CANONICAL_PROMPT_SHA256)
        self.assertEqual(len(transport.calls), 2)
        for row in result["results"]:
            review = row["review"]
            assert_schema_valid(review, lbr.REVIEW_SCHEMA_FILE)
            seed = self.seed_by_ref[row["candidate_ref"]]
            bundle = self.bundles[row["candidate_ref"]]
            _, recomputed = lbr.build_judged_bundle_manifest(bundle, seed=seed)
            self.assertEqual(review["judged_bundle_sha256"], recomputed)
            judged_refs = {item["item_ref"] for item in review["judged_bundle_manifest"]}
            citations = (
                review["lab_affiliation_evidence_citations"]
                + review["pretraining_experience_evidence_citations"]
            )
            self.assertTrue(all(citation in judged_refs for citation in citations))
            receipt = row["execution_receipt"]
            self.assertEqual(receipt["requested_model"], lbr.MODEL_ID)
            self.assertEqual(receipt["returned_model"], lbr.MODEL_ID)
            self.assertIs(receipt["exact_model_match"], True)
            self.assertEqual(receipt["endpoint"], lbr.RESPONSES_URL)
            self.assertEqual(receipt["outcome"], "completed")
        lbr.validate_candidate_review_binding(
            result["results"][0]["review"], seed=self.seeds[0], bundle=self.bundles[self.refs[0]]
        )

    def test_binding_rejects_tampered_bundle(self) -> None:
        review = self._review(self.refs[0])
        tampered_bio = copy.deepcopy(self.bundles[self.refs[0]])
        tampered_bio["x_bio"]["text"] += " Tampered sentence."
        with self.assertRaisesRegex(lbr.LunaBatchRunnerError, "luna_review_bundle_binding_invalid"):
            lbr.validate_candidate_review_binding(review, seed=self.seeds[0], bundle=tampered_bio)
        tampered_items = copy.deepcopy(self.bundles[self.refs[0]])
        tampered_items["items"] = tampered_items["items"][:1]
        with self.assertRaisesRegex(lbr.LunaBatchRunnerError, "luna_review_bundle_binding_invalid"):
            lbr.validate_candidate_review_binding(review, seed=self.seeds[0], bundle=tampered_items)
        tampered_manifest = copy.deepcopy(review)
        tampered_manifest["judged_bundle_manifest"] = tampered_manifest["judged_bundle_manifest"][::-1]
        with self.assertRaisesRegex(lbr.LunaBatchRunnerError, "luna_review_manifest_binding_invalid"):
            lbr.validate_candidate_review_binding(
                tampered_manifest, seed=self.seeds[0], bundle=self.bundles[self.refs[0]]
            )

    def test_binding_rejects_citations_outside_judged_items(self) -> None:
        bad_output = copy.deepcopy(self.outputs[self.refs[0]])
        bad_output["pretraining_experience_evidence_citations"] = ["post:9999999999999999999"]
        with self.assertRaisesRegex(lbr.LunaBatchRunnerError, "luna_output_citation_not_judged"):
            lbr.build_candidate_review(self.seeds[0], self.bundles[self.refs[0]], bad_output)
        forged = copy.deepcopy(self.outputs[self.refs[0]])
        forged["lab_affiliation_evidence_citations"] = ["seed_fact:attacker-controlled-ref"]
        with self.assertRaisesRegex(lbr.LunaBatchRunnerError, "luna_output_citation_not_judged"):
            lbr.build_candidate_review(self.seeds[0], self.bundles[self.refs[0]], forged)

    def test_returned_model_mismatch_fails_closed(self) -> None:
        transport = lbr.OfflineFakeLunaTransport(outputs=self.outputs, model_id="gpt-other")
        result, _ = self._run_batch(transport=transport)
        self.assertEqual(result["completed_count"], 0)
        self.assertEqual(result["failed_count"], 2)
        for row in result["results"]:
            self.assertEqual(row["error_code"], "luna_response_model_mismatch")
            self.assertEqual(row["execution_receipt"]["outcome"], "failed")
            self.assertIs(row["execution_receipt"]["exact_model_match"], False)

    def test_duplicate_key_model_output_rejected(self) -> None:
        text = (
            '{"proposed_lab_affiliation_state":"current",'
            '"proposed_lab_affiliation_state":"historical",'
            '"proposed_pretraining_experience_state":"current",'
            '"lab_affiliation_evidence_citations":[],'
            '"pretraining_experience_evidence_citations":[]}'
        )
        body = {
            "id": "resp_fake_dup",
            "object": "response",
            "status": "completed",
            "model": lbr.MODEL_ID,
            "output": [
                {
                    "id": "msg_fake_dup",
                    "type": "message",
                    "status": "completed",
                    "role": "assistant",
                    "content": [{"type": "output_text", "annotations": [], "text": text}],
                }
            ],
        }
        with self.assertRaisesRegex(lbr.LunaBatchRunnerError, "luna_output_json_invalid"):
            lbr.extract_judged_output(body)

    # ------------------------------------------------------------------
    # Adapter -> unchanged reducer.
    # ------------------------------------------------------------------

    def test_adapter_feeds_unchanged_reducer_with_design_pinned_rows(self) -> None:
        result, _ = self._run_batch()
        reviews = [row["review"] for row in result["results"]]
        reduction = lbr.candidate_reviews_to_axis_reduction(
            reviews=reviews,
            seeds=self.seeds,
            bundles=self.bundles,
            lab_descriptor=self.fixture["lab_descriptor"],
        )
        assert_schema_valid(reduction, LUNA_AXIS_REDUCTION_SCHEMA_FILE)
        self.assertEqual(reduction["schema_version"], LUNA_AXIS_REDUCTION_SCHEMA_VERSION)
        self.assertEqual(reduction["coverage_status"], "complete")
        self.assertEqual(reduction["authorized_transition_count"], 0)
        self.assertEqual(reduction["transition_authority"], "diagnostic_only_unattested")
        review_by_ref = {review["candidate_ref"]: review for review in reviews}
        self.assertEqual(len(reduction["candidate_axis_rows"]), 2 * len(self.refs))
        for row in reduction["candidate_axis_rows"]:
            review = review_by_ref[row["candidate_ref"]]
            self.assertEqual(row["reviewed_evidence_count"], len(review["judged_bundle_manifest"]))
            self.assertEqual(row["expected_evidence_count"], len(review["judged_bundle_manifest"]))
            self.assertEqual(row["reviewed_evidence_manifest_sha256"], review["judged_bundle_sha256"])
            self.assertEqual(row["coverage_status"], "complete")
            self.assertEqual(row["resolved_state"], "unsupported")
            self.assertEqual(row["transition_status"], "not_authorized")
            self.assertIs(row["transition_id"], None)
            if row["axis"] == "lab_affiliation":
                self.assertEqual(row["diagnostic_proposed_state"], review["proposed_lab_affiliation_state"])
            else:
                self.assertEqual(row["diagnostic_proposed_state"], review["proposed_pretraining_experience_state"])

    def test_adapter_preserves_explicit_priors_and_rejects_unbound_reviews(self) -> None:
        result, _ = self._run_batch()
        reviews = [row["review"] for row in result["results"]]
        priors = {
            self.refs[0]: {
                "lab_affiliation": {"state": "current", "evidence_status": "source_bound"},
                "pretraining_experience": {"state": "unsupported", "evidence_status": "unsupported"},
            }
        }
        reduction = lbr.candidate_reviews_to_axis_reduction(
            reviews=reviews,
            seeds=self.seeds,
            bundles=self.bundles,
            lab_descriptor=self.fixture["lab_descriptor"],
            priors=priors,
        )
        first_lab = next(
            row
            for row in reduction["candidate_axis_rows"]
            if row["candidate_ref"] == self.refs[0] and row["axis"] == "lab_affiliation"
        )
        self.assertEqual(first_lab["prior_state"], "current")
        self.assertEqual(first_lab["resolved_state"], "current")
        self.assertEqual(first_lab["diagnostic_proposed_state"], "current")
        tampered = copy.deepcopy(reviews[0])
        tampered["judged_bundle_sha256"] = "0" * 64
        with self.assertRaisesRegex(lbr.LunaBatchRunnerError, "luna_review_bundle_binding_invalid"):
            lbr.candidate_reviews_to_axis_reduction(
                reviews=[tampered],
                seeds=self.seeds,
                bundles=self.bundles,
                lab_descriptor=self.fixture["lab_descriptor"],
            )
        unknown = copy.deepcopy(reviews[0])
        unknown["candidate_ref"] = "fixture_luna_candidate_999"
        with self.assertRaisesRegex(lbr.LunaBatchRunnerError, "luna_review_candidate_unknown"):
            lbr.candidate_reviews_to_axis_reduction(
                reviews=[unknown],
                seeds=self.seeds,
                bundles=self.bundles,
                lab_descriptor=self.fixture["lab_descriptor"],
            )

    # ------------------------------------------------------------------
    # Worker pool determinism + failure isolation.
    # ------------------------------------------------------------------

    def test_worker_pool_restores_seed_ordinal_order(self) -> None:
        latency = {self.refs[0]: 0.35, self.refs[1]: 0.0}
        fast = lbr.OfflineFakeLunaTransport(outputs=self.outputs, latency_s=latency)
        slow = lbr.OfflineFakeLunaTransport(outputs=self.outputs, latency_s=latency)
        result_fast, _ = self._run_batch(transport=fast, worker_count=16)
        result_slow, _ = self._run_batch(transport=slow, worker_count=1)
        self.assertEqual(
            [row["candidate_ref"] for row in result_fast["results"]],
            self.refs,
        )
        self.assertEqual(
            [row["candidate_ref"] for row in result_fast["results"]],
            [row["candidate_ref"] for row in result_slow["results"]],
        )
        self.assertEqual(
            [row["review"] for row in result_fast["results"]],
            [row["review"] for row in result_slow["results"]],
        )

    def test_luna_failure_isolation_keeps_batch_alive(self) -> None:
        transport = lbr.OfflineFakeLunaTransport(
            outputs=self.outputs,
            failures={self.refs[1]: "luna_transport_boom"},
        )
        result, _ = self._run_batch(transport=transport)
        self.assertEqual(result["completed_count"], 1)
        self.assertEqual(result["failed_count"], 1)
        completed, failed = result["results"]
        self.assertEqual(completed["candidate_ref"], self.refs[0])
        self.assertIsNotNone(completed["review"])
        self.assertEqual(failed["candidate_ref"], self.refs[1])
        self.assertEqual(failed["status"], "failed")
        self.assertEqual(failed["error_code"], "luna_transport_boom")
        self.assertIsNone(failed["review"])
        self.assertEqual(failed["execution_receipt"]["outcome"], "failed")
        self.assertEqual(failed["execution_receipt"]["error_code"], "luna_transport_boom")

    # ------------------------------------------------------------------
    # Judgment-model binding (model-agnostic judge layer).
    # ------------------------------------------------------------------

    def test_substitute_judgment_binding_end_to_end(self) -> None:
        binding = lbr.judgment_binding_for_model(
            provider_id="deepseek_test_relay",
            endpoint="https://example.test/v1/chat/completions",
            model_id="deepseek-v4-flash",
        )
        transport = lbr.OfflineFakeLunaTransport(outputs=self.outputs, model_id="deepseek-v4-flash")
        result, _ = self._run_batch(transport=transport, binding=binding)

        self.assertEqual(result["completed_count"], len(self.refs))
        self.assertEqual(result["prompt_sha256"], binding.prompt_sha256)
        self.assertNotEqual(result["prompt_sha256"], lbr.CANONICAL_PROMPT_SHA256)
        for call in transport.calls:
            self.assertEqual(call["payload"]["model"], "deepseek-v4-flash")
            self.assertEqual(call["payload"]["metadata"]["prompt_sha256"], binding.prompt_sha256)
        for row in result["results"]:
            receipt = row["execution_receipt"]
            self.assertEqual(receipt["provider"], "deepseek_test_relay")
            self.assertEqual(receipt["endpoint"], "https://example.test/v1/chat/completions")
            self.assertEqual(receipt["requested_model"], "deepseek-v4-flash")
            self.assertEqual(receipt["returned_model"], "deepseek-v4-flash")
            self.assertTrue(receipt["exact_model_match"])
            self.assertEqual(receipt["prompt_sha256"], binding.prompt_sha256)

    def test_binding_prompt_hash_mismatch_fails_closed_before_any_call(self) -> None:
        binding = lbr.JudgmentModelBinding(
            provider_id="deepseek_test_relay",
            endpoint="https://example.test/v1/chat/completions",
            model_id="deepseek-v4-flash",
            prompt_sha256="0" * 64,
            prompt_asset={
                "schema_version": lbr.PROMPT_SCHEMA_VERSION,
                "prompt_version": lbr.PROMPT_VERSION,
                "model_id": "deepseek-v4-flash",
                "developer_instructions": "x" * 120,
            },
        )
        transport = lbr.OfflineFakeLunaTransport(outputs=self.outputs, model_id="deepseek-v4-flash")
        with self.assertRaisesRegex(lbr.LunaBatchRunnerError, "luna_candidate_review_prompt_invalid"):
            self._run_batch(transport=transport, binding=binding)
        self.assertEqual(transport.calls, [])

    # ------------------------------------------------------------------
    # Approval-receipt gate.
    # ------------------------------------------------------------------

    def test_approval_gate_blocks_without_receipt(self) -> None:
        transport = lbr.OfflineFakeLunaTransport(outputs=self.outputs)
        with self.assertRaisesRegex(PermissionError, "luna_approval_receipt_required"):
            lbr.run_luna_batch(
                seeds=self.seeds,
                bundles=self.bundles,
                transport=transport,
                approval=None,
            )
        self.assertEqual(transport.calls, [])

    def test_approval_gate_blocks_unbound_or_malformed_receipts(self) -> None:
        transport = lbr.OfflineFakeLunaTransport(outputs=self.outputs)
        wrong_binding = self._approval(refs=[self.refs[1], self.refs[0]])
        with self.assertRaisesRegex(PermissionError, "luna_approval_candidate_binding_invalid"):
            lbr.run_luna_batch(
                seeds=self.seeds,
                bundles=self.bundles,
                transport=transport,
                approval=wrong_binding,
            )
        malformed = self._approval()
        malformed["approved_at"] = "2026-07-19"
        with self.assertRaisesRegex(PermissionError, "luna_approval_receipt_invalid"):
            lbr.run_luna_batch(
                seeds=self.seeds,
                bundles=self.bundles,
                transport=transport,
                approval=malformed,
            )
        self.assertEqual(transport.calls, [])

    def test_streaming_pipeline_interleaves_collection_and_judgment_per_candidate(self) -> None:
        events: list[tuple[str, str]] = []
        lock = threading.Lock()

        class EventGrok(lbr.OfflineFakeGrokTransport):
            def run(self, *, argv, prompt, session_id, timeout_ms):
                ref = next(r for r in self._bundles if f"candidate_ref: {r}" in prompt)
                with lock:
                    events.append(("grok", ref))
                return super().run(argv=argv, prompt=prompt, session_id=session_id, timeout_ms=timeout_ms)

        class EventLuna(lbr.OfflineFakeLunaTransport):
            def complete(self, *, payload, timeout_ms):
                with lock:
                    events.append(("luna", payload["metadata"]["candidate_ref"]))
                return super().complete(payload=payload, timeout_ms=timeout_ms)

        result = lbr.run_streaming_pipeline(
            seeds=self.seeds,
            grok_transport=EventGrok(bundles=self.bundles),
            luna_transport=EventLuna(outputs=self.outputs),
            approval=self._approval(),
            worker_count=1,
        )
        # one worker fuses collect->judge per candidate: strict interleaving, no stage barrier
        expected = [(kind, ref) for ref in self.refs for kind in ("grok", "luna")]
        self.assertEqual(events, expected)
        self.assertEqual(result["schema_version"], lbr.PIPELINE_RESULT_SCHEMA_VERSION)
        self.assertEqual(result["grok_completed_count"], len(self.refs))
        self.assertEqual(result["luna_completed_count"], len(self.refs))
        for row in result["results"]:
            self.assertEqual(row["grok"]["status"], "completed")
            self.assertEqual(row["luna"]["status"], "completed")
            self.assertIsNotNone(row["luna"]["review"])
            self.assertEqual([r["candidate_ref"] for r in result["results"]], self.refs)

    def test_streaming_pipeline_grok_failure_marks_luna_not_attempted(self) -> None:
        failing = self.refs[1]
        grok = lbr.OfflineFakeGrokTransport(bundles=self.bundles, failures={failing: "grok_cli_exit_nonzero"})
        luna = lbr.OfflineFakeLunaTransport(outputs=self.outputs)
        result = lbr.run_streaming_pipeline(
            seeds=self.seeds,
            grok_transport=grok,
            luna_transport=luna,
            approval=self._approval(),
            worker_count=3,
        )
        by_ref = {row["candidate_ref"]: row for row in result["results"]}
        self.assertEqual(by_ref[failing]["grok"]["status"], "failed")
        self.assertEqual(by_ref[failing]["grok"]["error_code"], "grok_cli_exit_nonzero")
        self.assertEqual(by_ref[failing]["luna"]["status"], "not_attempted")
        self.assertIsNone(by_ref[failing]["luna"]["execution_receipt"])
        self.assertEqual(by_ref[self.refs[0]]["luna"]["status"], "completed")
        self.assertEqual(result["luna_completed_count"], len(self.refs) - 1)
        judged = {call["payload"]["metadata"]["candidate_ref"] for call in luna.calls}
        self.assertNotIn(failing, judged)

    def test_streaming_pipeline_luna_failure_never_discards_bundle(self) -> None:
        failing = self.refs[0]
        luna = lbr.OfflineFakeLunaTransport(outputs=self.outputs, failures={failing: "luna_transport_failed"})
        result = lbr.run_streaming_pipeline(
            seeds=self.seeds,
            grok_transport=lbr.OfflineFakeGrokTransport(bundles=self.bundles),
            luna_transport=luna,
            approval=self._approval(),
            worker_count=2,
        )
        by_ref = {row["candidate_ref"]: row for row in result["results"]}
        self.assertEqual(by_ref[failing]["grok"]["status"], "completed")
        self.assertIsNotNone(by_ref[failing]["grok"]["bundle"])
        self.assertEqual(by_ref[failing]["luna"]["status"], "failed")
        self.assertEqual(by_ref[failing]["luna"]["error_code"], "luna_transport_failed")
        self.assertEqual(result["grok_completed_count"], len(self.refs))
        self.assertEqual(result["luna_completed_count"], len(self.refs) - 1)

    def test_streaming_pipeline_requires_approval_before_any_transport_call(self) -> None:
        grok = lbr.OfflineFakeGrokTransport(bundles=self.bundles)
        luna = lbr.OfflineFakeLunaTransport(outputs=self.outputs)
        with self.assertRaisesRegex(PermissionError, "luna_approval_receipt_required"):
            lbr.run_streaming_pipeline(
                seeds=self.seeds,
                grok_transport=grok,
                luna_transport=luna,
                approval=None,
            )
        self.assertEqual(grok.calls, [])
        self.assertEqual(luna.calls, [])

    def test_streaming_pipeline_event_level_no_idle_under_skew(self) -> None:
        # slow grok for the first seed must not idle the fast candidate's judgment
        slow, fast = self.refs[0], self.refs[1]
        grok = lbr.OfflineFakeGrokTransport(bundles=self.bundles, latency_s={slow: 0.6})
        luna = lbr.OfflineFakeLunaTransport(outputs=self.outputs)
        started = time.monotonic()
        result = lbr.run_streaming_pipeline(
            seeds=[self.seed_by_ref[slow], self.seed_by_ref[fast]],
            grok_transport=grok,
            luna_transport=luna,
            approval=self._approval(refs=[slow, fast]),
            worker_count=2,
        )
        elapsed = time.monotonic() - started
        self.assertEqual(result["luna_completed_count"], 2)
        self.assertLess(elapsed, 1.2, "fused pipeline must overlap independent candidates")

    def test_shared_post_across_candidates_no_digest_collision(self) -> None:
        # the same public post legitimately appears in two bundles; candidate-scoped
        # per-item digests keep the adapter's global digest keyspace collision-free
        ref_a, ref_b = self.refs[0], self.refs[1]
        seed_a, seed_b = self.seed_by_ref[ref_a], self.seed_by_ref[ref_b]
        bundle_a = lbr.validate_candidate_bundle(copy.deepcopy(self.bundles[ref_a]), seed=seed_a)
        bundle_b = copy.deepcopy(self.bundles[ref_b])
        shared_post = copy.deepcopy((bundle_a["items"] or [None])[0])
        if shared_post is None:
            self.skipTest("fixture rich bundle has no items")
        bundle_b["items"] = (bundle_b["items"] or []) + [shared_post]
        bundle_b = lbr.validate_candidate_bundle(bundle_b, seed=seed_b)
        manifest_a, digest_a = lbr.build_judged_bundle_manifest(bundle_a, seed=seed_a)
        manifest_b, digest_b = lbr.build_judged_bundle_manifest(bundle_b, seed=seed_b)
        shas = [item["sha256"] for item in manifest_a + manifest_b]
        self.assertEqual(len(shas), len(set(shas)), "candidate-scoped digests must be globally unique")
        self.assertNotEqual(digest_a, digest_b)
        reviews = [
            lbr.build_candidate_review(seed_a, bundle_a, self.outputs[ref_a]),
            lbr.build_candidate_review(seed_b, bundle_b, self.outputs[ref_b]),
        ]
        reduction = lbr.candidate_reviews_to_axis_reduction(
            reviews=reviews,
            seeds=[seed_a, seed_b],
            bundles={ref_a: bundle_a, ref_b: bundle_b},
            lab_descriptor=self.fixture["lab_descriptor"],
        )
        self.assertEqual(reduction["schema_version"], LUNA_AXIS_REDUCTION_SCHEMA_VERSION)
        for row in reduction["candidate_axis_rows"]:
            self.assertEqual(row["coverage_status"], "complete")

    def test_not_found_resolution_shape_and_live_fit_repairs(self) -> None:
        seed = self.seed_by_ref[self.refs[1]]
        base = copy.deepcopy(self.bundles[self.refs[1]])
        # explicit negative resolution: empty handle + negative-search receipts
        not_found = copy.deepcopy(base)
        not_found["account_resolution"] = {
            "handle": "",
            "resolution_confidence": "not_found",
            "evidence_receipts": [
                {"receipt_kind": "x_user_search", "receipt_ref": "negative:name+labs"},
            ],
        }
        checked = lbr.validate_candidate_bundle(not_found, seed=seed)
        self.assertEqual(checked["account_resolution"]["resolution_confidence"], "not_found")
        # not_found with a non-empty handle or without receipts fails closed
        for mutate in (
            lambda value: value["account_resolution"].update({"handle": "s0mehandle"}),
            lambda value: value["account_resolution"].update({"evidence_receipts": []}),
        ):
            bad = copy.deepcopy(not_found)
            mutate(bad)
            with self.assertRaisesRegex(lbr.LunaBatchRunnerError, "account_resolution_not_found_invalid"):
                lbr.validate_candidate_bundle(bad, seed=seed)
        # live-fit repairs: narration-tolerant terminal JSON extraction, caller-owned
        # ref normalization, and CLI-minted session adoption
        class NarrationTransport:
            def run(self, *, argv, prompt, session_id, timeout_ms):
                return {
                    "sessionId": "cli-minted-session",
                    "stopReason": "EndTurn",
                    "text": f"narrating the search first. {json.dumps(base)}",
                }

        bundle, receipt = lbr.collect_candidate_bundle(seed, transport=NarrationTransport())
        self.assertEqual(bundle["candidate_ref"], seed["seed_ref"])
        self.assertEqual(receipt["session_id"], "cli-minted-session")


if __name__ == "__main__":
    unittest.main()
