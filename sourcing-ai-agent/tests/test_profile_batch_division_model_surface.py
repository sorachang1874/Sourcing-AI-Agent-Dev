"""WS7/W7.2 S2 offline suite for the divider model-invocation surface.

Provenance: docs/WS7_AI_BATCH_DIVIDER_DESIGN.md §2 (model invocation surface)
+ §3 (F1-F6), OQ1/OQ5/OQ6/OQ7/OQ8 RATIFIED 2026-07-23 (slice S2 per §7,
additive only — no flip, no dispatch-path changes). Pins: the
`divide_profile_prefetch_batches` ModelClient method (Deterministic → {} =
F1 marker; scripted divider client behind SOURCING_SCRIPTED_PROFILE_BATCH_DIVIDER
per OQ7; OpenAI-compatible/Qwen raw-output + divider-scoped 20 s timeout per
OQ8), the propose_and_validate_division helper's OQ5 >300 engagement gate and
once-per-mint call discipline (OQ6), and the mapping of every failure class to
the ruling-④ fallback audit shapes via the S1 contract
(profile_batch_division_contract — validation single-sourced there; companion
suite tests/test_profile_batch_division_contract.py).
"""

from __future__ import annotations

import json
import os
import unittest
from typing import Any
from unittest.mock import patch

import requests

import sourcing_agent.model_provider as model_provider_module
from sourcing_agent.model_provider import (
    PROFILE_BATCH_DIVIDER_CIRCUIT_ERROR_PREFIX,
    PROFILE_BATCH_DIVIDER_RESPONSE_DIVISION_KEY,
    PROFILE_BATCH_DIVIDER_RESPONSE_ERROR_KEY,
    PROFILE_BATCH_DIVIDER_RESPONSE_PROVENANCE_KEY,
    DeterministicModelClient,
    OfflineModelClient,
    OpenAICompatibleChatModelClient,
    QwenResponsesModelClient,
    ScriptedLivePlanningModelClient,
    ScriptedProfileBatchDividerModelClient,
    _build_profile_batch_division_system_prompt,
    _model_provider_circuit_key,
    _record_model_provider_failure,
    _reset_model_provider_circuits_for_tests,
    build_model_client,
)
from sourcing_agent.profile_batch_division import (
    AI_DIVIDER_ENGAGEMENT_THRESHOLD_URLS,
    DIVISION_PROPOSAL_STATUS_FALLBACK,
    DIVISION_PROPOSAL_STATUS_PROPOSED,
    SKIP_REASON_BELOW_ENGAGEMENT_THRESHOLD,
    build_divider_input_payload,
    build_fallback_audit,
    division_membership_sha256,
    propose_and_validate_division,
    stale_input_fallback_audit,
)
from sourcing_agent.profile_batch_division_contract import (
    DIVISION_SOURCE_RULE_LADDER_FALLBACK,
    FALLBACK_REASON_CALL_FAILED,
    FALLBACK_REASON_CIRCUIT_OPEN,
    FALLBACK_REASON_INPUT_STALE,
    FALLBACK_REASON_INVALID_OUTPUT,
    FALLBACK_REASON_MODEL_UNAVAILABLE,
    SCHEMA_ID_V1,
    VALIDATOR_V1_PROVIDER_ENVELOPE,
    normalize_ai_batch_division,
    normalize_fallback_audit,
    validate_ai_batch_division,
)
from sourcing_agent.settings import ModelProviderSettings, QwenSettings

_SCRIPTED_DIVIDER_ENV = {"SOURCING_SCRIPTED_PROFILE_BATCH_DIVIDER": "1"}


def _inventory(size: int) -> list[str]:
    return [f"linkedin.com/in/profile-{index:04d}" for index in range(size)]


def _valid_provenance() -> dict[str, Any]:
    return {
        "model_provider": "stub_provider",
        "requested_model": "stub-model",
        "response_model": "stub-model",
        "prompt_sha256": "a" * 64,
        "input_snapshot_sha256": "b" * 64,
        "usage": {"input_tokens": 10, "output_tokens": 5},
        "latency_ms": 12,
    }


class _StubDividerClient(DeterministicModelClient):
    """Spy client: canned divide response + call counting for OQ5/OQ6 pins."""

    def __init__(self, response: dict[str, Any]) -> None:
        self.response = response
        self.divide_calls = 0
        self.payloads: list[dict[str, Any]] = []

    def divide_profile_prefetch_batches(self, payload: dict[str, Any]) -> dict[str, Any]:
        self.divide_calls += 1
        self.payloads.append(payload)
        return self.response


def _stub_success_response(batches: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        PROFILE_BATCH_DIVIDER_RESPONSE_DIVISION_KEY: {"batches": batches},
        PROFILE_BATCH_DIVIDER_RESPONSE_PROVENANCE_KEY: _valid_provenance(),
        PROFILE_BATCH_DIVIDER_RESPONSE_ERROR_KEY: "",
    }


def _contiguous_batches(total: int, batch_count: int) -> list[dict[str, Any]]:
    base, remainder = divmod(total, batch_count)
    batches: list[dict[str, Any]] = []
    cursor = 0
    for ordinal in range(1, batch_count + 1):
        size = base + (1 if ordinal <= remainder else 0)
        batches.append(
            {
                "batch_index": ordinal,
                "member_index_ranges": [[cursor, cursor + size - 1]],
                "member_count": size,
                "reason": f"test chunk {ordinal}/{batch_count}",
                "reason_code": "ai_division",
            }
        )
        cursor += size
    return batches


class ProtocolConformanceTest(unittest.TestCase):
    """Design §2.1: Deterministic default {} is the structural F1 marker."""

    def test_deterministic_client_returns_empty_division_marker(self) -> None:
        self.assertEqual(DeterministicModelClient().divide_profile_prefetch_batches({"inventory": {}}), {})

    def test_offline_client_inherits_the_f1_marker_so_simulate_is_fallback_by_construction(self) -> None:
        # Discrepancy D2: the canonical simulate/replay hot path can never
        # exercise the AI branch without the OQ7 opt-in.
        for mode in ("simulate", "replay", "scripted"):
            self.assertEqual(OfflineModelClient(mode=mode).divide_profile_prefetch_batches({}), {})

    def test_scripted_live_planning_client_does_not_gain_the_divider(self) -> None:
        # OQ7 ruling: dedicated flag, no piggyback on the planning opt-in.
        client = ScriptedLivePlanningModelClient(DeterministicModelClient(), mode="scripted")
        self.assertEqual(client.divide_profile_prefetch_batches({}), {})

    def test_deterministic_marker_maps_to_f1_fallback_audit_in_the_helper(self) -> None:
        proposal = propose_and_validate_division(DeterministicModelClient(), inventory=_inventory(400))
        self.assertEqual(proposal["status"], DIVISION_PROPOSAL_STATUS_FALLBACK)
        self.assertTrue(proposal["engaged"])
        self.assertIsNone(proposal["division"])
        audit = proposal["fallback_audit"]
        self.assertEqual(audit["division_source"], DIVISION_SOURCE_RULE_LADDER_FALLBACK)
        self.assertEqual(audit["fallback_reason"], FALLBACK_REASON_MODEL_UNAVAILABLE)
        self.assertIn("deterministic", audit["divider_error"])
        self.assertIsNone(audit["provenance"])
        normalize_fallback_audit(audit)  # F1 shape round-trips the S1 contract


class ScriptedDividerClientTest(unittest.TestCase):
    """OQ7: opt-in env gate + schema-valid offline divisions."""

    def test_env_absent_returns_the_f1_marker_even_when_directly_constructed(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            client = ScriptedProfileBatchDividerModelClient(mode="simulate")
            self.assertEqual(client.divide_profile_prefetch_batches({"inventory": {"size": 400}}), {})

    def test_env_set_produces_a_payload_that_passes_the_full_s1_battery(self) -> None:
        inventory = _inventory(1000)
        with patch.dict(os.environ, _SCRIPTED_DIVIDER_ENV, clear=False):
            client = ScriptedProfileBatchDividerModelClient(mode="simulate")
            proposal = propose_and_validate_division(
                client, inventory=inventory, retry_wait_indices=[3, 999], actor_global_inflight=4
            )
        self.assertEqual(proposal["status"], DIVISION_PROPOSAL_STATUS_PROPOSED)
        division = proposal["division"]
        self.assertEqual(division["schema_id"], SCHEMA_ID_V1)
        self.assertEqual(division["division_id"], proposal["division_id"])
        # Re-validate the recorded payload end-to-end through the S1 battery.
        revalidated = validate_ai_batch_division(
            {**division, "validator_results": []},
            inventory_size=len(inventory),
            retry_wait_indices={3, 999},
            actor_global_inflight=4,
        )
        self.assertTrue(revalidated["valid"], revalidated["failures"])
        self.assertGreaterEqual(division["batch_count"], 4)
        self.assertLessEqual(division["batch_count"], 8)
        self.assertEqual(sum(batch["member_count"] for batch in division["batches"]), 998)
        # Membership hash is caller-derived from the raw ranges over the
        # canonical inventory ordering — recompute must agree.
        self.assertEqual(
            division["membership_sha256"],
            division_membership_sha256(division["batches"], inventory),
        )

    def test_normalized_scripted_division_round_trips_the_contract_normalizer(self) -> None:
        with patch.dict(os.environ, _SCRIPTED_DIVIDER_ENV, clear=False):
            client = ScriptedProfileBatchDividerModelClient(mode="scripted")
            proposal = propose_and_validate_division(client, inventory=_inventory(400))
        division = dict(proposal["division"])
        division["validator_results"] = []
        self.assertEqual(normalize_ai_batch_division(division).to_payload(), division)

    def test_oversized_eligible_set_is_rejected_into_f5_by_the_battery(self) -> None:
        # (300, 2400] is the valid scripted band: beyond it V1×V2 cannot both
        # hold, mirroring the ladder's 8-cap+defer the AI band cannot express.
        with patch.dict(os.environ, _SCRIPTED_DIVIDER_ENV, clear=False):
            client = ScriptedProfileBatchDividerModelClient(mode="simulate")
            proposal = propose_and_validate_division(client, inventory=_inventory(2500))
        self.assertEqual(proposal["status"], DIVISION_PROPOSAL_STATUS_FALLBACK)
        self.assertEqual(
            proposal["fallback_audit"]["fallback_reason"],
            f"divider_validator_rejected:{VALIDATOR_V1_PROVIDER_ENVELOPE}",
        )

    def test_build_model_client_wires_the_scripted_divider_only_with_the_opt_in(self) -> None:
        with patch.dict(os.environ, {"SOURCING_EXTERNAL_PROVIDER_MODE": "simulate"}, clear=True):
            self.assertIsInstance(build_model_client(None, None), OfflineModelClient)
            self.assertNotIsInstance(build_model_client(None, None), ScriptedProfileBatchDividerModelClient)
        with patch.dict(
            os.environ,
            {"SOURCING_EXTERNAL_PROVIDER_MODE": "simulate", **_SCRIPTED_DIVIDER_ENV},
            clear=True,
        ):
            self.assertIsInstance(build_model_client(None, None), ScriptedProfileBatchDividerModelClient)

    def test_scripted_live_planning_opt_in_takes_precedence_over_the_divider_opt_in(self) -> None:
        # Documented precedence (build_model_client): the two opt-ins are not
        # composable in one client yet; planning wins and the divider falls
        # back per ruling ④.
        with patch.dict(
            os.environ,
            {
                "SOURCING_EXTERNAL_PROVIDER_MODE": "scripted",
                "SOURCING_SCRIPTED_LIVE_MODEL_PLANNING": "1",
                **_SCRIPTED_DIVIDER_ENV,
            },
            clear=True,
        ):
            client = build_model_client(
                ModelProviderSettings(enabled=True, api_key="sk-x", base_url="https://x/v1", model="m"),
                QwenSettings(enabled=False),
            )
        self.assertIsInstance(client, ScriptedLivePlanningModelClient)
        self.assertEqual(client.divide_profile_prefetch_batches({}), {})


class HelperEngagementThresholdTest(unittest.TestCase):
    """OQ5: engage only >300 eligible urls; smaller sets never call the model."""

    def test_at_threshold_returns_structural_skip_without_a_model_call(self) -> None:
        client = _StubDividerClient(_stub_success_response(_contiguous_batches(300, 4)))
        proposal = propose_and_validate_division(client, inventory=_inventory(300))
        self.assertEqual(client.divide_calls, 0)
        self.assertEqual(proposal["status"], DIVISION_PROPOSAL_STATUS_FALLBACK)
        self.assertFalse(proposal["engaged"])
        self.assertIsNone(proposal["division"])
        self.assertIsNone(proposal["fallback_audit"])
        self.assertEqual(proposal["skip_reason"], SKIP_REASON_BELOW_ENGAGEMENT_THRESHOLD)
        self.assertEqual(proposal["eligible_member_count"], 300)
        self.assertEqual(proposal["engagement_threshold_urls"], AI_DIVIDER_ENGAGEMENT_THRESHOLD_URLS)

    def test_retry_wait_members_do_not_count_toward_engagement(self) -> None:
        client = _StubDividerClient(_stub_success_response(_contiguous_batches(295, 4)))
        proposal = propose_and_validate_division(client, inventory=_inventory(320), retry_wait_indices=range(25))
        self.assertEqual(client.divide_calls, 0)
        self.assertFalse(proposal["engaged"])
        self.assertEqual(proposal["eligible_member_count"], 295)

    def test_above_threshold_calls_the_model_exactly_once(self) -> None:
        client = _StubDividerClient(_stub_success_response(_contiguous_batches(301, 4)))
        proposal = propose_and_validate_division(client, inventory=_inventory(301))
        self.assertEqual(client.divide_calls, 1)
        self.assertEqual(proposal["status"], DIVISION_PROPOSAL_STATUS_PROPOSED)
        self.assertTrue(proposal["engaged"])


class HelperFallbackMappingTest(unittest.TestCase):
    """Design §3: every failure class maps to the pinned audit shape."""

    def _proposal_for(self, response: dict[str, Any]) -> dict[str, Any]:
        return propose_and_validate_division(_StubDividerClient(response), inventory=_inventory(400))

    def test_f2_circuit_open_error_maps_to_divider_circuit_open(self) -> None:
        proposal = self._proposal_for(
            {
                PROFILE_BATCH_DIVIDER_RESPONSE_ERROR_KEY: f"{PROFILE_BATCH_DIVIDER_CIRCUIT_ERROR_PREFIX}:874s_remaining:boom"
            }
        )
        audit = proposal["fallback_audit"]
        self.assertEqual(audit["fallback_reason"], FALLBACK_REASON_CIRCUIT_OPEN)
        self.assertIn("874s_remaining", audit["divider_error"])
        self.assertIsNone(audit["provenance"])
        normalize_fallback_audit(audit)

    def test_f3_transport_error_maps_to_divider_call_failed(self) -> None:
        proposal = self._proposal_for(
            {PROFILE_BATCH_DIVIDER_RESPONSE_ERROR_KEY: "OpenAI-compatible request failed: timeout"}
        )
        audit = proposal["fallback_audit"]
        self.assertEqual(audit["fallback_reason"], FALLBACK_REASON_CALL_FAILED)
        self.assertIsNone(audit["provenance"])
        normalize_fallback_audit(audit)

    def test_f4_non_json_output_maps_to_divider_invalid_output_with_provenance(self) -> None:
        proposal = self._proposal_for(
            {
                PROFILE_BATCH_DIVIDER_RESPONSE_DIVISION_KEY: {},
                PROFILE_BATCH_DIVIDER_RESPONSE_PROVENANCE_KEY: _valid_provenance(),
                PROFILE_BATCH_DIVIDER_RESPONSE_ERROR_KEY: "",
                "raw_response_preview": "not json at all",
            }
        )
        audit = proposal["fallback_audit"]
        self.assertEqual(audit["fallback_reason"], FALLBACK_REASON_INVALID_OUTPUT)
        self.assertIn("no JSON object", audit["divider_error"])
        self.assertEqual(audit["provenance"], _valid_provenance())
        normalize_fallback_audit(audit)

    def test_f4_wire_shape_violation_rejects_extra_top_level_keys(self) -> None:
        response = _stub_success_response(_contiguous_batches(400, 4))
        response[PROFILE_BATCH_DIVIDER_RESPONSE_DIVISION_KEY] = {
            "batches": _contiguous_batches(400, 4),
            "notes": "chatty model",
        }
        proposal = self._proposal_for(response)
        audit = proposal["fallback_audit"]
        self.assertEqual(audit["fallback_reason"], FALLBACK_REASON_INVALID_OUTPUT)
        self.assertIn("notes", audit["divider_error"])

    def test_f4_schema_violation_reports_the_schema_validator_id(self) -> None:
        batches = _contiguous_batches(400, 4)
        batches[0]["member_count"] = 999  # count/range mismatch → strict-parse F4
        proposal = self._proposal_for(_stub_success_response(batches))
        audit = proposal["fallback_audit"]
        self.assertEqual(audit["fallback_reason"], FALLBACK_REASON_INVALID_OUTPUT)
        self.assertIn("member_count", audit["divider_error"])
        normalize_fallback_audit(audit)

    def test_f5_battery_rejection_carries_the_validator_id_and_results(self) -> None:
        proposal = self._proposal_for(_stub_success_response(_contiguous_batches(1204, 4)))
        # 4 × 301 members: V1 provider-envelope rejection on a 1204-url set...
        audit = proposal["fallback_audit"]
        self.assertEqual(
            audit["fallback_reason"],
            f"divider_validator_rejected:{VALIDATOR_V1_PROVIDER_ENVELOPE}",
        )
        ran = {entry["validator"]: entry["status"] for entry in audit["validator_results"]}
        self.assertEqual(ran[VALIDATOR_V1_PROVIDER_ENVELOPE], "fail")
        self.assertEqual(audit["provenance"], _valid_provenance())
        normalize_fallback_audit(audit)

    def test_f5_uses_an_inventory_matching_the_batches(self) -> None:
        # Companion pin for the test above: same shape over the right-sized
        # inventory is V1-only — the partition itself is exact.
        proposal = propose_and_validate_division(
            _StubDividerClient(_stub_success_response(_contiguous_batches(1204, 4))),
            inventory=_inventory(1204),
        )
        failures = {
            entry["validator"] for entry in proposal["fallback_audit"]["validator_results"] if entry["status"] == "fail"
        }
        self.assertEqual(failures, {VALIDATOR_V1_PROVIDER_ENVELOPE})

    def test_malformed_client_provenance_is_dropped_with_an_audit_note(self) -> None:
        response = _stub_success_response(_contiguous_batches(1204, 4))
        response[PROFILE_BATCH_DIVIDER_RESPONSE_PROVENANCE_KEY] = {"bogus": True}
        proposal = propose_and_validate_division(_StubDividerClient(response), inventory=_inventory(1204))
        audit = proposal["fallback_audit"]
        self.assertIsNone(audit["provenance"])
        self.assertIn("provenance dropped", audit["divider_error"])

    def test_f6_stale_input_audit_builder_matches_the_design_shape(self) -> None:
        audit = stale_input_fallback_audit(
            recorded_membership_sha256="c" * 64,
            current_membership_sha256="d" * 64,
            provenance=_valid_provenance(),
        )
        self.assertEqual(audit["fallback_reason"], FALLBACK_REASON_INPUT_STALE)
        self.assertEqual(audit["division_source"], DIVISION_SOURCE_RULE_LADDER_FALLBACK)
        self.assertIn("c" * 64, audit["divider_error"])
        normalize_fallback_audit(audit)

    def test_build_fallback_audit_rejects_non_ruling_reasons(self) -> None:
        with self.assertRaises(Exception):
            build_fallback_audit("not_a_ruling_reason")


class InputPayloadContractTest(unittest.TestCase):
    """OQ1 + design §2.2: full input, index ranges + digests, never URL echo."""

    def test_payload_never_echoes_url_keys(self) -> None:
        inventory = [
            {"url_key": f"linkedin.com/in/very-secret-{index}", "queue_state": "ready", "source_shards": ["s1"]}
            for index in range(400)
        ]
        payload = build_divider_input_payload(
            inventory=inventory,
            division_id="div-1",
            retry_wait_indices=[7],
            actor_global_inflight=4,
        )
        serialized = json.dumps(payload)
        self.assertNotIn("linkedin.com", serialized)
        self.assertNotIn("very-secret", serialized)
        self.assertEqual(payload["inventory"]["size"], 400)
        self.assertEqual(len(payload["inventory"]["ordering_digest_sha256"]), 64)
        self.assertEqual(payload["retry_wait"], {"count": 1, "indices": [7]})
        self.assertEqual(payload["budget"]["provider_envelope_max_urls"], 300)
        self.assertEqual(payload["budget"]["ai_batch_count_band"], [4, 8])
        self.assertEqual(payload["budget"]["actor_global_inflight"], 4)
        self.assertIn("retry_isolation", payload["budget"]["legal_tiny_reason_codes"])

    def test_payload_groups_collapse_contiguous_state_shard_failure_runs(self) -> None:
        inventory: list[dict[str, Any]] = []
        for index in range(350):
            inventory.append(
                {
                    "url_key": f"k{index}",
                    "queue_state": "ready" if index < 200 else "retry_wait",
                    "source_shards": ["roster"] if index < 200 else ["search"],
                    "last_failure_class": "" if index < 200 else "backpressure",
                }
            )
        payload = build_divider_input_payload(
            inventory=inventory, division_id="div-2", retry_wait_indices=(), actor_global_inflight=4
        )
        groups = payload["inventory"]["groups"]
        self.assertEqual(len(groups), 2)
        self.assertEqual(groups[0]["index_range"], [0, 199])
        self.assertEqual(groups[0]["count"], 200)
        self.assertEqual(groups[1]["index_range"], [200, 349])
        self.assertEqual(groups[1]["last_failure_class"], "backpressure")

    def test_failure_history_and_prior_round_context_pass_through(self) -> None:
        payload = build_divider_input_payload(
            inventory=_inventory(400),
            division_id="div-3",
            retry_wait_indices=(),
            actor_global_inflight=4,
            failure_history={"backpressure": 12},
            prior_round_context={"deferred_count": 600},
        )
        self.assertEqual(payload["failure_history"], {"backpressure": 12})
        self.assertEqual(payload["prior_round_context"], {"deferred_count": 600})

    def test_system_prompt_instructs_index_range_output_and_forbids_url_echo(self) -> None:
        prompt = _build_profile_batch_division_system_prompt()
        self.assertIn("member_index_ranges", prompt)
        self.assertIn("NEVER echo URLs", prompt)
        self.assertIn("4 to 8", prompt)
        self.assertIn("retry_wait", prompt)
        self.assertIn("ai_division", prompt)
        self.assertNotIn("http", prompt.lower().replace("https", ""))


class _ChatResponse:
    def __init__(self, payload: dict[str, Any]) -> None:
        self.payload = payload

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict[str, Any]:
        return self.payload


class OpenAICompatibleDividerTest(unittest.TestCase):
    """OQ8 timeout + raw-output contract + circuit conventions."""

    def setUp(self) -> None:
        _reset_model_provider_circuits_for_tests()
        self.settings = ModelProviderSettings(
            enabled=True,
            provider_name="divider_test_provider",
            api_key="sk-synthetic",
            base_url="https://divider-test.invalid/v1",
            model="gpt-divider-test",
            timeout_seconds=45,
        )
        self.client = OpenAICompatibleChatModelClient(self.settings)

    def tearDown(self) -> None:
        _reset_model_provider_circuits_for_tests()

    def _chat_body(self, content: str) -> dict[str, Any]:
        return {
            "model": self.settings.model,
            "choices": [{"message": {"content": content}}],
            "usage": {"prompt_tokens": 111, "completion_tokens": 22},
        }

    def test_divider_call_uses_the_scoped_20s_timeout_not_the_settings_45s(self) -> None:
        batches = _contiguous_batches(400, 4)
        response = _ChatResponse(self._chat_body(json.dumps({"batches": batches})))
        with patch.object(model_provider_module.requests, "post", return_value=response) as post:
            result = self.client.divide_profile_prefetch_batches({"inventory": {"size": 400}})
        post.assert_called_once()
        self.assertEqual(post.call_args.kwargs["timeout"], 20)
        system_content = post.call_args.kwargs["json"]["messages"][0]["content"]
        self.assertIn("member_index_ranges", system_content)
        self.assertIn("NEVER echo URLs", system_content)
        self.assertEqual(result[PROFILE_BATCH_DIVIDER_RESPONSE_ERROR_KEY], "")
        self.assertEqual(result[PROFILE_BATCH_DIVIDER_RESPONSE_DIVISION_KEY], {"batches": batches})

    def test_non_divider_calls_keep_the_settings_timeout(self) -> None:
        response = _ChatResponse(self._chat_body(json.dumps({"patch": {}})))
        with patch.object(model_provider_module.requests, "post", return_value=response) as post:
            self.client.normalize_review_instruction({})
        self.assertEqual(post.call_args.kwargs["timeout"], 45)

    def test_raw_payload_is_returned_unvalidated_with_client_side_provenance(self) -> None:
        # A V1-violating division must pass through raw: validation is
        # single-sourced in the S1 contract at the caller (design §2.3).
        oversized = _contiguous_batches(2000, 4)
        response = _ChatResponse(self._chat_body(json.dumps({"batches": oversized})))
        with patch.object(model_provider_module.requests, "post", return_value=response):
            result = self.client.divide_profile_prefetch_batches({"inventory": {"size": 2000}})
        self.assertEqual(result[PROFILE_BATCH_DIVIDER_RESPONSE_DIVISION_KEY], {"batches": oversized})
        provenance = result[PROFILE_BATCH_DIVIDER_RESPONSE_PROVENANCE_KEY]
        self.assertEqual(provenance["model_provider"], "divider_test_provider")
        self.assertEqual(provenance["requested_model"], "gpt-divider-test")
        self.assertEqual(provenance["response_model"], "gpt-divider-test")
        self.assertRegex(provenance["prompt_sha256"], r"^[0-9a-f]{64}$")
        self.assertRegex(provenance["input_snapshot_sha256"], r"^[0-9a-f]{64}$")
        self.assertEqual(provenance["usage"], {"input_tokens": 111, "output_tokens": 22})
        self.assertGreaterEqual(provenance["latency_ms"], 0)

    def test_transport_failure_returns_the_truncated_error_string_and_maps_to_f3(self) -> None:
        with patch.object(
            model_provider_module.requests,
            "post",
            side_effect=requests.ConnectionError("connection refused"),
        ):
            result = self.client.divide_profile_prefetch_batches({"inventory": {"size": 400}})
            self.assertIn("request failed", result[PROFILE_BATCH_DIVIDER_RESPONSE_ERROR_KEY])

            # The first failure opened the shared circuit (F2 territory);
            # reset so this pin isolates the raw transport-failure F3 mapping.
            _reset_model_provider_circuits_for_tests()
            proposal = propose_and_validate_division(self.client, inventory=_inventory(400))
        self.assertEqual(
            proposal["fallback_audit"]["fallback_reason"],
            FALLBACK_REASON_CALL_FAILED,
        )

    def test_open_circuit_short_circuits_before_transport_and_maps_to_f2(self) -> None:
        key = _model_provider_circuit_key(self.settings.provider_name, self.settings.base_url, self.settings.model)
        _record_model_provider_failure(key, "previous divider failure")
        with patch.object(model_provider_module.requests, "post") as post:
            result = self.client.divide_profile_prefetch_batches({"inventory": {"size": 400}})
            post.assert_not_called()
            self.assertTrue(
                result[PROFILE_BATCH_DIVIDER_RESPONSE_ERROR_KEY].startswith(PROFILE_BATCH_DIVIDER_CIRCUIT_ERROR_PREFIX)
            )
            proposal = propose_and_validate_division(self.client, inventory=_inventory(400))
        self.assertEqual(
            proposal["fallback_audit"]["fallback_reason"],
            FALLBACK_REASON_CIRCUIT_OPEN,
        )

    def test_model_identity_mismatch_surfaces_as_an_error_not_a_division(self) -> None:
        body = self._chat_body(json.dumps({"batches": []}))
        body["model"] = "some-rerouted-model"
        with patch.object(model_provider_module.requests, "post", return_value=_ChatResponse(body)):
            result = self.client.divide_profile_prefetch_batches({"inventory": {"size": 400}})
        self.assertIn("model_response_identity_mismatch", result[PROFILE_BATCH_DIVIDER_RESPONSE_ERROR_KEY])


class QwenDividerTest(unittest.TestCase):
    """Design §2.1: Qwen gets the same method; D6 — no circuit, F2 never arises."""

    def test_qwen_divider_uses_scoped_timeout_and_returns_raw_output(self) -> None:
        batches = _contiguous_batches(400, 4)
        captured: dict[str, Any] = {}

        class _UrlOpenResponse:
            def __enter__(self) -> "_UrlOpenResponse":
                return self

            def __exit__(self, *_args: object) -> None:
                return None

            def read(self) -> bytes:
                return json.dumps({"output_text": json.dumps({"batches": batches})}).encode("utf-8")

        def _fake_urlopen(request_obj: Any, timeout: int | None = None) -> "_UrlOpenResponse":
            captured["timeout"] = timeout
            return _UrlOpenResponse()

        client = QwenResponsesModelClient(QwenSettings(enabled=True, api_key="sk-qwen"))
        with patch.object(model_provider_module.request, "urlopen", _fake_urlopen):
            result = client.divide_profile_prefetch_batches({"inventory": {"size": 400}})
        self.assertEqual(captured["timeout"], 20)
        self.assertEqual(result[PROFILE_BATCH_DIVIDER_RESPONSE_DIVISION_KEY], {"batches": batches})
        provenance = result[PROFILE_BATCH_DIVIDER_RESPONSE_PROVENANCE_KEY]
        self.assertEqual(provenance["model_provider"], "qwen")
        self.assertEqual(provenance["response_model"], "")

    def test_qwen_transport_failure_maps_to_f3_only(self) -> None:
        client = QwenResponsesModelClient(QwenSettings(enabled=True, api_key="sk-qwen"))
        with patch.object(model_provider_module.request, "urlopen", side_effect=OSError("connection reset")):
            proposal = propose_and_validate_division(client, inventory=_inventory(400))
        self.assertEqual(
            proposal["fallback_audit"]["fallback_reason"],
            FALLBACK_REASON_CALL_FAILED,
        )


if __name__ == "__main__":
    unittest.main()
