"""WS7/W7.3 S2 offline suite for the promote-judge model-invocation surface.

Provenance: docs/WS7_AI_PROMOTE_DESIGN.md §3 (model invocation surface) + §4
(ruling-④ failure classes F1-F6 → keep incumbent), OQ4/OQ5/OQ8 RATIFIED
2026-07-24 (slice S2 per §7/§9, additive only — no flip, no promote-path /
asset_reuse_planning / storage changes). Pins: the
`judge_organization_asset_promotion` ModelClient method (Deterministic → {} =
F1 keep-incumbent marker; scripted judge client behind
SOURCING_SCRIPTED_ORGANIZATION_PROMOTE_JUDGE per OQ8; OpenAI-compatible/Qwen
raw-output + judge-scoped 20 s timeout per OQ8), the judge_and_validate_promotion
helper's OQ4 contested-only gate + once-per-decision call discipline, and the
mapping of every failure/reject class to the ruling-④ keep-incumbent audit shapes
via the S1 contract (organization_promote_contract — validation single-sourced
there; companion suite tests/test_organization_promote_contract.py). Offline: the
guard verdict + incumbent arrive as parameters; no PG / model / network.
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
    ORGANIZATION_PROMOTE_JUDGE_CIRCUIT_ERROR_PREFIX,
    ORGANIZATION_PROMOTE_JUDGE_RESPONSE_ERROR_KEY,
    ORGANIZATION_PROMOTE_JUDGE_RESPONSE_JUDGMENT_KEY,
    ORGANIZATION_PROMOTE_JUDGE_RESPONSE_PROVENANCE_KEY,
    DeterministicModelClient,
    OfflineModelClient,
    OpenAICompatibleChatModelClient,
    QwenResponsesModelClient,
    ScriptedLivePlanningModelClient,
    ScriptedOrganizationPromoteJudgeModelClient,
    _build_organization_promote_judge_system_prompt,
    _model_provider_circuit_key,
    _record_model_provider_failure,
    _reset_model_provider_circuits_for_tests,
    build_model_client,
)
from sourcing_agent.organization_promote_contract import (
    SCHEMA_ID_V1,
    VALIDATOR_V_LINEAGE,
    normalize_ai_promote_decision,
    normalize_fallback_audit,
    validate_ai_promote_decision,
)
from sourcing_agent.organization_promote_judgment import (
    STATUS_KEPT_INCUMBENT,
    STATUS_PROMOTED,
    build_promote_judge_input_payload,
    judge_and_validate_promotion,
)
from sourcing_agent.settings import ModelProviderSettings, QwenSettings

_SCRIPTED_JUDGE_ENV = {"SOURCING_SCRIPTED_ORGANIZATION_PROMOTE_JUDGE": "1"}
_HEX = "ab" * 32


def _metrics(**overrides: Any) -> dict[str, Any]:
    metrics = {
        "candidate_count": 200,
        "evidence_count": 200,
        "profile_detail_count": 200,
        "missing_linkedin_count": 0,
        "profile_completion_backlog_count": 0,
        "effective_lane_total": 200,
    }
    metrics.update(overrides)
    return metrics


def _descriptor(
    snapshot_id: str,
    *,
    completeness_score: float = 80.0,
    selected: list[str] | None = None,
    generation_sequence: int = 6,
    generation_key: str = "lineage-a",
    lifecycle_status: str = "ready",
    metrics: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "snapshot_id": snapshot_id,
        "metrics": _metrics(**(metrics or {})),
        "completeness_score": completeness_score,
        "completeness_band": "high",
        "selected_snapshot_ids": list(selected if selected is not None else [snapshot_id]),
        "source_snapshot_count": 3,
        "materialization_generation_sequence": generation_sequence,
        "lifecycle_status": lifecycle_status,
        "materialization_generation_key": generation_key,
        "explicit_baseline_inclusion": False,
    }


def _shard(**overrides: Any) -> dict[str, Any]:
    shard = {
        "shard_id": "shard-1",
        "lane": "current",
        "search_query": "staff engineer",
        "locations": ["US"],
        "function_ids": ["8", "9"],
        "job_titles": ["Staff Engineer"],
        "seniority": ["senior"],
        "query_family": "eng-core",
        "result_count": 304,
        "estimated_total_count": 0,
        "provider_cap_hit": False,
        "payload_snapshot_sha256": None,
    }
    shard.update(overrides)
    return shard


def _incumbent() -> dict[str, Any]:
    return _descriptor("inc", generation_sequence=6)


def _candidate_descriptor(
    *,
    candidate: dict[str, Any] | None = None,
    simulate_or_placeholder: bool = False,
) -> dict[str, Any]:
    return {
        "incumbent": _incumbent(),
        "candidate": candidate
        if candidate is not None
        else _descriptor(
            "cand",
            completeness_score=82.0,
            selected=["cand"],
            generation_sequence=7,
            metrics={"candidate_count": 260, "profile_detail_count": 260, "effective_lane_total": 260},
        ),
        "coverage_evidence": {
            "shards": [_shard()],
            "request_population_match": {
                "candidate_covers_incumbent_shards": True,
                "new_shards": [],
                "dropped_shards": [],
            },
        },
        "prior_snapshot_comparison": {
            "candidate_sort_key": [82.0, 260],
            "incumbent_sort_key": [80.0, 200],
            "simulate_or_placeholder_provenance": simulate_or_placeholder,
        },
    }


def _passing_guard_verdict() -> dict[str, Any]:
    return {"refused": False, "reason": ""}


def _refusing_guard_verdict() -> dict[str, Any]:
    return {
        "refused": True,
        "reason": "source_snapshot_coverage_regression",
        "incoming_snapshot_id": "cand",
        "incoming_generation_key": "lineage-a",
        "incoming_generation_sequence": 3,
        "incoming_selected_snapshot_ids": ["cand"],
        "blocking_snapshot_id": "inc",
        "blocking_generation_key": "lineage-a",
        "blocking_generation_sequence": 6,
        "blocking_selected_snapshot_ids": ["cand", "inc"],
    }


def _valid_provenance() -> dict[str, Any]:
    return {
        "model_provider": "stub_provider",
        "requested_model": "stub-model",
        "response_model": "stub-model",
        "prompt_sha256": _HEX,
        "input_snapshot_sha256": _HEX,
        "usage": {"input_tokens": 10, "output_tokens": 5},
        "latency_ms": 12,
    }


def _judgment_response(
    decision: str,
    reason_code: str,
    *,
    reason: str = "candidate coverage is a superset with no completeness regression",
    provenance: dict[str, Any] | None = None,
    error: str = "",
) -> dict[str, Any]:
    if error:
        return {ORGANIZATION_PROMOTE_JUDGE_RESPONSE_ERROR_KEY: error}
    return {
        ORGANIZATION_PROMOTE_JUDGE_RESPONSE_JUDGMENT_KEY: {
            "decision": decision,
            "reason": reason,
            "reason_code": reason_code,
        },
        ORGANIZATION_PROMOTE_JUDGE_RESPONSE_PROVENANCE_KEY: provenance
        if provenance is not None
        else _valid_provenance(),
        ORGANIZATION_PROMOTE_JUDGE_RESPONSE_ERROR_KEY: "",
        "raw_response_preview": "",
    }


class _StubJudgeClient(DeterministicModelClient):
    """Spy client: canned judge response + call counting for OQ4 pins."""

    def __init__(self, response: dict[str, Any]) -> None:
        self.response = response
        self.judge_calls = 0
        self.payloads: list[dict[str, Any]] = []

    def judge_organization_asset_promotion(self, payload: dict[str, Any]) -> dict[str, Any]:
        self.judge_calls += 1
        self.payloads.append(payload)
        return self.response


def _judge(
    client: Any,
    *,
    contested: bool = True,
    guard: dict[str, Any] | None = None,
    candidate_descriptor: dict[str, Any] | None = None,
    incumbent_descriptor: dict[str, Any] | None = None,
    deterministic_promote: bool = False,
) -> dict[str, Any]:
    return judge_and_validate_promotion(
        client,
        candidate_descriptor=candidate_descriptor if candidate_descriptor is not None else _candidate_descriptor(),
        guard_predicted_verdict=guard if guard is not None else _passing_guard_verdict(),
        incumbent_descriptor=incumbent_descriptor if incumbent_descriptor is not None else _incumbent(),
        contested=contested,
        deterministic_promote=deterministic_promote,
    )


class ProtocolConformanceTest(unittest.TestCase):
    """Design §3.1: Deterministic default {} is the structural F1 marker."""

    def test_deterministic_client_returns_empty_judgment_marker(self) -> None:
        self.assertEqual(DeterministicModelClient().judge_organization_asset_promotion({"x": 1}), {})

    def test_offline_client_inherits_the_f1_marker_so_simulate_is_keep_incumbent(self) -> None:
        for mode in ("simulate", "replay", "scripted"):
            self.assertEqual(OfflineModelClient(mode=mode).judge_organization_asset_promotion({}), {})

    def test_scripted_live_planning_client_does_not_gain_the_judge(self) -> None:
        client = ScriptedLivePlanningModelClient(DeterministicModelClient(), mode="scripted")
        self.assertEqual(client.judge_organization_asset_promotion({}), {})

    def test_deterministic_marker_maps_to_f1_keep_incumbent_in_the_helper(self) -> None:
        result = _judge(DeterministicModelClient())
        self.assertEqual(result["status"], STATUS_KEPT_INCUMBENT)
        self.assertTrue(result["engaged"])
        self.assertIsNone(result["decision"])
        audit = result["audit"]
        self.assertEqual(audit["fallback_reason"], "judge_model_unavailable")
        self.assertIsNone(audit["provenance"])
        normalize_fallback_audit(audit)  # F1 shape round-trips the S1 contract


class ScriptedPromoteJudgeClientTest(unittest.TestCase):
    """OQ8: opt-in env gate + schema-valid offline verdicts."""

    def test_env_absent_returns_the_f1_marker_even_when_directly_constructed(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            client = ScriptedOrganizationPromoteJudgeModelClient(mode="simulate")
            self.assertEqual(client.judge_organization_asset_promotion({"candidate_descriptor": {}}), {})

    def test_env_set_promote_verdict_passes_the_full_s1_battery(self) -> None:
        with patch.dict(os.environ, _SCRIPTED_JUDGE_ENV, clear=False):
            client = ScriptedOrganizationPromoteJudgeModelClient(mode="simulate")
            result = _judge(client)
        self.assertEqual(result["status"], STATUS_PROMOTED)
        decision = result["decision"]
        self.assertEqual(decision["schema_id"], SCHEMA_ID_V1)
        self.assertEqual(decision["decision"], "promote")
        self.assertEqual(decision["decision_id"], result["decision_id"])
        # Re-validate the recorded payload end-to-end through the S1 battery.
        revalidated = validate_ai_promote_decision(
            {**decision, "validator_results": []},
            guard_predicted_verdict=_passing_guard_verdict(),
            incumbent_descriptor=_incumbent(),
        )
        self.assertTrue(revalidated["valid"], revalidated["failures"])
        self.assertEqual(revalidated["effective_decision"], "promote")
        # And the normalizer round-trips the recorded record.
        self.assertEqual(
            normalize_ai_promote_decision({**decision, "validator_results": []}).to_payload(),
            {**decision, "validator_results": []},
        )

    def test_scripted_reject_on_simulate_provenance_keeps_incumbent(self) -> None:
        with patch.dict(os.environ, _SCRIPTED_JUDGE_ENV, clear=False):
            client = ScriptedOrganizationPromoteJudgeModelClient(mode="simulate")
            result = _judge(client, candidate_descriptor=_candidate_descriptor(simulate_or_placeholder=True))
        self.assertEqual(result["status"], STATUS_KEPT_INCUMBENT)
        # An honest AI reject is a valid recorded decision, not an F-audit.
        self.assertIsNone(result["audit"])
        self.assertEqual(result["decision"]["decision"], "reject")
        self.assertEqual(result["decision"]["reason_code"], "ai_provenance_suspect")

    def test_build_model_client_wires_the_scripted_judge_only_with_the_opt_in(self) -> None:
        with patch.dict(os.environ, {"SOURCING_EXTERNAL_PROVIDER_MODE": "simulate"}, clear=True):
            self.assertIsInstance(build_model_client(None, None), OfflineModelClient)
            self.assertNotIsInstance(build_model_client(None, None), ScriptedOrganizationPromoteJudgeModelClient)
        with patch.dict(
            os.environ,
            {"SOURCING_EXTERNAL_PROVIDER_MODE": "simulate", **_SCRIPTED_JUDGE_ENV},
            clear=True,
        ):
            self.assertIsInstance(build_model_client(None, None), ScriptedOrganizationPromoteJudgeModelClient)

    def test_scripted_divider_opt_in_takes_precedence_over_the_judge_opt_in(self) -> None:
        # Documented precedence (build_model_client): the two opt-ins are not
        # composable in one client yet; the divider wins and the judge falls back
        # per ruling ④ (its default {} keeps the incumbent — a safe state).
        with patch.dict(
            os.environ,
            {
                "SOURCING_EXTERNAL_PROVIDER_MODE": "simulate",
                "SOURCING_SCRIPTED_PROFILE_BATCH_DIVIDER": "1",
                **_SCRIPTED_JUDGE_ENV,
            },
            clear=True,
        ):
            client = build_model_client(None, None)
        self.assertEqual(client.judge_organization_asset_promotion({"candidate_descriptor": {}}), {})


class HelperContestedGatingTest(unittest.TestCase):
    """OQ4: engage only on contested decisions; pre-branches never call the model."""

    def test_non_contested_keep_incumbent_without_a_model_call(self) -> None:
        client = _StubJudgeClient(_judgment_response("promote", "ai_coverage_superset"))
        result = _judge(client, contested=False, deterministic_promote=False)
        self.assertEqual(client.judge_calls, 0)
        self.assertEqual(result["status"], STATUS_KEPT_INCUMBENT)
        self.assertFalse(result["engaged"])
        self.assertIsNone(result["decision"])
        self.assertIsNone(result["audit"])

    def test_non_contested_deterministic_promote_without_a_model_call(self) -> None:
        client = _StubJudgeClient(_judgment_response("reject", "ai_coverage_not_superset"))
        result = _judge(client, contested=False, deterministic_promote=True)
        self.assertEqual(client.judge_calls, 0)
        self.assertEqual(result["status"], STATUS_PROMOTED)
        self.assertFalse(result["engaged"])

    def test_contested_calls_the_model_exactly_once(self) -> None:
        client = _StubJudgeClient(_judgment_response("promote", "ai_coverage_superset"))
        result = _judge(client, contested=True)
        self.assertEqual(client.judge_calls, 1)
        self.assertEqual(result["status"], STATUS_PROMOTED)
        self.assertTrue(result["engaged"])

    def test_input_payload_never_carries_the_guard_verdict(self) -> None:
        client = _StubJudgeClient(_judgment_response("promote", "ai_coverage_superset"))
        _judge(client, contested=True, guard=_refusing_guard_verdict())
        payload = client.payloads[0]
        serialized = json.dumps(payload)
        self.assertNotIn("refused", serialized)
        self.assertNotIn("guard", serialized.lower())
        self.assertIn("candidate_descriptor", payload)
        self.assertEqual(payload["output_contract_schema_id"], SCHEMA_ID_V1)


class HelperFallbackMappingTest(unittest.TestCase):
    """Design §4: every failure/reject class → keep incumbent + pinned audit shape."""

    def test_f2_circuit_open_maps_to_judge_circuit_open(self) -> None:
        client = _StubJudgeClient(
            _judgment_response(
                "promote",
                "ai_coverage_superset",
                error=f"{ORGANIZATION_PROMOTE_JUDGE_CIRCUIT_ERROR_PREFIX}:874s_remaining:boom",
            )
        )
        result = _judge(client)
        self.assertEqual(result["status"], STATUS_KEPT_INCUMBENT)
        audit = result["audit"]
        self.assertEqual(audit["fallback_reason"], "judge_circuit_open")
        self.assertIn("874s_remaining", audit["judge_error"])
        self.assertIsNone(audit["provenance"])
        normalize_fallback_audit(audit)

    def test_f3_transport_error_maps_to_judge_call_failed(self) -> None:
        client = _StubJudgeClient(_judgment_response("promote", "x", error="Qwen request failed: timeout"))
        result = _judge(client)
        audit = result["audit"]
        self.assertEqual(audit["fallback_reason"], "judge_call_failed")
        self.assertIsNone(audit["provenance"])
        normalize_fallback_audit(audit)

    def test_f4_non_json_output_maps_to_judge_invalid_output_with_provenance(self) -> None:
        client = _StubJudgeClient(
            {
                ORGANIZATION_PROMOTE_JUDGE_RESPONSE_JUDGMENT_KEY: {},
                ORGANIZATION_PROMOTE_JUDGE_RESPONSE_PROVENANCE_KEY: _valid_provenance(),
                ORGANIZATION_PROMOTE_JUDGE_RESPONSE_ERROR_KEY: "",
                "raw_response_preview": "not json at all",
            }
        )
        result = _judge(client)
        audit = result["audit"]
        self.assertEqual(audit["fallback_reason"], "judge_invalid_output")
        self.assertIn("no JSON object", audit["judge_error"])
        self.assertEqual(audit["provenance"], _valid_provenance())
        normalize_fallback_audit(audit)

    def test_f4_wire_shape_violation_rejects_extra_keys(self) -> None:
        response = _judgment_response("promote", "ai_coverage_superset")
        response[ORGANIZATION_PROMOTE_JUDGE_RESPONSE_JUDGMENT_KEY]["confidence"] = 0.9
        result = _judge(_StubJudgeClient(response))
        audit = result["audit"]
        self.assertEqual(audit["fallback_reason"], "judge_invalid_output")
        self.assertIn("confidence", audit["judge_error"])

    def test_f4_schema_violation_reports_invalid_output(self) -> None:
        # decision value the model must never emit → S1 strict-parse F4.
        result = _judge(_StubJudgeClient(_judgment_response("maybe", "ai_coverage_superset")))
        audit = result["audit"]
        self.assertEqual(audit["fallback_reason"], "judge_invalid_output")
        self.assertIn("decision", audit["judge_error"])
        normalize_fallback_audit(audit)

    def test_f5_guard_predicted_refuse_maps_to_v_lineage_keep_incumbent(self) -> None:
        # The model says promote but the guard would refuse → V_LINEAGE rejects;
        # the AI can never override a guard refusal (ruling ③).
        result = _judge(
            _StubJudgeClient(_judgment_response("promote", "ai_coverage_superset")),
            guard=_refusing_guard_verdict(),
        )
        self.assertEqual(result["status"], STATUS_KEPT_INCUMBENT)
        self.assertIsNone(result["decision"])
        audit = result["audit"]
        self.assertEqual(audit["fallback_reason"], f"judge_validator_rejected:{VALIDATOR_V_LINEAGE}")
        statuses = {entry["validator"]: entry["status"] for entry in audit["validator_results"]}
        self.assertEqual(statuses[VALIDATOR_V_LINEAGE], "fail")
        self.assertEqual(audit["provenance"], _valid_provenance())
        normalize_fallback_audit(audit)

    def test_f5_completeness_regression_maps_to_v_comp_keep_incumbent(self) -> None:
        # The model approves a strictly narrower candidate → V_COMP rejects.
        narrower = _descriptor("cand", selected=["cand"], generation_sequence=7, metrics={"effective_lane_total": 150})
        result = _judge(
            _StubJudgeClient(_judgment_response("promote", "ai_coverage_superset")),
            candidate_descriptor=_candidate_descriptor(candidate=narrower),
        )
        self.assertEqual(result["status"], STATUS_KEPT_INCUMBENT)
        self.assertTrue(result["audit"]["fallback_reason"].startswith("judge_validator_rejected:V_COMP"))

    def test_honest_ai_reject_keeps_incumbent_with_the_decision_recorded(self) -> None:
        result = _judge(_StubJudgeClient(_judgment_response("reject", "ai_coverage_not_superset")))
        self.assertEqual(result["status"], STATUS_KEPT_INCUMBENT)
        self.assertIsNone(result["audit"])
        self.assertEqual(result["decision"]["decision"], "reject")
        self.assertEqual(result["decision"]["reason_code"], "ai_coverage_not_superset")

    def test_malformed_client_provenance_is_dropped_with_an_audit_note(self) -> None:
        # A structurally-invalid client provenance must never make the audit fail
        # to record: it is dropped to null with a note (mirror of divider F4).
        client = _StubJudgeClient(
            {
                ORGANIZATION_PROMOTE_JUDGE_RESPONSE_JUDGMENT_KEY: {},
                ORGANIZATION_PROMOTE_JUDGE_RESPONSE_PROVENANCE_KEY: {"bogus": True},
                ORGANIZATION_PROMOTE_JUDGE_RESPONSE_ERROR_KEY: "",
                "raw_response_preview": "garbage",
            }
        )
        dropped = _judge(client)
        self.assertIsNone(dropped["audit"]["provenance"])
        self.assertIn("provenance dropped", dropped["audit"]["judge_error"])


class _ChatResponse:
    def __init__(self, payload: dict[str, Any]) -> None:
        self.payload = payload

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict[str, Any]:
        return self.payload


class OpenAICompatiblePromoteJudgeTest(unittest.TestCase):
    """OQ8 timeout + raw-output contract + circuit conventions."""

    def setUp(self) -> None:
        _reset_model_provider_circuits_for_tests()
        self.settings = ModelProviderSettings(
            enabled=True,
            provider_name="judge_test_provider",
            api_key="sk-synthetic",
            base_url="https://judge-test.invalid/v1",
            model="gpt-judge-test",
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

    def _judgment(self) -> dict[str, Any]:
        return {"decision": "promote", "reason": "superset coverage", "reason_code": "ai_coverage_superset"}

    def test_judge_call_uses_the_scoped_20s_timeout_not_the_settings_45s(self) -> None:
        response = _ChatResponse(self._chat_body(json.dumps(self._judgment())))
        with patch.object(model_provider_module.requests, "post", return_value=response) as post:
            result = self.client.judge_organization_asset_promotion({"candidate_descriptor": {}})
        post.assert_called_once()
        self.assertEqual(post.call_args.kwargs["timeout"], 20)
        system_content = post.call_args.kwargs["json"]["messages"][0]["content"]
        self.assertIn("reason_code", system_content)
        self.assertIn("promote", system_content)
        self.assertEqual(result[ORGANIZATION_PROMOTE_JUDGE_RESPONSE_ERROR_KEY], "")
        self.assertEqual(result[ORGANIZATION_PROMOTE_JUDGE_RESPONSE_JUDGMENT_KEY], self._judgment())

    def test_non_judge_calls_keep_the_settings_timeout(self) -> None:
        response = _ChatResponse(self._chat_body(json.dumps({"patch": {}})))
        with patch.object(model_provider_module.requests, "post", return_value=response) as post:
            self.client.normalize_review_instruction({})
        self.assertEqual(post.call_args.kwargs["timeout"], 45)

    def test_raw_payload_is_returned_unvalidated_with_client_side_provenance(self) -> None:
        # A schema-violating verdict must pass through raw: validation is
        # single-sourced in the S1 contract at the caller (design §3.3).
        bad = {"decision": "maybe", "reason": "", "reason_code": "nope", "extra": 1}
        response = _ChatResponse(self._chat_body(json.dumps(bad)))
        with patch.object(model_provider_module.requests, "post", return_value=response):
            result = self.client.judge_organization_asset_promotion({"candidate_descriptor": {}})
        self.assertEqual(result[ORGANIZATION_PROMOTE_JUDGE_RESPONSE_JUDGMENT_KEY], bad)
        provenance = result[ORGANIZATION_PROMOTE_JUDGE_RESPONSE_PROVENANCE_KEY]
        self.assertEqual(provenance["model_provider"], "judge_test_provider")
        self.assertEqual(provenance["requested_model"], "gpt-judge-test")
        self.assertEqual(provenance["response_model"], "gpt-judge-test")
        self.assertRegex(provenance["prompt_sha256"], r"^[0-9a-f]{64}$")
        self.assertRegex(provenance["input_snapshot_sha256"], r"^[0-9a-f]{64}$")
        self.assertEqual(provenance["usage"], {"input_tokens": 111, "output_tokens": 22})

    def test_transport_failure_returns_error_and_maps_to_f3(self) -> None:
        with patch.object(
            model_provider_module.requests,
            "post",
            side_effect=requests.ConnectionError("connection refused"),
        ):
            result = self.client.judge_organization_asset_promotion({"candidate_descriptor": {}})
            self.assertIn("request failed", result[ORGANIZATION_PROMOTE_JUDGE_RESPONSE_ERROR_KEY])
            _reset_model_provider_circuits_for_tests()
            outcome = _judge(self.client)
        self.assertEqual(outcome["status"], STATUS_KEPT_INCUMBENT)
        self.assertEqual(outcome["audit"]["fallback_reason"], "judge_call_failed")

    def test_open_circuit_short_circuits_before_transport_and_maps_to_f2(self) -> None:
        key = _model_provider_circuit_key(self.settings.provider_name, self.settings.base_url, self.settings.model)
        _record_model_provider_failure(key, "previous judge failure")
        with patch.object(model_provider_module.requests, "post") as post:
            result = self.client.judge_organization_asset_promotion({"candidate_descriptor": {}})
            post.assert_not_called()
            self.assertTrue(
                result[ORGANIZATION_PROMOTE_JUDGE_RESPONSE_ERROR_KEY].startswith(
                    ORGANIZATION_PROMOTE_JUDGE_CIRCUIT_ERROR_PREFIX
                )
            )
            outcome = _judge(self.client)
        self.assertEqual(outcome["audit"]["fallback_reason"], "judge_circuit_open")

    def test_model_identity_mismatch_surfaces_as_an_error_not_a_judgment(self) -> None:
        body = self._chat_body(json.dumps(self._judgment()))
        body["model"] = "some-rerouted-model"
        with patch.object(model_provider_module.requests, "post", return_value=_ChatResponse(body)):
            result = self.client.judge_organization_asset_promotion({"candidate_descriptor": {}})
        self.assertIn("model_response_identity_mismatch", result[ORGANIZATION_PROMOTE_JUDGE_RESPONSE_ERROR_KEY])


class QwenPromoteJudgeTest(unittest.TestCase):
    """Design §3.1: Qwen gets the same method; D6 — no circuit, F2 never arises."""

    def test_qwen_judge_uses_scoped_timeout_and_returns_raw_output(self) -> None:
        judgment = {"decision": "reject", "reason": "not a superset", "reason_code": "ai_coverage_not_superset"}
        captured: dict[str, Any] = {}

        class _UrlOpenResponse:
            def __enter__(self) -> "_UrlOpenResponse":
                return self

            def __exit__(self, *_args: object) -> None:
                return None

            def read(self) -> bytes:
                return json.dumps({"output_text": json.dumps(judgment)}).encode("utf-8")

        def _fake_urlopen(request_obj: Any, timeout: int | None = None) -> "_UrlOpenResponse":
            captured["timeout"] = timeout
            return _UrlOpenResponse()

        client = QwenResponsesModelClient(QwenSettings(enabled=True, api_key="sk-qwen"))
        with patch.object(model_provider_module.request, "urlopen", _fake_urlopen):
            result = client.judge_organization_asset_promotion({"candidate_descriptor": {}})
        self.assertEqual(captured["timeout"], 20)
        self.assertEqual(result[ORGANIZATION_PROMOTE_JUDGE_RESPONSE_JUDGMENT_KEY], judgment)
        provenance = result[ORGANIZATION_PROMOTE_JUDGE_RESPONSE_PROVENANCE_KEY]
        self.assertEqual(provenance["model_provider"], "qwen")
        self.assertEqual(provenance["response_model"], "")

    def test_qwen_transport_failure_maps_to_f3_only(self) -> None:
        client = QwenResponsesModelClient(QwenSettings(enabled=True, api_key="sk-qwen"))
        with patch.object(model_provider_module.request, "urlopen", side_effect=OSError("connection reset")):
            outcome = _judge(client)
        self.assertEqual(outcome["audit"]["fallback_reason"], "judge_call_failed")


class PromptAndInputContractTest(unittest.TestCase):
    """Design §3.2/§3.3: prompt instructs promote|reject + reason_code + one-gate;
    input payload carries the descriptor, never the guard verdict."""

    def test_prompt_instructs_verdict_reason_code_and_one_gate_status(self) -> None:
        prompt = _build_organization_promote_judge_system_prompt()
        self.assertIn("promote", prompt)
        self.assertIn("reject", prompt)
        self.assertIn("reason_code", prompt)
        self.assertIn("decision, reason, reason_code", prompt)
        # One gate among several: cannot force a promote past guard/validators.
        self.assertIn("MORE conservative", prompt)
        self.assertIn("guard", prompt.lower())
        self.assertIn("Do not output markdown", prompt)

    def test_input_payload_builder_carries_descriptor_and_schema_id(self) -> None:
        payload = build_promote_judge_input_payload(
            candidate_descriptor=_candidate_descriptor(),
            decision_id="dec-1",
            contested_context={"note": "two real snapshots compete"},
        )
        self.assertEqual(payload["output_contract_schema_id"], SCHEMA_ID_V1)
        self.assertEqual(payload["decision_id"], "dec-1")
        self.assertIn("candidate", payload["candidate_descriptor"])
        self.assertNotIn("refused", json.dumps(payload))


if __name__ == "__main__":
    unittest.main()
