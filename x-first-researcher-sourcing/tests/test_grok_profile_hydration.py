from __future__ import annotations

import copy
import json
import unittest
from dataclasses import replace
from pathlib import Path
from typing import Any

from tests.grok_raw_session_fixture import (
    fixture_grok_session_precommit,
    raw_grok_session,
)
from x_first.grok_operator_session_replay import GrokOperatorExecutionFacts
from x_first.grok_profile_hydration import (
    CONTRACT_VERSION,
    HYDRATION_FIELDS,
    ProfileHydrationBatchExpectation,
    ProfileHydrationContractError,
    ProfileHydrationIdentityExpectation,
    ProfileHydrationToolCompletion,
    canonical_json_sha256,
    evaluate_profile_hydration_batch,
    profile_identity_input_set_sha256,
    validate_profile_hydration_result,
)
from x_first.grok_profile_hydration import (
    build_profile_hydration_operator_projection as _build_profile_hydration_operator_projection,
)
from x_first.grok_profile_hydration import (
    project_profile_execution_limitations as _project_profile_execution_limitations,
)
from x_first.recall_pool_schema import schema_errors

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCHEMA_PATH = PROJECT_ROOT / "contracts" / "x.grok.profile_hydration.result.v1.schema.json"
TARGET_DIGEST = "1" * 64
PROMPT_DIGEST = "4" * 64
UNION_DIGEST = "5" * 64


def build_profile_hydration_operator_projection(
    raw_session_files: dict[str, bytes],
    *,
    session_precommit=None,
    **facts: bool,
):
    return _build_profile_hydration_operator_projection(
        raw_session_files,
        session_precommit=session_precommit or fixture_grok_session_precommit(),
        **facts,
    )


def project_profile_execution_limitations(
    result,
    *,
    receipt,
    raw_session_files=None,
    session_precommit=None,
    operator_execution_facts=None,
):
    return _project_profile_execution_limitations(
        result,
        receipt=receipt,
        session_precommit=session_precommit or fixture_grok_session_precommit(),
        operator_execution_facts=operator_execution_facts
        or GrokOperatorExecutionFacts(False, False, False, False),
        raw_session_files=raw_session_files,
    )


def _matched_record(handle: str, *, sequence: int = 1) -> dict[str, Any]:
    return {
        "input_handle": handle,
        "lookup_status": "matched",
        "matched_handle": handle,
        "platform_user_id": str(90_000 + sequence),
        "display_name": f"Fixture Person {sequence}",
        "bio": f"Synthetic fixture biography {sequence}",
        "location": "Fixture City",
        "external_urls": [f"https://profile-{sequence}.invalid/about"],
        "professional_category": "Science and technology",
        "affiliations": [
            {
                "organization_name": f"Fixture Research Lab {sequence}",
                "organization_handle": f"FxOrg{sequence:04d}",
                "temporal_state": "current",
                "evidence_source": "bio_explicit",
            }
        ],
        "verification": {
            "account_verified": sequence % 2 == 0,
            "organization_affiliation_badge_observed": False,
        },
        "org_affiliation_signals": ["bio_explicit"],
        "source_status": "model_mediated_unverified",
        "missing_fields": [],
        "limitations": [],
    }


def _unmatched_record(handle: str, status: str = "not_found") -> dict[str, Any]:
    code = {"not_found": "lookup_not_found", "blocked": "lookup_blocked", "error": "lookup_error"}[status]
    record: dict[str, Any] = {
        "input_handle": handle,
        "lookup_status": status,
        "matched_handle": None,
        "source_status": "model_mediated_unverified",
        "missing_fields": list(HYDRATION_FIELDS),
        "limitations": [code],
    }
    record.update({field: None for field in HYDRATION_FIELDS})
    return record


def _result(
    records: list[dict[str, Any]],
    *,
    status: str = "X_PROFILE_HYDRATION_OK",
    batch_id: str = "fixture.profile-hydration-v1",
    campaign_id: str = "fixture.gdm-campaign-v1",
) -> dict[str, Any]:
    handles = [record["input_handle"] for record in records]
    identities = [
        ProfileHydrationIdentityExpectation(
            lead_identity=(
                f"platform:{record['platform_user_id']}"
                if record["platform_user_id"] is not None
                else f"provisional:{record['input_handle'].casefold()}"
            ),
            lookup_handle=record["input_handle"],
            expected_platform_user_id=record["platform_user_id"],
            identity_state=(
                "stable_platform_id"
                if record["platform_user_id"] is not None
                else "provisional_handle"
            ),
        )
        for record in records
    ]
    try:
        input_digest = profile_identity_input_set_sha256(identities) if handles else "7" * 64
    except ProfileHydrationContractError:
        input_digest = "7" * 64
    return {
        "contract_version": CONTRACT_VERSION,
        "campaign_id": campaign_id,
        "target_descriptor_id": "fixture.gdm-target-v1",
        "target_descriptor_sha256": TARGET_DIGEST,
        "prompt_policy_sha256": PROMPT_DIGEST,
        "discovery_union_sha256": UNION_DIGEST,
        "input_set_sha256": input_digest,
        "run_id": "fixture.profile-run-v1",
        "batch_id": batch_id,
        "status": status,
        "records": records,
        "limitations": [],
    }


def _completions(handles: list[str]) -> tuple[ProfileHydrationToolCompletion, ...]:
    return tuple(
        ProfileHydrationToolCompletion(
            call_id=f"call:{index}",
            provider_call_id=f"provider-call:{index}",
            tool_name="x_user_search",
            query=handle,
            arguments_json=json.dumps(
                {"count": "20", "query": handle},
                separators=(",", ":"),
                sort_keys=True,
            ),
            started_ledger_sequence=index * 2 - 1,
            completed_ledger_sequence=index * 2,
            start_event_sha256=canonical_json_sha256(
                {"call_id": f"call:{index}", "event": "start"}
            ),
            completion_event_sha256=canonical_json_sha256(
                {"call_id": f"call:{index}", "event": "completion"}
            ),
        )
        for index, handle in enumerate(handles, 1)
    )


def _projection(
    result: dict[str, Any],
    *,
    completions: tuple[ProfileHydrationToolCompletion, ...] | None = None,
    **facts: bool,
):
    handles = [record["input_handle"] for record in result["records"]]
    calls = _completions(handles) if completions is None else completions
    session_id = f"session:{result['batch_id']}"
    raw = raw_grok_session(
        result,
        calls=[
            (completion.tool_name, {"query": completion.query, "count": "20"})
            for completion in calls
        ],
        session_id=session_id,
    )
    return build_profile_hydration_operator_projection(
        raw,
        session_precommit=fixture_grok_session_precommit(session_id=session_id),
        result_truncated=facts.get("result_truncated", False),
        execution_deadline_reached=facts.get("execution_deadline_reached", False),
        transport_failure=facts.get("transport_failure", False),
        model_output_repaired=facts.get("model_output_repaired", False),
    )


def _expectation(
    result: dict[str, Any],
    handles: list[str],
    expected_platform_user_ids: dict[str, str | None] | None = None,
    **overrides: str,
) -> ProfileHydrationBatchExpectation:
    result_ids = {
        record["input_handle"].casefold(): record["platform_user_id"]
        for record in result["records"]
    }
    expected_platform_user_ids = expected_platform_user_ids or {}
    identities = []
    for handle in handles:
        platform_id = expected_platform_user_ids.get(handle.casefold(), result_ids.get(handle.casefold()))
        identities.append(
            ProfileHydrationIdentityExpectation(
                lead_identity=(
                    f"platform:{platform_id}"
                    if platform_id is not None
                    else f"provisional:{handle.casefold()}"
                ),
                lookup_handle=handle,
                expected_platform_user_id=platform_id,
                identity_state="stable_platform_id" if platform_id is not None else "provisional_handle",
            )
        )
    return ProfileHydrationBatchExpectation(
        campaign_id=overrides.get("campaign_id", result["campaign_id"]),
        target_descriptor_id=overrides.get(
            "target_descriptor_id", result["target_descriptor_id"]
        ),
        target_descriptor_sha256=overrides.get(
            "target_descriptor_sha256", result["target_descriptor_sha256"]
        ),
        prompt_policy_sha256=overrides.get(
            "prompt_policy_sha256", result["prompt_policy_sha256"]
        ),
        discovery_union_sha256=overrides.get(
            "discovery_union_sha256", result["discovery_union_sha256"]
        ),
        run_id=overrides.get("run_id", result["run_id"]),
        batch_id=overrides.get("batch_id", result["batch_id"]),
        input_identities=tuple(identities),
    )


class GrokProfileHydrationContractTests(unittest.TestCase):
    def setUp(self) -> None:
        self.schema = json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))

    def test_schema_is_closed_bound_and_contains_no_business_array_cap(self) -> None:
        self.assertEqual(set(self.schema["required"]), set(_result([_matched_record("FxSchema001")])))
        self.assertFalse(self.schema["additionalProperties"])

        def assert_no_max_items(value: Any) -> None:
            if isinstance(value, dict):
                self.assertNotIn("maxItems", value)
                for child in value.values():
                    assert_no_max_items(child)
            elif isinstance(value, list):
                for child in value:
                    assert_no_max_items(child)

        assert_no_max_items(self.schema)

    def test_typical_payload_cross_validates_schema_runtime_projection_and_evaluator(self) -> None:
        handles = ["FxHydr0001", "FxHydr0002"]
        payload = _result([_matched_record(handle, sequence=index) for index, handle in enumerate(handles, 1)])
        self.assertEqual(schema_errors(payload, self.schema), [])
        self.assertEqual(validate_profile_hydration_result(payload), [])
        evaluation = evaluate_profile_hydration_batch(
            _projection(payload), _expectation(payload, handles)
        )
        self.assertEqual(evaluation["status"], "valid")
        self.assertEqual(evaluation["aggregate"]["call_reconciliation"]["matched_query_count"], 2)

    def test_raw_result_and_call_shaped_dict_cannot_bypass_projection(self) -> None:
        handle = "FxRaw0001"
        payload = _result([_matched_record(handle)])
        evaluation = evaluate_profile_hydration_batch(  # type: ignore[arg-type]
            payload, _expectation(payload, [handle])
        )
        self.assertEqual(evaluation["status"], "invalid")
        self.assertIn("hydration_projection_required", evaluation["errors"])
        with self.assertRaises(TypeError):
            evaluate_profile_hydration_batch(  # type: ignore[call-arg,arg-type]
                payload,
                _expectation(payload, [handle]),
                [{"tool_name": "x_user_search", "arguments": {"query": handle}}],
            )

    def test_zero_completed_calls_is_not_x_native(self) -> None:
        handles = [f"FxZero{index:04d}" for index in range(44)]
        payload = _result([_matched_record(handle, sequence=index + 1) for index, handle in enumerate(handles)])
        evaluation = evaluate_profile_hydration_batch(
            _projection(payload, completions=()), _expectation(payload, handles)
        )
        self.assertEqual(evaluation["status"], "invalid")
        self.assertIn("native_tool_query_missing", evaluation["errors"])
        self.assertEqual(evaluation["aggregate"]["call_reconciliation"]["native_tool_call_count"], 0)

    def test_empty_batch_cannot_vacuously_prove_x_native_hydration(self) -> None:
        payload = _result([], status="X_PROFILE_HYDRATION_BLOCKED")
        payload["limitations"] = ["native_x_lookup_incomplete"]
        projection = _projection(payload, completions=())
        expectation = _expectation(payload, [])
        evaluation = evaluate_profile_hydration_batch(projection, expectation)
        self.assertEqual(evaluation["status"], "invalid")
        self.assertIn("expected_identities_empty", evaluation["errors"])

    def test_completed_lifecycle_requires_pairing_unique_sequences_and_terminal_order(self) -> None:
        handle = "FxLife0001"
        payload = _result([_matched_record(handle)])
        calls = [("x_user_search", {"query": handle, "count": "20"})]
        raw = raw_grok_session(payload, calls=calls, progress_payloads=({}, {}, {}))
        projection = build_profile_hydration_operator_projection(raw)
        forged_completion = replace(
            projection.receipt.tool_completions[0],
            completion_event_sha256="f" * 64,
        )
        forged_receipt = replace(
            projection.receipt,
            tool_completions=(forged_completion,),
        )
        with self.assertRaisesRegex(ProfileHydrationContractError, "receipt_replay_mismatch"):
            project_profile_execution_limitations(
                payload,
                receipt=forged_receipt,
                raw_session_files=raw,
            )
        with self.assertRaisesRegex(ProfileHydrationContractError, "receipt_replay_mismatch"):
            project_profile_execution_limitations(
                payload,
                receipt=replace(projection.receipt, result_truncated=True),
                session_precommit=projection.session_precommit,
                operator_execution_facts=projection.operator_execution_facts,
                raw_session_files=raw,
            )
        with self.assertRaisesRegex(ProfileHydrationContractError, "terminal_pairing_invalid"):
            build_profile_hydration_operator_projection(
                raw_grok_session(
                    payload,
                    calls=calls,
                    omit_completion_indices=frozenset({1}),
                )
            )
        with self.assertRaisesRegex(ProfileHydrationContractError, "causality_invalid"):
            build_profile_hydration_operator_projection(
                raw_grok_session(payload, calls=calls, terminal_before_tools=True)
            )
        with self.assertRaisesRegex(ProfileHydrationContractError, "unsupported_tool"):
            build_profile_hydration_operator_projection(
                raw_grok_session(payload, calls=[("x_keyword_search", {"query": handle})])
            )
        for bad_arguments in (
            {"query": handle},
            {"query": handle, "count": "-1"},
            {"query": handle, "count": "20", "extra": True},
        ):
            with self.subTest(bad_arguments=bad_arguments):
                with self.assertRaisesRegex(
                    ProfileHydrationContractError,
                    "tool_arguments_invalid",
                ):
                    build_profile_hydration_operator_projection(
                        raw_grok_session(
                            payload,
                            calls=[("x_user_search", bad_arguments)],
                        )
                    )
        bad_query = replace(projection.receipt.tool_completions[0], query=f"@{handle}")
        with self.assertRaisesRegex(ProfileHydrationContractError, "receipt_replay_mismatch"):
            project_profile_execution_limitations(
                payload,
                receipt=replace(projection.receipt, tool_completions=(bad_query,)),
                raw_session_files=raw,
            )

    def test_raw_session_replay_requires_user_turn_as_first_registered_update(self) -> None:
        handle = "FxUserFirst02"
        payload = _result([_matched_record(handle)])
        calls = [("x_user_search", {"query": handle, "count": "20"})]
        with self.assertRaisesRegex(
            ProfileHydrationContractError,
            "raw_session_user_turn_order_invalid",
        ):
            build_profile_hydration_operator_projection(
                raw_grok_session(
                    payload,
                    calls=calls,
                    thought_texts=(),
                    user_message_after_tools=True,
                )
            )

    def test_raw_session_replay_bounds_assistant_json_before_recursive_decode(self) -> None:
        handle = "FxJsonBound02"
        payload = _result([_matched_record(handle)])
        deeply_nested = '{"x":' * 10_000 + "0" + "}" * 10_000
        with self.assertRaisesRegex(
            ProfileHydrationContractError,
            "assistant_output_json_depth_budget_exceeded",
        ) as caught:
            build_profile_hydration_operator_projection(
                raw_grok_session(
                    payload,
                    calls=[("x_user_search", {"query": handle, "count": "20"})],
                    terminal_text_override=deeply_nested,
                )
            )
        self.assertEqual(
            str(caught.exception),
            "operator_raw_session_invalid:assistant_output_json_depth_budget_exceeded",
        )

    def test_receipt_binds_session_terminal_campaign_batch_and_input_set(self) -> None:
        payload = _result([_matched_record("FxReceipt01")])
        projection = _projection(payload)
        raw = {
            artifact.name: artifact.content
            for artifact in projection.raw_session_artifacts
        }
        mismatches = (
            ("campaign_id", "fixture.other-campaign", "campaign_id_mismatch"),
            ("batch_id", "fixture.other-batch", "batch_id_mismatch"),
            ("input_set_sha256", "8" * 64, "input_set_sha256_mismatch"),
            ("terminal_sha256", "9" * 64, "terminal_sha256_mismatch"),
        )
        for field, value, _error in mismatches:
            with self.subTest(field=field):
                with self.assertRaisesRegex(ProfileHydrationContractError, "receipt_replay_mismatch"):
                    project_profile_execution_limitations(
                        payload,
                        receipt=replace(projection.receipt, **{field: value}),
                        session_precommit=projection.session_precommit,
                        operator_execution_facts=projection.operator_execution_facts,
                        raw_session_files=raw,
                    )
        with self.assertRaisesRegex(ProfileHydrationContractError, "operator_raw_session_required"):
            project_profile_execution_limitations(
                payload,
                receipt=projection.receipt,
                session_precommit=projection.session_precommit,
                operator_execution_facts=projection.operator_execution_facts,
            )

        cross_campaign = evaluate_profile_hydration_batch(
            projection,
            _expectation(
                payload,
                ["FxReceipt01"],
                campaign_id="fixture.other-campaign",
            ),
        )
        self.assertEqual(cross_campaign["status"], "invalid")
        self.assertIn(
            "result_expectation_campaign_id_mismatch",
            cross_campaign["errors"],
        )
        self.assertIn(
            "receipt_expectation_campaign_id_mismatch",
            cross_campaign["errors"],
        )
        with self.assertRaisesRegex(ProfileHydrationContractError, "receipt_replay_mismatch"):
            project_profile_execution_limitations(
                payload,
                receipt=replace(
                    projection.receipt,
                    transport_failure=1,  # type: ignore[arg-type]
                ),
                session_precommit=projection.session_precommit,
                operator_execution_facts=projection.operator_execution_facts,
                raw_session_files=raw,
            )

    def test_more_than_one_hundred_rows_calls_urls_and_affiliations_have_no_cap(self) -> None:
        handles = [f"FxBig{index:04d}" for index in range(137)]
        records = [_matched_record(handle, sequence=index + 1) for index, handle in enumerate(handles)]
        records[0]["external_urls"] = [f"https://link-{index}.invalid" for index in range(121)]
        records[0]["affiliations"] = [
            {
                "organization_name": f"Fixture Organization {index}",
                "organization_handle": None,
                "temporal_state": "ambiguous",
                "evidence_source": "profile_field",
            }
            for index in range(121)
        ]
        records[0]["org_affiliation_signals"] = ["profile_field"]
        payload = _result(records)
        self.assertEqual(validate_profile_hydration_result(payload), [])
        evaluation = evaluate_profile_hydration_batch(
            _projection(payload), _expectation(payload, handles)
        )
        self.assertEqual(evaluation["status"], "valid")
        self.assertEqual(evaluation["aggregate"]["input_record_reconciliation"]["expected_input_count"], 137)

    def test_missing_duplicate_extra_queries_fail_closed_from_typed_receipt(self) -> None:
        handles = ["FxCall0001", "FxCall0002", "FxCall0003"]
        payload = _result([_matched_record(handle, sequence=index) for index, handle in enumerate(handles, 1)])
        base = list(_completions(handles))
        duplicate = replace(
            base[2],
            call_id="call:4",
            query=handles[0],
            started_ledger_sequence=7,
            completed_ledger_sequence=8,
            start_event_sha256=canonical_json_sha256(
                {"call_id": "call:4", "event": "start"}
            ),
            completion_event_sha256=canonical_json_sha256(
                {"call_id": "call:4", "event": "completion"}
            ),
        )
        duplicate_eval = evaluate_profile_hydration_batch(
            _projection(payload, completions=tuple((*base, duplicate))),
            _expectation(payload, handles),
        )
        self.assertIn("native_tool_query_duplicate", duplicate_eval["errors"])
        missing_eval = evaluate_profile_hydration_batch(
            _projection(payload, completions=tuple(base[:-1])),
            _expectation(payload, handles),
        )
        self.assertIn("native_tool_query_missing", missing_eval["errors"])
        extra = replace(
            base[2],
            call_id="call:4",
            query="FxExtra0001",
            started_ledger_sequence=7,
            completed_ledger_sequence=8,
            start_event_sha256=canonical_json_sha256(
                {"call_id": "call:4", "event": "start"}
            ),
            completion_event_sha256=canonical_json_sha256(
                {"call_id": "call:4", "event": "completion"}
            ),
        )
        extra_eval = evaluate_profile_hydration_batch(
            _projection(payload, completions=tuple((*base, extra))),
            _expectation(payload, handles),
        )
        self.assertIn("native_tool_query_extra", extra_eval["errors"])

    def test_expected_input_digest_and_row_bijection_fail_closed(self) -> None:
        handles = ["FxRow0001", "FxRow0002"]
        payload = _result([_matched_record(handle, sequence=index) for index, handle in enumerate(handles, 1)])
        mismatched_handles = [handles[0], "FxOther001"]
        evaluation = evaluate_profile_hydration_batch(
            _projection(payload), _expectation(payload, mismatched_handles)
        )
        self.assertIn("result_input_set_digest_mismatch", evaluation["errors"])
        self.assertIn("records_input_missing", evaluation["errors"])
        self.assertIn("records_input_extra", evaluation["errors"])

    def test_stable_platform_identity_mismatch_is_quarantined_fail_closed(self) -> None:
        handle = "FxStable001"
        payload = _result([_matched_record(handle)])
        expected_id = "999001"
        expectation = _expectation(
            payload,
            [handle],
            expected_platform_user_ids={handle.casefold(): expected_id},
        )
        payload["input_set_sha256"] = profile_identity_input_set_sha256(
            expectation.input_identities
        )
        evaluation = evaluate_profile_hydration_batch(
            _projection(payload), expectation
        )
        self.assertEqual(evaluation["status"], "invalid")
        self.assertIn(
            "stable_identity_platform_user_id_mismatch", evaluation["errors"]
        )
        self.assertEqual(
            evaluation["aggregate"]["input_record_reconciliation"][
                "platform_id_mismatch_quarantine_count"
            ],
            1,
        )

    def test_stable_platform_identity_missing_is_fail_closed(self) -> None:
        handle = "FxStable002"
        record = _matched_record(handle)
        record["platform_user_id"] = None
        record["missing_fields"] = ["platform_user_id"]
        record["limitations"] = ["profile_fields_not_exposed"]
        payload = _result([record])
        expected_id = "999002"
        expectation = _expectation(
            payload,
            [handle],
            expected_platform_user_ids={handle.casefold(): expected_id},
        )
        payload["input_set_sha256"] = profile_identity_input_set_sha256(
            expectation.input_identities
        )
        evaluation = evaluate_profile_hydration_batch(
            _projection(payload), expectation
        )
        self.assertEqual(evaluation["status"], "invalid")
        self.assertIn(
            "stable_identity_platform_user_id_missing", evaluation["errors"]
        )
        self.assertEqual(
            evaluation["aggregate"]["input_record_reconciliation"][
                "stable_platform_id_missing_count"
            ],
            1,
        )

    def test_wrong_nested_projection_and_expectation_types_fail_closed_candidate_free(self) -> None:
        handle = "FxNested001"
        payload = _result([_matched_record(handle)])
        projection = _projection(payload)
        expectation = _expectation(payload, [handle])
        malformed_projection = replace(
            projection,
            receipt={"forged": True},  # type: ignore[arg-type]
        )
        evaluation = evaluate_profile_hydration_batch(
            malformed_projection, expectation
        )
        self.assertEqual(evaluation["status"], "invalid")
        self.assertIn("hydration_projection_receipt_type_invalid", evaluation["errors"])
        self.assertNotIn(handle, json.dumps(evaluation, sort_keys=True))

        malformed_result = replace(
            projection,
            result=[],  # type: ignore[arg-type]
        )
        evaluation = evaluate_profile_hydration_batch(
            malformed_result, expectation
        )
        self.assertEqual(evaluation["status"], "invalid")
        self.assertIn("hydration_projection_result_type_invalid", evaluation["errors"])
        self.assertNotIn(handle, json.dumps(evaluation, sort_keys=True))

        malformed_precommit = replace(
            projection,
            session_precommit={"forged": True},  # type: ignore[arg-type]
        )
        evaluation = evaluate_profile_hydration_batch(
            malformed_precommit, expectation
        )
        self.assertEqual(evaluation["status"], "invalid")
        self.assertIn(
            "hydration_projection_session_precommit_type_invalid",
            evaluation["errors"],
        )
        self.assertNotIn(handle, json.dumps(evaluation, sort_keys=True))

        malformed_execution_facts = replace(
            projection,
            operator_execution_facts={"result_truncated": True},  # type: ignore[arg-type]
        )
        evaluation = evaluate_profile_hydration_batch(
            malformed_execution_facts, expectation
        )
        self.assertEqual(evaluation["status"], "invalid")
        self.assertIn(
            "hydration_projection_execution_facts_type_invalid",
            evaluation["errors"],
        )
        self.assertNotIn(handle, json.dumps(evaluation, sort_keys=True))

        malformed_expectation = replace(
            expectation,
            input_identities=({"lookup_handle": handle},),  # type: ignore[arg-type]
        )
        evaluation = evaluate_profile_hydration_batch(
            projection, malformed_expectation
        )
        self.assertEqual(evaluation["status"], "invalid")
        self.assertIn("expectation_identity_type_invalid", evaluation["errors"])
        self.assertNotIn(handle, json.dumps(evaluation, sort_keys=True))

        leaked_result = copy.deepcopy(projection.result)
        leaked_result["records"][0][handle] = True
        evaluation = evaluate_profile_hydration_batch(
            replace(projection, result=leaked_result), expectation
        )
        self.assertEqual(evaluation["status"], "invalid")
        self.assertIn("hydration_projection_invalid", evaluation["errors"])
        self.assertNotIn(handle, json.dumps(evaluation, sort_keys=True))

    def test_matched_handle_must_bind_to_exact_input_identity(self) -> None:
        handle = "FxMatch0001"
        payload = _result([_matched_record(handle)])
        payload["records"][0]["matched_handle"] = "FxOther0001"
        self.assertIn(
            "record:0:matched_handle_binding_invalid",
            validate_profile_hydration_result(payload),
        )

    def test_operator_projection_blocks_empty_hard_execution_failure(self) -> None:
        payload = _result([], status="X_PROFILE_HYDRATION_BLOCKED")
        payload["limitations"] = ["transport_failure"]
        projection = _projection(
            payload,
            completions=(),
            execution_deadline_reached=True,
        )
        self.assertEqual(projection.result["status"], "X_PROFILE_HYDRATION_BLOCKED")
        self.assertEqual(
            projection.result["limitations"],
            ["execution_deadline_reached"],
        )

    def test_not_found_is_partial_with_exact_failure_limitation(self) -> None:
        handles = ["FxFound001", "FxMissing01"]
        payload = _result(
            [_matched_record(handles[0]), _unmatched_record(handles[1])],
            status="X_PROFILE_HYDRATION_PARTIAL",
        )
        self.assertEqual(validate_profile_hydration_result(payload), [])
        self.assertEqual(
            evaluate_profile_hydration_batch(
                _projection(payload), _expectation(payload, handles)
            )["status"],
            "valid",
        )
        contradictory = copy.deepcopy(payload)
        contradictory["records"][1]["limitations"].extend(["lookup_blocked", "lookup_error"])
        self.assertIn(
            "record:1:lookup_limitation_mismatch",
            validate_profile_hydration_result(contradictory),
        )

    def test_operator_projection_normalizes_before_status_coherence(self) -> None:
        payload = _result([_matched_record("FxProject001")])
        payload["status"] = "X_PROFILE_HYDRATION_OK"
        payload["limitations"] = ["result_truncated"]
        self.assertIn("status_coherence_invalid", validate_profile_hydration_result(payload))
        projection = _projection(payload)
        self.assertEqual(projection.result["status"], "X_PROFILE_HYDRATION_OK")
        self.assertEqual(projection.result["limitations"], [])

    def test_record_repair_is_operator_owned_and_projected_for_every_record(self) -> None:
        records = [_matched_record("FxRepair001"), _matched_record("FxRepair002", sequence=2)]
        records[0]["limitations"] = ["model_output_repaired"]
        payload = _result(records)
        self.assertIn(
            "record:0:record_repair_ownership_mismatch",
            validate_profile_hydration_result(payload),
        )
        removed = _projection(payload)
        self.assertEqual(removed.removed_model_record_repair_count, 1)
        self.assertTrue(all("model_output_repaired" not in row["limitations"] for row in removed.result["records"]))
        added = _projection(payload, model_output_repaired=True)
        self.assertEqual(added.added_operator_record_repair_count, 2)
        self.assertIn("model_output_repaired", added.result["limitations"])
        self.assertTrue(all("model_output_repaired" in row["limitations"] for row in added.result["records"]))
        self.assertEqual(validate_profile_hydration_result(added.result), [])

    def test_missing_fields_duplicate_platform_id_and_badge_binding_fail_closed(self) -> None:
        record = _matched_record("FxNull0001")
        record["bio"] = None
        record["missing_fields"] = ["bio"]
        record["limitations"] = ["profile_fields_not_exposed"]
        self.assertEqual(validate_profile_hydration_result(_result([record])), [])
        missing = copy.deepcopy(record)
        missing["missing_fields"] = []
        self.assertIn("record:0:missing_fields_mismatch", validate_profile_hydration_result(_result([missing])))

        records = [_matched_record("FxDupId001"), _matched_record("FxDupId002", sequence=2)]
        records[1]["platform_user_id"] = records[0]["platform_user_id"]
        self.assertIn("record:1:platform_user_id_duplicate", validate_profile_hydration_result(_result(records)))

        badge = _matched_record("FxBadge001")
        badge["affiliations"][0]["evidence_source"] = "profile_affiliation_badge"
        badge["org_affiliation_signals"] = ["profile_affiliation_badge"]
        badge["verification"]["organization_affiliation_badge_observed"] = False
        self.assertIn(
            "record:0:verification_badge_signal_mismatch",
            validate_profile_hydration_result(_result([badge])),
        )

    def test_evaluation_is_candidate_free_and_inputs_are_not_mutated(self) -> None:
        handle = "FxPrivate01"
        payload = _result([_matched_record(handle)])
        projection = _projection(payload)
        before = copy.deepcopy((payload, projection))
        evaluation = evaluate_profile_hydration_batch(
            projection, _expectation(payload, [handle])
        )
        self.assertEqual((payload, projection), before)
        serialized = json.dumps(evaluation, sort_keys=True)
        self.assertNotIn(handle, serialized)
        self.assertNotIn(payload["records"][0]["bio"], serialized)
        self.assertNotIn(payload["records"][0]["location"], serialized)

    def test_projection_digest_tamper_fails_at_evaluation_boundary(self) -> None:
        payload = _result([_matched_record("FxTamper001")])
        projection = _projection(payload)
        forged = replace(projection, projected_result_sha256="a" * 64)
        evaluation = evaluate_profile_hydration_batch(
            forged, _expectation(payload, ["FxTamper001"])
        )
        self.assertEqual(evaluation["status"], "invalid")
        self.assertIn("hydration_projection_result_digest_mismatch", evaluation["errors"])


if __name__ == "__main__":
    unittest.main()
