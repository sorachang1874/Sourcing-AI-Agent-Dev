from __future__ import annotations

import copy
import json
import unittest
from pathlib import Path
from typing import Any

from x_first.grok_profile_hydration import (
    HYDRATION_FIELDS,
    ProfileHydrationContractError,
    evaluate_profile_hydration_batch,
    project_profile_execution_limitations,
    validate_profile_hydration_result,
)
from x_first.recall_pool_schema import schema_errors

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCHEMA_PATH = PROJECT_ROOT / "contracts" / "x.grok.profile_hydration.result.v1.schema.json"


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


def _not_found_record(handle: str) -> dict[str, Any]:
    record: dict[str, Any] = {
        "input_handle": handle,
        "lookup_status": "not_found",
        "matched_handle": None,
        "source_status": "model_mediated_unverified",
        "missing_fields": list(HYDRATION_FIELDS),
        "limitations": ["lookup_not_found"],
    }
    record.update({field: None for field in HYDRATION_FIELDS})
    return record


def _result(records: list[dict[str, Any]], *, status: str = "X_PROFILE_HYDRATION_OK") -> dict[str, Any]:
    return {
        "batch_id": "fixture.profile-hydration-v1",
        "status": status,
        "records": records,
        "limitations": [],
    }


def _calls(handles: list[str]) -> list[dict[str, Any]]:
    return [
        {
            "tool_name": "x_user_search",
            "arguments": {"query": handle, "count": "50"},
            "ledger_sequence": index,
        }
        for index, handle in enumerate(handles, start=1)
    ]


class GrokProfileHydrationContractTests(unittest.TestCase):
    def setUp(self) -> None:
        self.schema = json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))

    def test_schema_is_closed_and_contains_no_business_array_cap(self) -> None:
        self.assertEqual(set(self.schema["required"]), {"batch_id", "status", "records", "limitations"})
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

    def test_typical_payload_cross_validates_schema_runtime_and_evaluator(self) -> None:
        handles = ["FxHydr0001", "FxHydr0002"]
        payload = _result([_matched_record(handle, sequence=index) for index, handle in enumerate(handles, 1)])

        self.assertEqual(schema_errors(payload, self.schema), [])
        self.assertEqual(validate_profile_hydration_result(payload), [])
        evaluation = evaluate_profile_hydration_batch(payload, handles, _calls(handles))

        self.assertEqual(evaluation["status"], "valid")
        self.assertEqual(evaluation["errors"], [])
        self.assertEqual(evaluation["aggregate"]["call_reconciliation"]["matched_query_count"], 2)
        self.assertEqual(
            evaluation["aggregate"]["field_coverage"]["bio"]["matched_record_coverage_rate"],
            1.0,
        )

    def test_schema_valid_zero_tool_output_is_not_x_native(self) -> None:
        handles = [f"FxZero{index:04d}" for index in range(44)]
        payload = _result([_matched_record(handle, sequence=index + 1) for index, handle in enumerate(handles)])
        self.assertEqual(schema_errors(payload, self.schema), [])

        evaluation = evaluate_profile_hydration_batch(payload, handles, [])

        self.assertEqual(evaluation["status"], "invalid")
        self.assertIn("native_tool_query_missing", evaluation["errors"])
        self.assertIn("native_tool_call_count_mismatch", evaluation["errors"])
        reconciliation = evaluation["aggregate"]["call_reconciliation"]
        self.assertEqual(reconciliation["native_tool_call_count"], 0)
        self.assertEqual(reconciliation["missing_query_count"], 44)

    def test_empty_batch_cannot_vacuously_prove_x_native_hydration(self) -> None:
        payload = {
            "batch_id": "fixture.empty-hydration-v1",
            "status": "X_PROFILE_HYDRATION_BLOCKED",
            "records": [],
            "limitations": ["native_x_lookup_incomplete"],
        }
        self.assertEqual(validate_profile_hydration_result(payload), [])

        evaluation = evaluate_profile_hydration_batch(payload, [], [])

        self.assertEqual(evaluation["status"], "invalid")
        self.assertIn("expected_handles_empty", evaluation["errors"])

    def test_exact_forty_four_calls_and_rows_are_valid(self) -> None:
        handles = [f"FxFort{index:04d}" for index in range(44)]
        payload = _result([_matched_record(handle, sequence=index + 1) for index, handle in enumerate(handles)])

        evaluation = evaluate_profile_hydration_batch(payload, handles, _calls(handles))

        self.assertEqual(evaluation["status"], "valid")
        self.assertEqual(evaluation["aggregate"]["input_record_reconciliation"]["expected_input_count"], 44)
        self.assertEqual(evaluation["aggregate"]["call_reconciliation"]["valid_x_user_search_count"], 44)

    def test_more_than_one_hundred_records_calls_urls_and_affiliations_have_no_cap(self) -> None:
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

        self.assertEqual(schema_errors(payload, self.schema), [])
        self.assertEqual(validate_profile_hydration_result(payload), [])
        evaluation = evaluate_profile_hydration_batch(payload, handles, _calls(handles))
        self.assertEqual(evaluation["status"], "valid")
        self.assertEqual(evaluation["aggregate"]["input_record_reconciliation"]["expected_input_count"], 137)

    def test_duplicate_missing_extra_and_unexpected_native_calls_fail_closed(self) -> None:
        handles = ["FxCall0001", "FxCall0002", "FxCall0003"]
        payload = _result([_matched_record(handle, sequence=index) for index, handle in enumerate(handles, 1)])

        duplicate = _calls(handles) + _calls([handles[0]])
        duplicate_evaluation = evaluate_profile_hydration_batch(payload, handles, duplicate)
        self.assertEqual(duplicate_evaluation["status"], "invalid")
        self.assertIn("native_tool_query_duplicate", duplicate_evaluation["errors"])

        missing_evaluation = evaluate_profile_hydration_batch(payload, handles, _calls(handles[:-1]))
        self.assertEqual(missing_evaluation["status"], "invalid")
        self.assertIn("native_tool_query_missing", missing_evaluation["errors"])

        extra = _calls(handles) + _calls(["FxExtra0001"])
        extra_evaluation = evaluate_profile_hydration_batch(payload, handles, extra)
        self.assertEqual(extra_evaluation["status"], "invalid")
        self.assertIn("native_tool_query_extra", extra_evaluation["errors"])

        wrong_tool = _calls(handles)
        wrong_tool[0]["tool_name"] = "x_semantic_search"
        wrong_tool_evaluation = evaluate_profile_hydration_batch(payload, handles, wrong_tool)
        self.assertEqual(wrong_tool_evaluation["status"], "invalid")
        self.assertIn("native_tool_family_invalid", wrong_tool_evaluation["errors"])

    def test_user_search_query_must_be_exact_bare_expected_handle(self) -> None:
        handle = "FxBare0001"
        payload = _result([_matched_record(handle)])
        for query in (f"@{handle}", f"https://x.com/{handle}", f"{handle} researcher"):
            with self.subTest(query=query):
                calls = _calls([handle])
                calls[0]["arguments"]["query"] = query
                evaluation = evaluate_profile_hydration_batch(payload, [handle], calls)
                self.assertEqual(evaluation["status"], "invalid")
                self.assertIn("native_tool_call_shape_invalid", evaluation["errors"])

    def test_record_input_set_requires_exact_casefold_bijection(self) -> None:
        handles = ["FxRow0001", "FxRow0002"]
        valid_records = [_matched_record(handle, sequence=index) for index, handle in enumerate(handles, 1)]

        duplicate_payload = _result([valid_records[0], copy.deepcopy(valid_records[0])])
        duplicate_evaluation = evaluate_profile_hydration_batch(duplicate_payload, handles, _calls(handles))
        self.assertEqual(duplicate_evaluation["status"], "invalid")
        self.assertIn("records_input_duplicate", duplicate_evaluation["errors"])
        self.assertIn("records_input_missing", duplicate_evaluation["errors"])

        extra_payload = _result(valid_records + [_matched_record("FxRow9999", sequence=9)])
        extra_evaluation = evaluate_profile_hydration_batch(extra_payload, handles, _calls(handles))
        self.assertEqual(extra_evaluation["status"], "invalid")
        self.assertIn("records_input_extra", extra_evaluation["errors"])

    def test_matched_handle_must_bind_to_input_identity(self) -> None:
        handle = "FxBind0001"
        payload = _result([_matched_record(handle)])
        payload["records"][0]["matched_handle"] = "FxOther0001"

        self.assertIn(
            "record:0:matched_handle_binding_invalid",
            validate_profile_hydration_result(payload),
        )
        self.assertEqual(evaluate_profile_hydration_batch(payload, [handle], _calls([handle]))["status"], "invalid")

    def test_not_found_is_valid_partial_when_row_and_call_are_preserved(self) -> None:
        handles = ["FxFound001", "FxMissing01"]
        payload = _result(
            [_matched_record(handles[0]), _not_found_record(handles[1])],
            status="X_PROFILE_HYDRATION_PARTIAL",
        )

        self.assertEqual(schema_errors(payload, self.schema), [])
        self.assertEqual(validate_profile_hydration_result(payload), [])
        evaluation = evaluate_profile_hydration_batch(payload, handles, _calls(handles))
        self.assertEqual(evaluation["status"], "valid")
        self.assertEqual(evaluation["aggregate"]["lookup_status_counts"]["not_found"], 1)
        self.assertEqual(evaluation["aggregate"]["call_reconciliation"]["matched_query_count"], 2)

    def test_missing_fields_exactly_track_null_not_observed_empty(self) -> None:
        handle = "FxNull0001"
        valid = _matched_record(handle)
        valid["bio"] = None
        valid["external_urls"] = []
        valid["missing_fields"] = ["bio"]
        valid["limitations"] = ["profile_fields_not_exposed"]
        self.assertEqual(validate_profile_hydration_result(_result([valid])), [])

        missing_marker = copy.deepcopy(valid)
        missing_marker["missing_fields"] = []
        self.assertIn(
            "record:0:missing_fields_mismatch",
            validate_profile_hydration_result(_result([missing_marker])),
        )

        false_missing = copy.deepcopy(valid)
        false_missing["missing_fields"] = ["bio", "external_urls"]
        self.assertIn(
            "record:0:missing_fields_mismatch",
            validate_profile_hydration_result(_result([false_missing])),
        )

        missing_limitation = copy.deepcopy(valid)
        missing_limitation["limitations"] = []
        self.assertIn(
            "record:0:profile_fields_limitation_mismatch",
            validate_profile_hydration_result(_result([missing_limitation])),
        )

    def test_duplicate_platform_user_id_is_quarantined(self) -> None:
        handles = ["FxIdDup0001", "FxIdDup0002"]
        records = [_matched_record(handle, sequence=index) for index, handle in enumerate(handles, 1)]
        records[1]["platform_user_id"] = records[0]["platform_user_id"]
        payload = _result(records)

        self.assertIn(
            "record:1:platform_user_id_duplicate",
            validate_profile_hydration_result(payload),
        )
        self.assertEqual(
            evaluate_profile_hydration_batch(payload, handles, _calls(handles))["status"],
            "invalid",
        )

    def test_affiliation_sources_and_verification_badge_are_cross_field_bound(self) -> None:
        handle = "FxBadge0001"
        record = _matched_record(handle)
        record["affiliations"] = [
            {
                "organization_name": "Fixture Badge Lab",
                "organization_handle": "FxBadgeLab",
                "temporal_state": "current",
                "evidence_source": "profile_affiliation_badge",
            }
        ]
        record["org_affiliation_signals"] = ["profile_affiliation_badge"]
        record["verification"]["organization_affiliation_badge_observed"] = True
        self.assertEqual(validate_profile_hydration_result(_result([record])), [])

        mismatched = copy.deepcopy(record)
        mismatched["verification"]["organization_affiliation_badge_observed"] = False
        self.assertIn(
            "record:0:verification_badge_signal_mismatch",
            validate_profile_hydration_result(_result([mismatched])),
        )

        missing_verification = copy.deepcopy(record)
        missing_verification["verification"] = None
        missing_verification["missing_fields"] = ["verification"]
        missing_verification["limitations"] = ["profile_fields_not_exposed"]
        self.assertIn(
            "record:0:verification_badge_signal_mismatch",
            validate_profile_hydration_result(_result([missing_verification])),
        )

    def test_validation_and_evaluation_do_not_mutate_inputs_or_emit_candidate_text(self) -> None:
        handles = ["FxPrivate01"]
        payload = _result([_matched_record(handles[0])])
        calls = _calls(handles)
        before = copy.deepcopy((payload, handles, calls))

        self.assertEqual(validate_profile_hydration_result(payload), [])
        evaluation = evaluate_profile_hydration_batch(payload, handles, calls)

        self.assertEqual((payload, handles, calls), before)
        serialized = json.dumps(evaluation, sort_keys=True)
        self.assertNotIn(handles[0], serialized)
        self.assertNotIn(payload["records"][0]["bio"], serialized)
        self.assertNotIn(payload["records"][0]["location"], serialized)
        self.assertNotIn(payload["records"][0]["affiliations"][0]["organization_name"], serialized)

    def test_operator_projection_removes_false_batch_execution_claims(self) -> None:
        payload = _result(
            [_matched_record("FxProject001")],
            status="X_PROFILE_HYDRATION_PARTIAL",
        )
        payload["limitations"] = [
            "native_x_lookup_incomplete",
            "execution_deadline_reached",
            "result_truncated",
        ]
        before = copy.deepcopy(payload)

        projection = project_profile_execution_limitations(
            payload,
            execution_deadline_reached=False,
            transport_failure=False,
            result_truncated=False,
            model_output_repaired=False,
        )

        self.assertEqual(projection.result["limitations"], ["native_x_lookup_incomplete"])
        self.assertEqual(projection.result["status"], "X_PROFILE_HYDRATION_PARTIAL")
        self.assertEqual(
            projection.removed_model_operator_limitations,
            ("execution_deadline_reached", "result_truncated"),
        )
        self.assertEqual(projection.added_operator_limitations, ())
        self.assertEqual(validate_profile_hydration_result(projection.result), [])
        self.assertEqual(payload, before)

    def test_operator_projection_adds_receipt_facts_and_recomputes_status(self) -> None:
        payload = _result([_matched_record("FxProject002")])

        projection = project_profile_execution_limitations(
            payload,
            execution_deadline_reached=False,
            transport_failure=True,
            result_truncated=True,
            model_output_repaired=False,
        )

        self.assertEqual(
            projection.result["limitations"],
            ["transport_failure", "result_truncated"],
        )
        self.assertEqual(projection.result["status"], "X_PROFILE_HYDRATION_PARTIAL")
        self.assertEqual(
            projection.added_operator_limitations,
            ("transport_failure", "result_truncated"),
        )
        self.assertEqual(validate_profile_hydration_result(projection.result), [])

    def test_operator_projection_can_upgrade_false_partial_to_ok(self) -> None:
        payload = _result(
            [_matched_record("FxProject004")],
            status="X_PROFILE_HYDRATION_PARTIAL",
        )
        payload["limitations"] = ["result_truncated"]

        projection = project_profile_execution_limitations(
            payload,
            execution_deadline_reached=False,
            transport_failure=False,
            result_truncated=False,
            model_output_repaired=False,
        )

        self.assertEqual(projection.result["status"], "X_PROFILE_HYDRATION_OK")
        self.assertEqual(projection.result["limitations"], [])
        self.assertEqual(validate_profile_hydration_result(projection.result), [])

    def test_operator_projection_blocks_empty_hard_failure(self) -> None:
        payload = {
            "batch_id": "fixture.profile-blocked-v1",
            "status": "X_PROFILE_HYDRATION_BLOCKED",
            "records": [],
            "limitations": ["transport_failure"],
        }

        projection = project_profile_execution_limitations(
            payload,
            execution_deadline_reached=True,
            transport_failure=False,
            result_truncated=False,
            model_output_repaired=False,
        )

        self.assertEqual(projection.result["status"], "X_PROFILE_HYDRATION_BLOCKED")
        self.assertEqual(projection.result["limitations"], ["execution_deadline_reached"])
        self.assertEqual(validate_profile_hydration_result(projection.result), [])

    def test_operator_projection_rejects_non_boolean_fact(self) -> None:
        with self.assertRaisesRegex(
            ProfileHydrationContractError,
            "operator_fact_invalid:transport_failure",
        ):
            project_profile_execution_limitations(
                _result([_matched_record("FxProject005")]),
                execution_deadline_reached=False,
                transport_failure=1,  # type: ignore[arg-type]
                result_truncated=False,
                model_output_repaired=False,
            )


if __name__ == "__main__":
    unittest.main()
