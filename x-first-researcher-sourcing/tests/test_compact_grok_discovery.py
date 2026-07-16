from __future__ import annotations

import copy
import json
import unittest
from itertools import product
from pathlib import Path

from x_first.compact_grok_discovery import (
    COVERAGE_CELLS,
    SOURCE_SURFACES,
    CompactDiscoveryContractError,
    canonical_profile_url,
    compare_compact_discovery_results,
    compare_lead_sets,
    merge_compact_discovery_results,
    project_compact_execution_limitations,
    summarize_compact_discovery,
    validate_compact_discovery_result,
)
from x_first.recall_pool_schema import schema_errors

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCHEMA_PATH = PROJECT_ROOT / "contracts" / "x.grok.compact_discovery.result.v1.schema.json"


def _reason_codes(lab_state: str, pretraining_state: str) -> list[str]:
    return [
        f"lab_affiliation_{lab_state}_signal",
        f"pretraining_relevance_{pretraining_state}_signal",
    ]


def _lead(
    handle: str,
    *,
    lab_state: str = "current",
    pretraining_state: str = "current",
    platform_user_id: str | None = None,
) -> dict[str, object]:
    numeric_suffix = "".join(character for character in handle if character.isdigit()) or "1"
    post_id = str(1_000_000 + int(numeric_suffix))
    return {
        "handle": handle,
        "profile_url": f"https://x.com/{handle}",
        "platform_user_id": platform_user_id,
        "target_lab_affiliation_state": lab_state,
        "pretraining_experience_state": pretraining_state,
        "source_status": "model_mediated_unverified",
        "source_refs": [
            {
                "surface": "bio",
                "url": f"https://x.com/{handle}",
                "subject_handle": handle,
                "author_handle": handle,
                "support_dimensions": ["lab_affiliation"],
            },
            {
                "surface": "self_post",
                "url": f"https://x.com/{handle}/status/{post_id}",
                "subject_handle": handle,
                "author_handle": handle,
                "support_dimensions": ["pretraining_relevance"],
            },
        ],
        "reason_codes": _reason_codes(lab_state, pretraining_state),
    }


def _result(leads: list[dict[str, object]]) -> dict[str, object]:
    return {
        "status": "X_DISCOVERY_OK",
        "strategy_id": "gdm.compact.discovery-v1",
        "leads": leads,
        "coverage_cells": list(COVERAGE_CELLS),
        "uncovered_cells": [],
        "limitations": [],
    }


class CompactGrokDiscoveryContractTests(unittest.TestCase):
    def test_schema_is_compact_closed_and_has_no_business_array_cap(self) -> None:
        schema = json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))
        self.assertEqual(
            set(schema["required"]),
            {"status", "strategy_id", "leads", "coverage_cells", "uncovered_cells", "limitations"},
        )
        self.assertFalse(schema["additionalProperties"])
        self.assertNotIn("queries", json.dumps(schema))
        self.assertNotIn("tool_counts", json.dumps(schema))
        self.assertNotIn("oneOf", schema["$defs"]["source_ref"]["properties"]["url"])
        self.assertNotIn("maxItems", schema["properties"]["leads"])
        self.assertNotIn("maxItems", schema["$defs"]["lead"]["properties"]["source_refs"])

    def test_typical_payload_matches_schema_and_runtime(self) -> None:
        schema = json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))
        payload = _result([_lead("FxSchema001", platform_user_id="99001")])
        self.assertEqual(schema_errors(payload, schema), [])
        self.assertEqual(validate_compact_discovery_result(payload), [])

    def test_more_than_one_hundred_leads_are_valid_without_business_cap(self) -> None:
        payload = _result(
            [_lead(f"FxLead{index:04d}", platform_user_id=str(50_000 + index)) for index in range(137)]
        )
        self.assertEqual(validate_compact_discovery_result(payload), [])
        self.assertEqual(summarize_compact_discovery(payload)["unique_lead_count"], 137)

    def test_more_than_one_hundred_source_refs_are_valid_without_business_cap(self) -> None:
        lead = _lead("FxManyRefs001")
        lead["source_refs"].extend(
            {
                "surface": "self_post",
                "url": f"https://x.com/FxManyRefs001/status/{2_000_000 + index}",
                "subject_handle": "FxManyRefs001",
                "author_handle": "FxManyRefs001",
                "support_dimensions": ["pretraining_relevance"],
            }
            for index in range(121)
        )
        payload = _result([lead])
        self.assertEqual(validate_compact_discovery_result(payload), [])
        self.assertEqual(summarize_compact_discovery(payload)["source_refs"]["total"], 123)

    def test_root_search_and_placeholder_profile_urls_fail_closed(self) -> None:
        for invalid_url in (
            "https://x.com",
            "https://x.com/",
            "https://x.com/search",
            "https://x.com/placeholder",
            "https://x.com/example_user",
        ):
            with self.subTest(invalid_url=invalid_url):
                payload = _result([_lead("FxRoute001")])
                payload["leads"][0]["profile_url"] = invalid_url
                payload["leads"][0]["source_refs"][0]["url"] = invalid_url
                self.assertTrue(validate_compact_discovery_result(payload))

    def test_casefold_duplicate_handles_fail(self) -> None:
        payload = _result([_lead("FxCase001"), _lead("fxcase001")])
        self.assertIn("lead:1:handle_casefold_duplicate", validate_compact_discovery_result(payload))

    def test_surface_url_type_mismatch_fails(self) -> None:
        payload = _result([_lead("FxSurface001")])
        payload["leads"][0]["source_refs"][1]["surface"] = "profile"
        errors = validate_compact_discovery_result(payload)
        self.assertIn("lead:0:source_ref:1:source_ref_profile_binding_invalid", errors)

    def test_profile_url_must_bind_to_handle_identity(self) -> None:
        payload = _result([_lead("FxProfile001")])
        payload["leads"][0]["profile_url"] = "https://x.com/FxOther001"
        self.assertIn("lead:0:profile_url_binding_invalid", validate_compact_discovery_result(payload))

    def test_source_subject_author_and_url_identity_mismatch_fail(self) -> None:
        subject_mismatch = _result([_lead("FxBind001")])
        subject_mismatch["leads"][0]["source_refs"][1]["subject_handle"] = "FxOther001"
        self.assertIn(
            "lead:0:source_ref:1:source_ref_subject_binding_invalid",
            validate_compact_discovery_result(subject_mismatch),
        )

        author_mismatch = _result([_lead("FxBind002")])
        author_mismatch["leads"][0]["source_refs"][1]["author_handle"] = "FxOther002"
        self.assertIn(
            "lead:0:source_ref:1:source_ref_status_binding_invalid",
            validate_compact_discovery_result(author_mismatch),
        )

        host_mismatch = _result([_lead("FxBind003")])
        host_mismatch["leads"][0]["source_refs"][1]["url"] = (
            "https://X.com/FxBind003/status/1000003"
        )
        self.assertIn(
            "lead:0:source_ref:1:source_ref_status_binding_invalid",
            validate_compact_discovery_result(host_mismatch),
        )

    def test_handle_profile_subject_author_and_status_url_case_compare_by_identity(self) -> None:
        payload = _result([_lead("FxMixedCase01")])
        lead = payload["leads"][0]
        lead["profile_url"] = "https://x.com/fxmixedcase01"
        lead["source_refs"][0].update(
            {
                "url": "https://x.com/FXMIXEDCASE01",
                "subject_handle": "fxmixedcase01",
                "author_handle": "FXMIXEDCASE01",
            }
        )
        lead["source_refs"][1].update(
            {
                "url": "https://x.com/fxmixedcase01/status/1000001",
                "subject_handle": "FXMIXEDCASE01",
                "author_handle": "fxmixedcase01",
            }
        )

        self.assertEqual(schema_errors(payload, json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))), [])
        self.assertEqual(validate_compact_discovery_result(payload), [])
        self.assertEqual(canonical_profile_url("FxMixedCase01"), "https://x.com/FxMixedCase01")

    def test_all_nine_independent_temporal_state_combinations_are_valid_and_counted(self) -> None:
        states = ("current", "historical", "ambiguous")
        leads = [
            _lead(
                f"FxState{index:02d}",
                lab_state=lab_state,
                pretraining_state=pretraining_state,
            )
            for index, (lab_state, pretraining_state) in enumerate(product(states, states), start=1)
        ]
        payload = _result(leads)
        self.assertEqual(validate_compact_discovery_result(payload), [])
        matrix = summarize_compact_discovery(payload)["state_matrix"]
        for lab_state, pretraining_state in product(states, states):
            self.assertEqual(matrix[lab_state][pretraining_state], 1)

    def test_ambiguous_axis_may_lack_support_without_losing_recall(self) -> None:
        schema = json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))
        cases = (
            ("current", "ambiguous", "lab_affiliation"),
            ("historical", "ambiguous", "lab_affiliation"),
            ("ambiguous", "current", "pretraining_relevance"),
            ("ambiguous", "historical", "pretraining_relevance"),
        )
        for index, (lab_state, pretraining_state, retained_dimension) in enumerate(cases, start=1):
            with self.subTest(lab_state=lab_state, pretraining_state=pretraining_state):
                lead = _lead(
                    f"FxPartial{index:02d}",
                    lab_state=lab_state,
                    pretraining_state=pretraining_state,
                )
                lead["source_refs"] = [
                    source_ref
                    for source_ref in lead["source_refs"]
                    if retained_dimension in source_ref["support_dimensions"]
                ]
                payload = _result([lead])
                self.assertEqual(schema_errors(payload, schema), [])
                self.assertEqual(validate_compact_discovery_result(payload), [])

    def test_non_ambiguous_axes_each_require_corresponding_support(self) -> None:
        payload = _result([_lead("FxMissing001", lab_state="current", pretraining_state="current")])
        payload["leads"][0]["source_refs"] = payload["leads"][0]["source_refs"][:1]
        self.assertIn(
            "lead:0:source_dimension_coverage_invalid",
            validate_compact_discovery_result(payload),
        )

    def test_operator_summary_recomputes_leads_refs_surfaces_and_dimensions(self) -> None:
        first = _lead("FxSummary001", lab_state="current", pretraining_state="historical")
        second = _lead("FxSummary002", lab_state="historical", pretraining_state="ambiguous")
        second["source_refs"][1]["surface"] = "reply"
        payload = _result([first, second])

        summary = summarize_compact_discovery(payload)

        self.assertEqual(summary["unique_lead_count"], 2)
        self.assertEqual(summary["source_refs"]["total"], 4)
        self.assertEqual(summary["source_refs"]["by_surface"]["bio"], 2)
        self.assertEqual(summary["source_refs"]["by_surface"]["self_post"], 1)
        self.assertEqual(summary["source_refs"]["by_surface"]["reply"], 1)
        self.assertEqual(summary["source_refs"]["by_support_dimension"]["lab_affiliation"], 2)
        self.assertEqual(summary["source_refs"]["by_support_dimension"]["pretraining_relevance"], 2)
        self.assertEqual(summary["state_matrix"]["current"]["historical"], 1)
        self.assertEqual(summary["state_matrix"]["historical"]["ambiguous"], 1)
        serialized = json.dumps(summary, sort_keys=True)
        self.assertNotIn("FxSummary", serialized)
        self.assertNotIn("tool", serialized)
        self.assertEqual(set(summary["source_refs"]["by_surface"]), set(SOURCE_SURFACES))

    def test_comparison_is_casefolded_and_input_order_independent(self) -> None:
        expected = {
            "left_unique_leads": 3,
            "right_unique_leads": 3,
            "union": 4,
            "intersection": 2,
            "left_only": 1,
            "right_only": 1,
            "jaccard": 0.5,
        }
        self.assertEqual(
            compare_lead_sets(
                ["FxSetA001", "FxSetB001", "FxSetC001", "fxseta001"],
                ["FxSetD001", "fxsetc001", "FxSetB001"],
            ),
            expected,
        )
        self.assertEqual(
            compare_lead_sets(
                ["FxSetC001", "FxSetA001", "FxSetB001"],
                ["FxSetB001", "FxSetD001", "fxsetc001"],
            ),
            expected,
        )

        left = _result([_lead("FxSetA001"), _lead("FxSetB001"), _lead("FxSetC001")])
        right = _result([_lead("FxSetD001"), _lead("fxsetc001"), _lead("FxSetB001")])
        self.assertEqual(compare_compact_discovery_results(left, right), expected)

    def test_coverage_reason_and_source_dimension_formats_are_closed(self) -> None:
        bad_coverage = _result([_lead("FxClosed001")])
        bad_coverage["coverage_cells"].append("free_form_cell")
        self.assertIn("coverage_cells_invalid", validate_compact_discovery_result(bad_coverage))

        bad_reason = _result([_lead("FxClosed002")])
        bad_reason["leads"][0]["reason_codes"] = ["free_form_reason"]
        self.assertIn("lead:0:reason_codes_invalid", validate_compact_discovery_result(bad_reason))

        missing_dimension = _result([_lead("FxClosed003")])
        missing_dimension["leads"][0]["source_refs"] = missing_dimension["leads"][0]["source_refs"][:1]
        self.assertIn(
            "lead:0:source_dimension_coverage_invalid",
            validate_compact_discovery_result(missing_dimension),
        )

    def test_nullable_platform_user_id_and_non_mutating_validation(self) -> None:
        payload = _result([_lead("FxNullable001", platform_user_id=None)])
        before = copy.deepcopy(payload)
        self.assertEqual(validate_compact_discovery_result(payload), [])
        self.assertEqual(payload, before)

    def test_ok_status_rejects_any_uncovered_cell_or_limitation(self) -> None:
        uncovered = _result([_lead("FxStatus001")])
        uncovered["coverage_cells"] = list(COVERAGE_CELLS[:-1])
        uncovered["uncovered_cells"] = [COVERAGE_CELLS[-1]]
        self.assertIn("status_coverage_invalid", validate_compact_discovery_result(uncovered))

        limited = _result([_lead("FxStatus002")])
        limited["limitations"] = ["native_x_search_incomplete"]
        self.assertIn("status_coverage_invalid", validate_compact_discovery_result(limited))

    def test_operator_projection_removes_false_model_execution_claims(self) -> None:
        payload = _result([_lead("FxProject001")])
        payload.update(
            {
                "status": "X_DISCOVERY_PARTIAL",
                "limitations": [
                    "native_x_search_incomplete",
                    "result_truncated",
                    "execution_deadline_reached",
                ],
            }
        )
        before = copy.deepcopy(payload)

        projection = project_compact_execution_limitations(
            payload,
            result_truncated=False,
            execution_deadline_reached=False,
            transport_failure=False,
            model_output_repaired=False,
        )

        self.assertEqual(
            projection.result["limitations"],
            ["native_x_search_incomplete"],
        )
        self.assertEqual(projection.result["status"], "X_DISCOVERY_PARTIAL")
        self.assertEqual(
            projection.removed_model_operator_limitations,
            ("result_truncated", "execution_deadline_reached"),
        )
        self.assertEqual(projection.added_operator_limitations, ())
        self.assertEqual(validate_compact_discovery_result(projection.result), [])
        self.assertEqual(payload, before)

    def test_operator_projection_adds_true_receipt_facts_in_canonical_order(self) -> None:
        payload = _result([_lead("FxProject002")])

        projection = project_compact_execution_limitations(
            payload,
            result_truncated=True,
            execution_deadline_reached=False,
            transport_failure=True,
            model_output_repaired=True,
        )

        self.assertEqual(
            projection.result["limitations"],
            ["result_truncated", "transport_failure", "model_output_repaired"],
        )
        self.assertEqual(projection.result["status"], "X_DISCOVERY_PARTIAL")
        self.assertEqual(
            projection.added_operator_limitations,
            ("result_truncated", "transport_failure", "model_output_repaired"),
        )
        self.assertEqual(validate_compact_discovery_result(projection.result), [])

    def test_operator_projection_blocks_empty_result_on_hard_execution_failure(self) -> None:
        payload = _result([])

        projection = project_compact_execution_limitations(
            payload,
            result_truncated=False,
            execution_deadline_reached=True,
            transport_failure=False,
            model_output_repaired=False,
        )

        self.assertEqual(projection.result["status"], "X_DISCOVERY_BLOCKED")
        self.assertEqual(projection.result["limitations"], ["execution_deadline_reached"])
        self.assertEqual(validate_compact_discovery_result(projection.result), [])

    def test_operator_projection_can_upgrade_false_partial_to_ok(self) -> None:
        payload = _result([_lead("FxProject003")])
        payload.update(
            {
                "status": "X_DISCOVERY_PARTIAL",
                "limitations": ["result_truncated"],
            }
        )

        projection = project_compact_execution_limitations(
            payload,
            result_truncated=False,
            execution_deadline_reached=False,
            transport_failure=False,
            model_output_repaired=False,
        )

        self.assertEqual(projection.result["status"], "X_DISCOVERY_OK")
        self.assertEqual(projection.result["limitations"], [])
        self.assertEqual(validate_compact_discovery_result(projection.result), [])

    def test_operator_projection_rejects_non_boolean_operator_fact(self) -> None:
        with self.assertRaisesRegex(
            CompactDiscoveryContractError,
            "operator_fact_invalid:result_truncated",
        ):
            project_compact_execution_limitations(
                _result([_lead("FxProject004")]),
                result_truncated=1,  # type: ignore[arg-type]
                execution_deadline_reached=False,
                transport_failure=False,
                model_output_repaired=False,
            )

    def test_merge_three_shards_unions_more_than_one_hundred_leads_without_cap(self) -> None:
        shards = [
            _result([_lead(f"FxUnion{index:04d}") for index in range(start, stop)])
            for start, stop in ((0, 75), (50, 125), (100, 175))
        ]

        merged = merge_compact_discovery_results(shards)

        self.assertEqual(validate_compact_discovery_result(merged.result), [])
        self.assertEqual(merged.summary.input_result_count, 3)
        self.assertEqual(merged.summary.input_lead_count, 225)
        self.assertEqual(merged.summary.unique_lead_count, 175)
        self.assertEqual(merged.summary.overlapping_handle_count, 50)
        self.assertEqual(len(merged.result["leads"]), 175)

    def test_merge_casefolds_handles_and_reconciles_temporal_states(self) -> None:
        first = _lead(
            "FxMergeState01",
            lab_state="current",
            pretraining_state="historical",
        )
        second = _lead(
            "fxmergestate01",
            lab_state="ambiguous",
            pretraining_state="current",
        )

        merged = merge_compact_discovery_results([_result([first]), _result([second])])
        lead = merged.result["leads"][0]

        self.assertEqual(merged.summary.unique_lead_count, 1)
        self.assertEqual(merged.summary.overlapping_handle_count, 1)
        self.assertEqual(merged.summary.lab_affiliation_state_conflict_count, 0)
        self.assertEqual(merged.summary.pretraining_experience_state_conflict_count, 1)
        self.assertEqual(lead["handle"], "FxMergeState01")
        self.assertEqual(lead["target_lab_affiliation_state"], "current")
        self.assertEqual(lead["pretraining_experience_state"], "ambiguous")
        self.assertEqual(
            lead["reason_codes"],
            ["lab_affiliation_current_signal", "pretraining_relevance_ambiguous_signal"],
        )
        self.assertEqual(validate_compact_discovery_result(merged.result), [])

    def test_merge_counts_concrete_lab_state_conflict_separately(self) -> None:
        current = _lead("FxLabConflict01", lab_state="current")
        historical = _lead("fxlabconflict01", lab_state="historical")

        merged = merge_compact_discovery_results(
            [_result([current]), _result([historical])]
        )

        self.assertEqual(
            merged.result["leads"][0]["target_lab_affiliation_state"],
            "ambiguous",
        )
        self.assertEqual(merged.summary.lab_affiliation_state_conflict_count, 1)
        self.assertEqual(merged.summary.pretraining_experience_state_conflict_count, 0)
        self.assertEqual(validate_compact_discovery_result(merged.result), [])

    def test_merge_nulls_conflicting_platform_user_ids_and_counts_conflict(self) -> None:
        first = _result([_lead("FxIdConflict01", platform_user_id="91001")])
        second = _result([_lead("fxidconflict01", platform_user_id="91002")])

        merged = merge_compact_discovery_results([first, second])

        self.assertIsNone(merged.result["leads"][0]["platform_user_id"])
        self.assertEqual(merged.summary.platform_user_id_conflict_count, 1)
        self.assertEqual(validate_compact_discovery_result(merged.result), [])

    def test_merge_preserves_post_and_reply_refs_and_sorts_full_identity_key(self) -> None:
        first = _lead("FxMergeRefs01")
        second = _lead("fxmergerefs01")
        second["source_refs"][1]["support_dimensions"] = [
            "pretraining_relevance",
            "lab_affiliation",
        ]
        second["source_refs"].extend(
            [
                {
                    "surface": "reply",
                    "url": "https://x.com/fxmergerefs01/status/3000001",
                    "subject_handle": "fxmergerefs01",
                    "author_handle": "fxmergerefs01",
                    "support_dimensions": ["pretraining_relevance"],
                },
                {
                    "surface": "official_post",
                    "url": "https://x.com/GDMOfficial/status/3000002",
                    "subject_handle": "fxmergerefs01",
                    "author_handle": "GDMOfficial",
                    "support_dimensions": ["lab_affiliation"],
                },
            ]
        )
        self.assertEqual(validate_compact_discovery_result(_result([second])), [])
        inputs = [_result([first]), _result([second])]
        before = copy.deepcopy(inputs)

        merged = merge_compact_discovery_results(inputs)
        source_refs = merged.result["leads"][0]["source_refs"]

        self.assertEqual(
            [source_ref["surface"] for source_ref in source_refs],
            ["bio", "official_post", "reply", "self_post"],
        )
        self.assertEqual(
            source_refs[-1]["support_dimensions"],
            ["lab_affiliation", "pretraining_relevance"],
        )
        self.assertEqual(merged.summary.input_source_ref_count, 6)
        self.assertEqual(merged.summary.merged_source_ref_count, 4)
        self.assertEqual(validate_compact_discovery_result(merged.result), [])
        self.assertEqual(inputs, before)

    def test_merge_combines_coverage_limitations_and_is_input_order_independent(self) -> None:
        first = _result([_lead("FxCoverage01")])
        first.update(
            {
                "status": "X_DISCOVERY_PARTIAL",
                "coverage_cells": list(COVERAGE_CELLS[:4]),
                "uncovered_cells": list(COVERAGE_CELLS[4:]),
                "limitations": ["thread_hydration_incomplete"],
            }
        )
        second = _result([_lead("FxCoverage02")])
        second.update(
            {
                "status": "X_DISCOVERY_PARTIAL",
                "coverage_cells": list(COVERAGE_CELLS[4:]),
                "uncovered_cells": list(COVERAGE_CELLS[:4]),
                "limitations": ["native_x_search_incomplete"],
            }
        )
        strategy_id = "gdm.compact.three-shard-union-v1"

        forward = merge_compact_discovery_results(
            [first, second], strategy_id=strategy_id
        )
        reverse = merge_compact_discovery_results(
            [second, first], strategy_id=strategy_id
        )

        self.assertEqual(forward, reverse)
        self.assertEqual(forward.result["status"], "X_DISCOVERY_PARTIAL")
        self.assertEqual(forward.result["coverage_cells"], list(COVERAGE_CELLS))
        self.assertEqual(forward.result["uncovered_cells"], [])
        self.assertEqual(
            forward.result["limitations"],
            ["native_x_search_incomplete", "thread_hydration_incomplete"],
        )
        self.assertEqual(validate_compact_discovery_result(forward.result), [])

    def test_merge_validates_all_inputs_before_work_and_does_not_mutate_them(self) -> None:
        valid = _result([_lead("FxMergeValid01")])
        invalid = _result([_lead("FxMergeBad01")])
        invalid["leads"][0]["source_refs"][1]["subject_handle"] = "FxOtherBad01"
        before = copy.deepcopy([valid, invalid])

        with self.assertRaisesRegex(
            CompactDiscoveryContractError,
            r"merge_input:1:lead:0:source_ref:1:source_ref_subject_binding_invalid",
        ):
            merge_compact_discovery_results([valid, invalid])

        self.assertEqual([valid, invalid], before)

    def test_merge_rejects_empty_input(self) -> None:
        with self.assertRaisesRegex(CompactDiscoveryContractError, "merge_results_empty"):
            merge_compact_discovery_results([])


if __name__ == "__main__":
    unittest.main()
