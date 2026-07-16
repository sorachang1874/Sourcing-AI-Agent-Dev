from __future__ import annotations

import copy
import json
import unittest
from dataclasses import replace
from itertools import product
from pathlib import Path

from x_first.compact_grok_discovery import (
    CONTRACT_VERSION,
    COVERAGE_CELLS,
    SOURCE_SURFACES,
    CompactDiscoveryContractError,
    CompactDiscoveryExecutionReceipt,
    canonical_json_sha256,
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
AGGREGATE_RECEIPT_PATH = (
    PROJECT_ROOT
    / "docs"
    / "live-evidence"
    / "2026-07-16-gdm-compact-strategy-matrix.aggregate-receipt.v1.json"
)
TARGET_DIGEST = "1" * 64
PROMPT_DIGEST = "2" * 64
TRANSCRIPT_DIGEST = "3" * 64


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
    shard_id: str = "fixture.shard-a",
) -> dict[str, object]:
    numeric_suffix = "".join(character for character in handle if character.isdigit()) or "1"
    post_id = str(1_000_000 + int(numeric_suffix))
    return {
        "handle": handle,
        "profile_url": f"https://x.com/{handle}",
        "platform_user_id": platform_user_id,
        "identity_status": "stable_platform_id" if platform_user_id else "provisional_handle",
        "handle_history_proposals": [
            {
                "handle": handle,
                "profile_url": f"https://x.com/{handle}",
                "source_status": "model_mediated_unverified",
                "origin_shard_ids": [shard_id],
            }
        ],
        "origin_shard_ids": [shard_id],
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
                "origin_shard_ids": [shard_id],
            },
            {
                "surface": "self_post",
                "url": f"https://x.com/{handle}/status/{post_id}",
                "subject_handle": handle,
                "author_handle": handle,
                "support_dimensions": ["pretraining_relevance"],
                "origin_shard_ids": [shard_id],
            },
        ],
        "reason_codes": _reason_codes(lab_state, pretraining_state),
    }


def _result(
    leads: list[dict[str, object]],
    *,
    shard_id: str = "fixture.shard-a",
    campaign_id: str = "fixture.gdm-campaign-v1",
    target_digest: str = TARGET_DIGEST,
) -> dict[str, object]:
    normalized = copy.deepcopy(leads)
    for lead in normalized:
        lead["origin_shard_ids"] = [shard_id]
        for proposal in lead["handle_history_proposals"]:
            proposal["origin_shard_ids"] = [shard_id]
        for source_ref in lead["source_refs"]:
            source_ref["origin_shard_ids"] = [shard_id]
    return {
        "contract_version": CONTRACT_VERSION,
        "campaign_id": campaign_id,
        "target_descriptor_id": "fixture.gdm-target-v1",
        "target_descriptor_sha256": target_digest,
        "prompt_policy_sha256": PROMPT_DIGEST,
        "result_kind": "shard",
        "shard_id": shard_id,
        "input_shards": [],
        "status": "X_DISCOVERY_OK",
        "strategy_id": "gdm.compact.discovery-v1",
        "leads": normalized,
        "coverage_cells": list(COVERAGE_CELLS),
        "uncovered_cells": [],
        "limitations": [],
    }


def _projection(result: dict[str, object], **facts: bool):
    receipt = CompactDiscoveryExecutionReceipt(
        receipt_version="x.grok.compact_discovery.execution_receipt.v1",
        campaign_id=result["campaign_id"],
        target_descriptor_id=result["target_descriptor_id"],
        target_descriptor_sha256=result["target_descriptor_sha256"],
        prompt_policy_sha256=result["prompt_policy_sha256"],
        shard_id=result["shard_id"],
        session_id=f"session:{result['shard_id']}",
        transcript_sha256=TRANSCRIPT_DIGEST,
        terminal_sha256=canonical_json_sha256(result),
        terminal_selected_after_last_tool_completion=True,
        result_truncated=facts.get("result_truncated", False),
        execution_deadline_reached=facts.get("execution_deadline_reached", False),
        transport_failure=facts.get("transport_failure", False),
        model_output_repaired=facts.get("model_output_repaired", False),
    )
    return project_compact_execution_limitations(result, receipt=receipt)


class CompactGrokDiscoveryContractTests(unittest.TestCase):
    def test_schema_is_closed_bound_and_has_no_business_array_cap(self) -> None:
        schema = json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))
        self.assertEqual(set(schema["required"]), set(_result([])))
        self.assertFalse(schema["additionalProperties"])
        self.assertNotIn("maxItems", schema["properties"]["leads"])
        self.assertNotIn("maxItems", schema["$defs"]["lead"]["properties"]["source_refs"])
        self.assertIn("origin_shard_ids", schema["$defs"]["source_ref"]["required"])
        self.assertIn("handle_history_proposals", schema["$defs"]["lead"]["required"])

    def test_live_diagnostic_aggregate_receipt_is_hash_bound_and_arithmetically_closed(self) -> None:
        receipt = json.loads(AGGREGATE_RECEIPT_PATH.read_text(encoding="utf-8"))
        self.assertEqual(receipt["claim_status"], "diagnostic_only")
        self.assertEqual(receipt["privacy_class"], "candidate_free_aggregate")
        bindings = receipt["source_artifact_bindings"]
        self.assertEqual(len(bindings), 6)
        self.assertTrue(
            all(
                len(binding["sha256"]) == 64
                and set(binding["sha256"]) <= set("0123456789abcdef")
                for binding in bindings
            )
        )
        hydration = receipt["aggregate_metrics"]["profile_hydration"]
        self.assertEqual(hydration["input_count"], 93)
        self.assertEqual(
            hydration["exact_match_count"] + hydration["not_found_count"],
            hydration["tool_evidence_compliant_input_count"],
        )
        self.assertEqual(
            receipt["aggregate_metrics"]["discovery"]["population_exhaustion_proven"],
            False,
        )

    def test_typical_payload_matches_schema_and_runtime(self) -> None:
        payload = _result([_lead("FxSchema001", platform_user_id="99001")])
        schema = json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))
        self.assertEqual(schema_errors(payload, schema), [])
        self.assertEqual(validate_compact_discovery_result(payload), [])

    def test_more_than_one_hundred_leads_and_refs_have_no_business_cap(self) -> None:
        payload = _result(
            [_lead(f"FxLead{index:04d}", platform_user_id=str(50_000 + index)) for index in range(137)]
        )
        self.assertEqual(validate_compact_discovery_result(payload), [])
        self.assertEqual(summarize_compact_discovery(payload)["unique_lead_count"], 137)
        lead = _lead("FxManyRefs001")
        lead["source_refs"].extend(
            {
                "surface": "self_post",
                "url": f"https://x.com/FxManyRefs001/status/{2_000_000 + index}",
                "subject_handle": "FxManyRefs001",
                "author_handle": "FxManyRefs001",
                "support_dimensions": ["pretraining_relevance"],
                "origin_shard_ids": ["fixture.shard-a"],
            }
            for index in range(121)
        )
        many_refs = _result([lead])
        self.assertEqual(validate_compact_discovery_result(many_refs), [])
        self.assertEqual(summarize_compact_discovery(many_refs)["source_refs"]["total"], 123)

    def test_campaign_and_target_binding_fields_fail_closed(self) -> None:
        for field, value in (
            ("campaign_id", "INVALID CAMPAIGN"),
            ("target_descriptor_id", "INVALID TARGET"),
            ("target_descriptor_sha256", "short"),
            ("prompt_policy_sha256", "short"),
            ("contract_version", "wrong"),
        ):
            with self.subTest(field=field):
                payload = _result([_lead("FxBind001")])
                payload[field] = value
                self.assertTrue(validate_compact_discovery_result(payload))

    def test_profile_source_and_origin_bindings_fail_closed(self) -> None:
        payload = _result([_lead("FxBind002")])
        payload["leads"][0]["source_refs"][1]["subject_handle"] = "FxOther002"
        self.assertIn(
            "lead:0:source_ref:1:source_ref_subject_binding_invalid",
            validate_compact_discovery_result(payload),
        )
        origin = _result([_lead("FxBind003")])
        origin["leads"][0]["source_refs"][0]["origin_shard_ids"] = ["fixture.other"]
        self.assertIn(
            "lead:0:source_ref:0:source_ref_origin_binding_invalid",
            validate_compact_discovery_result(origin),
        )

    def test_root_placeholder_casefold_duplicate_and_status_author_fail_closed(self) -> None:
        for invalid_url in (
            "https://x.com/",
            "https://x.com/search",
            "https://x.com/placeholder",
        ):
            with self.subTest(invalid_url=invalid_url):
                payload = _result([_lead("FxRoute001")])
                payload["leads"][0]["profile_url"] = invalid_url
                self.assertTrue(validate_compact_discovery_result(payload))

        duplicates = _result([_lead("FxCase001"), _lead("fxcase001")])
        self.assertIn(
            "lead:1:provisional_handle_duplicate",
            validate_compact_discovery_result(duplicates),
        )
        bad_author = _result([_lead("FxAuthor001")])
        bad_author["leads"][0]["source_refs"][1]["author_handle"] = "FxOther001"
        self.assertIn(
            "lead:0:source_ref:1:source_ref_status_binding_invalid",
            validate_compact_discovery_result(bad_author),
        )

    def test_identity_binding_is_case_insensitive_but_canonical_url_shape_is_strict(self) -> None:
        payload = _result([_lead("FxMixedCase01")])
        lead = payload["leads"][0]
        lead["profile_url"] = "https://x.com/fxmixedcase01"
        lead["handle_history_proposals"][0].update(
            {"handle": "FXMIXEDCASE01", "profile_url": "https://x.com/FXMIXEDCASE01"}
        )
        lead["source_refs"][0].update(
            {
                "url": "https://x.com/fxmixedcase01",
                "subject_handle": "FXMIXEDCASE01",
                "author_handle": "fxmixedcase01",
            }
        )
        lead["source_refs"][1].update(
            {
                "url": "https://x.com/fxmixedcase01/status/1000001",
                "subject_handle": "fxmixedcase01",
                "author_handle": "FXMIXEDCASE01",
            }
        )
        self.assertEqual(validate_compact_discovery_result(payload), [])

    def test_closed_coverage_reason_status_and_partition_contracts(self) -> None:
        bad_coverage = _result([_lead("FxClosed001")])
        bad_coverage["coverage_cells"].append("free_form_cell")
        self.assertIn("coverage_cells_invalid", validate_compact_discovery_result(bad_coverage))
        bad_reason = _result([_lead("FxClosed002")])
        bad_reason["leads"][0]["reason_codes"] = ["free_form_reason"]
        self.assertIn("lead:0:reason_codes_invalid", validate_compact_discovery_result(bad_reason))
        bad_status = _result([_lead("FxClosed003")])
        bad_status["limitations"] = ["native_x_search_incomplete"]
        self.assertIn("status_coverage_invalid", validate_compact_discovery_result(bad_status))
        overlap = _result([_lead("FxClosed004")])
        overlap["uncovered_cells"] = [COVERAGE_CELLS[0]]
        self.assertIn("coverage_cells_overlap", validate_compact_discovery_result(overlap))

    def test_all_temporal_combinations_require_both_evidence_dimensions(self) -> None:
        leads = [
            _lead(f"FxState{index:02d}", lab_state=lab, pretraining_state=pre)
            for index, (lab, pre) in enumerate(product(("current", "historical", "ambiguous"), repeat=2), 1)
        ]
        payload = _result(leads)
        self.assertEqual(validate_compact_discovery_result(payload), [])
        ambiguous = _result([_lead("FxUnknown001", pretraining_state="ambiguous")])
        ambiguous["leads"][0]["source_refs"] = ambiguous["leads"][0]["source_refs"][:1]
        self.assertIn(
            "lead:0:source_dimension_coverage_invalid",
            validate_compact_discovery_result(ambiguous),
        )

    def test_operator_projection_normalizes_before_status_coherence(self) -> None:
        payload = _result([_lead("FxProject001")])
        payload["status"] = "X_DISCOVERY_OK"
        payload["limitations"] = ["result_truncated"]
        self.assertIn("status_coverage_invalid", validate_compact_discovery_result(payload))
        projection = _projection(payload)
        self.assertEqual(projection.result["status"], "X_DISCOVERY_OK")
        self.assertEqual(projection.result["limitations"], [])
        self.assertEqual(projection.removed_model_operator_limitations, ("result_truncated",))

    def test_operator_projection_requires_terminal_and_receipt_binding(self) -> None:
        payload = _result([_lead("FxProject002")])
        receipt = CompactDiscoveryExecutionReceipt(
            receipt_version="x.grok.compact_discovery.execution_receipt.v1",
            campaign_id="fixture.other-campaign",
            target_descriptor_id=payload["target_descriptor_id"],
            target_descriptor_sha256=payload["target_descriptor_sha256"],
            prompt_policy_sha256=payload["prompt_policy_sha256"],
            shard_id=payload["shard_id"],
            session_id="session:bad",
            transcript_sha256=TRANSCRIPT_DIGEST,
            terminal_sha256=canonical_json_sha256(payload),
            terminal_selected_after_last_tool_completion=True,
            result_truncated=False,
            execution_deadline_reached=False,
            transport_failure=False,
            model_output_repaired=False,
        )
        with self.assertRaisesRegex(CompactDiscoveryContractError, "campaign_id_mismatch"):
            project_compact_execution_limitations(payload, receipt=receipt)
        receipt = replace(receipt, campaign_id=payload["campaign_id"], terminal_sha256="4" * 64)
        with self.assertRaisesRegex(CompactDiscoveryContractError, "terminal_sha256_mismatch"):
            project_compact_execution_limitations(payload, receipt=receipt)

    def test_operator_projection_adds_true_facts_and_blocks_empty_hard_failure(self) -> None:
        payload = _result([_lead("FxProject003")])
        projection = _projection(
            payload,
            result_truncated=True,
            transport_failure=True,
            model_output_repaired=True,
        )
        self.assertEqual(
            projection.result["limitations"],
            ["result_truncated", "transport_failure", "model_output_repaired"],
        )
        self.assertEqual(projection.result["status"], "X_DISCOVERY_PARTIAL")
        empty = _projection(_result([]), execution_deadline_reached=True)
        self.assertEqual(empty.result["status"], "X_DISCOVERY_BLOCKED")

    def test_merge_rejects_raw_mapping_and_forged_projection_digest(self) -> None:
        payload = _result([_lead("FxEnvelope001")])
        with self.assertRaisesRegex(CompactDiscoveryContractError, "merge_projection_required"):
            merge_compact_discovery_results([payload])  # type: ignore[list-item]
        projection = _projection(payload)
        forged = replace(projection, projected_result_sha256="f" * 64)
        with self.assertRaisesRegex(CompactDiscoveryContractError, "result_digest_mismatch"):
            merge_compact_discovery_results([forged])

    def test_merge_rejects_cross_campaign_descriptor_and_duplicate_shard(self) -> None:
        first = _projection(_result([_lead("FxCampaign01")], shard_id="fixture.shard-a"))
        cross_campaign = _projection(
            _result(
                [_lead("FxCampaign02")],
                shard_id="fixture.shard-b",
                campaign_id="fixture.other-campaign",
            )
        )
        with self.assertRaisesRegex(CompactDiscoveryContractError, "campaign_binding_mismatch:campaign_id"):
            merge_compact_discovery_results([first, cross_campaign])
        cross_descriptor = _projection(
            _result(
                [_lead("FxCampaign04")],
                shard_id="fixture.shard-c",
                target_digest="a" * 64,
            )
        )
        with self.assertRaisesRegex(
            CompactDiscoveryContractError,
            "campaign_binding_mismatch:target_descriptor_sha256",
        ):
            merge_compact_discovery_results([first, cross_descriptor])
        duplicate = _projection(_result([_lead("FxCampaign03")], shard_id="fixture.shard-a"))
        with self.assertRaisesRegex(CompactDiscoveryContractError, "merge_shard_id_duplicate"):
            merge_compact_discovery_results([first, duplicate])

    def test_platform_id_first_merge_reconciles_handle_rename(self) -> None:
        first = _projection(
            _result([_lead("FxOldHandle01", platform_user_id="91001")], shard_id="fixture.shard-a")
        )
        second = _projection(
            _result([_lead("FxNewHandle01", platform_user_id="91001")], shard_id="fixture.shard-b")
        )
        merged = merge_compact_discovery_results([first, second])
        self.assertEqual(merged.summary.unique_lead_count, 1)
        self.assertEqual(merged.summary.renamed_stable_identity_count, 1)
        lead = merged.result["leads"][0]
        self.assertEqual(lead["platform_user_id"], "91001")
        self.assertEqual(lead["identity_status"], "stable_platform_id")
        self.assertEqual(
            {item["handle"] for item in lead["handle_history_proposals"]},
            {"FxOldHandle01", "FxNewHandle01"},
        )
        self.assertEqual(lead["origin_shard_ids"], ["fixture.shard-a", "fixture.shard-b"])
        self.assertEqual(validate_compact_discovery_result(merged.result), [])

    def test_recycled_handle_is_quarantined_without_evidence_merge(self) -> None:
        first = _projection(
            _result([_lead("FxReused001", platform_user_id="92001")], shard_id="fixture.shard-a")
        )
        second_lead = _lead("fxreused001", platform_user_id="92002")
        second_lead["source_refs"][1]["url"] = "https://x.com/fxreused001/status/3999999"
        second = _projection(_result([second_lead], shard_id="fixture.shard-b"))
        merged = merge_compact_discovery_results([first, second])
        self.assertEqual(merged.summary.unique_lead_count, 2)
        self.assertEqual(merged.summary.platform_user_id_conflict_count, 1)
        self.assertEqual(merged.summary.quarantined_handle_reuse_identity_count, 2)
        self.assertEqual(
            {lead["platform_user_id"] for lead in merged.result["leads"]},
            {"92001", "92002"},
        )
        self.assertTrue(
            all(lead["identity_status"] == "quarantined_handle_reuse" for lead in merged.result["leads"])
        )
        self.assertTrue(all(len(lead["source_refs"]) == 2 for lead in merged.result["leads"]))
        self.assertEqual(validate_compact_discovery_result(merged.result), [])

    def test_missing_id_remains_separate_explicit_provisional_identity(self) -> None:
        stable = _projection(
            _result([_lead("FxMaybeSame01", platform_user_id="93001")], shard_id="fixture.shard-a")
        )
        provisional = _projection(
            _result([_lead("fxmaybesame01")], shard_id="fixture.shard-b")
        )
        merged = merge_compact_discovery_results([stable, provisional])
        self.assertEqual(merged.summary.unique_lead_count, 2)
        self.assertEqual(merged.summary.provisional_identity_count, 1)
        self.assertEqual(
            {lead["identity_status"] for lead in merged.result["leads"]},
            {"stable_platform_id", "provisional_handle"},
        )

    def test_merge_persists_input_digests_and_lead_ref_membership(self) -> None:
        first = _projection(
            _result([_lead("FxAudit001", platform_user_id="94001")], shard_id="fixture.shard-a")
        )
        second = _projection(
            _result([_lead("fxaudit001", platform_user_id="94001")], shard_id="fixture.shard-b")
        )
        merged = merge_compact_discovery_results([second, first], union_id="fixture.union-v1")
        self.assertEqual(
            merged.result["input_shards"],
            [
                {"shard_id": "fixture.shard-a", "projected_result_sha256": first.projected_result_sha256},
                {"shard_id": "fixture.shard-b", "projected_result_sha256": second.projected_result_sha256},
            ],
        )
        lead = merged.result["leads"][0]
        self.assertEqual(lead["origin_shard_ids"], ["fixture.shard-a", "fixture.shard-b"])
        self.assertTrue(all(source["origin_shard_ids"] for source in lead["source_refs"]))
        self.assertEqual(validate_compact_discovery_result(merged.result), [])
        schema = json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))
        self.assertEqual(schema_errors(merged.result, schema), [])

    def test_merge_preserves_post_reply_and_official_reference_surfaces(self) -> None:
        first = _lead("FxRefs001", platform_user_id="94501")
        second = _lead("fxrefs001", platform_user_id="94501")
        second["source_refs"].extend(
            [
                {
                    "surface": "reply",
                    "url": "https://x.com/fxrefs001/status/3000001",
                    "subject_handle": "fxrefs001",
                    "author_handle": "fxrefs001",
                    "support_dimensions": ["pretraining_relevance"],
                    "origin_shard_ids": ["fixture.shard-b"],
                },
                {
                    "surface": "official_post",
                    "url": "https://x.com/FxOfficial/status/3000002",
                    "subject_handle": "fxrefs001",
                    "author_handle": "FxOfficial",
                    "support_dimensions": ["lab_affiliation"],
                    "origin_shard_ids": ["fixture.shard-b"],
                },
            ]
        )
        merged = merge_compact_discovery_results(
            [
                _projection(_result([first], shard_id="fixture.shard-a")),
                _projection(_result([second], shard_id="fixture.shard-b")),
            ]
        )
        self.assertEqual(
            {source["surface"] for source in merged.result["leads"][0]["source_refs"]},
            {"bio", "self_post", "reply", "official_post"},
        )

    def test_merge_temporal_conflicts_and_limitations_are_deterministic(self) -> None:
        first_result = _result(
            [_lead("FxStateMerge1", lab_state="current", pretraining_state="historical", platform_user_id="95001")],
            shard_id="fixture.shard-a",
        )
        first_result["status"] = "X_DISCOVERY_PARTIAL"
        first_result["limitations"] = ["thread_hydration_incomplete"]
        second_result = _result(
            [_lead("fxstatemerge1", lab_state="historical", pretraining_state="current", platform_user_id="95001")],
            shard_id="fixture.shard-b",
        )
        second_result["status"] = "X_DISCOVERY_PARTIAL"
        second_result["limitations"] = ["native_x_search_incomplete"]
        first = _projection(first_result)
        second = _projection(second_result)
        forward = merge_compact_discovery_results([first, second])
        reverse = merge_compact_discovery_results([second, first])
        self.assertEqual(forward, reverse)
        lead = forward.result["leads"][0]
        self.assertEqual(lead["target_lab_affiliation_state"], "ambiguous")
        self.assertEqual(lead["pretraining_experience_state"], "ambiguous")
        self.assertEqual(forward.summary.lab_affiliation_state_conflict_count, 1)
        self.assertEqual(forward.summary.pretraining_experience_state_conflict_count, 1)

    def test_summary_and_comparison_remain_candidate_free_and_bound(self) -> None:
        payload = _result([_lead("FxSummary001")])
        summary = summarize_compact_discovery(payload)
        self.assertNotIn("FxSummary001", json.dumps(summary))
        self.assertEqual(set(summary["source_refs"]["by_surface"]), set(SOURCE_SURFACES))
        self.assertEqual(
            compare_lead_sets(["FxA001", "FxB001"], ["fxa001", "FxC001"])["intersection"],
            1,
        )
        other = _result([_lead("FxOther001")], campaign_id="fixture.other-campaign")
        with self.assertRaisesRegex(CompactDiscoveryContractError, "comparison_campaign_binding_mismatch"):
            compare_compact_discovery_results(payload, other)

    def test_canonical_profile_url_and_input_non_mutation(self) -> None:
        payload = _result([_lead("FxNoMutation1")])
        before = copy.deepcopy(payload)
        projection = _projection(payload)
        self.assertEqual(payload, before)
        self.assertEqual(canonical_profile_url("FxNoMutation1"), "https://x.com/FxNoMutation1")
        self.assertEqual(validate_compact_discovery_result(projection.result), [])


if __name__ == "__main__":
    unittest.main()
