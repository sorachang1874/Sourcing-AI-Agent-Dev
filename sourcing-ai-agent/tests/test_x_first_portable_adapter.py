from __future__ import annotations

import copy
import inspect
import unittest

import sourcing_agent.x_first_portable_adapter as adapter
from sourcing_agent.x_first_portable_adapter import (
    XFirstPortableAdapterError,
    build_subject_selection_artifact,
    build_verification_import_preview,
    validate_subject_selection_artifact,
)


def _bound_target() -> dict:
    return {
        "workspace_id": "workspace_fixture",
        "projection_id": "projection_fixture",
        "membership_revision": "revision_fixture_1",
        "source_candidate_count": 12,
        "candidate_identity_keys": ["linkedin:ada", "linkedin:grace"],
    }


def _members() -> list[dict]:
    return [
        {
            "projection_id": "projection_fixture",
            "candidate_identity_key": "linkedin:grace",
            "public_summary": {
                "display_name": "Grace Example",
                "headline": "Research Engineer",
                "current_company": "Example AI",
                "experience_lines": ["2022~Present, Example AI, Research Engineer"],
                "education_lines": ["MS, Example University"],
                "primary_email": "must-not-cross@example.invalid",
                "raw_profile": {"must": "not cross"},
            },
        },
        {
            "projection_id": "projection_fixture",
            "candidate_identity_key": "linkedin:ada",
            "public_summary": {
                "display_name": "Ada Example",
                "headline": "Pre-training Researcher",
                "current_company": "Example AI",
                "location": "San Francisco, CA",
                "linkedin_url": "https://www.linkedin.com/in/ada-example/",
                "summary": "Works on model training and evaluation.",
            },
        },
    ]


def _selection() -> dict:
    return build_subject_selection_artifact(
        bound_target_ref=_bound_target(),
        members=_members(),
        exported_at="2026-07-18T04:00:00Z",
        x_handle_proposals_by_candidate={"linkedin:ada": ["ada_ai"]},
    )


def _binding(selection: dict) -> dict:
    def portable_seed(subject: dict) -> dict:
        return {
            "seed_ref": subject["source_subject_ref"],
            "source_kind": subject["source_kind"],
            "external_record_ref": subject["source_record_ref"],
            "source_record_sha256": subject["source_record_sha256"],
            "source_status": subject["source_status"],
            "source_profile_url": subject["source_profile_url"],
            "name_text": subject["name_text"],
            "x_handle_proposals": subject["x_handle_proposals"],
            "professional_facts": subject["professional_facts"],
        }

    record = {
        "schema_version": adapter.REQUEST_BINDING_SCHEMA_VERSION,
        "selection_id": selection["selection_id"],
        "campaign_id": "campaign_fixture",
        "selection_schema_version": adapter.SELECTION_SCHEMA_VERSION,
        "selection_contract_schema_sha256": adapter.selection_contract_schema_sha256(),
        "selection_artifact_sha256": selection["artifact_sha256"],
        "portable_request_schema_version": adapter.PORTABLE_REQUEST_SCHEMA_VERSION,
        "portable_request_contract_schema_sha256": "a" * 64,
        "portable_request_sha256": "b" * 64,
        "subject_bindings": [
            {
                "source_subject_ref": subject["source_subject_ref"],
                "source_record_ref": subject["source_record_ref"],
                "seed_ref": subject["source_subject_ref"],
                "source_record_sha256": subject["source_record_sha256"],
                "source_kind": subject["source_kind"],
                "seed_sha256": adapter.canonical_sha256(portable_seed(subject)),
            }
            for subject in selection["subjects"]
        ],
        "authority": {
            "provider_calls_allowed": False,
            "product_writes_allowed": False,
            "canonical_person_merge_allowed": False,
            "outreach_allowed": False,
        },
        "binding_sha256": "",
    }
    record["binding_sha256"] = adapter._content_sha256(record, "binding_sha256")
    return record


def _result(selection: dict, binding: dict) -> dict:
    subject_by_source = {subject["source_record_ref"]: subject for subject in selection["subjects"]}
    ada_ref = subject_by_source["linkedin:ada"]["source_subject_ref"]
    grace_ref = subject_by_source["linkedin:grace"]["source_subject_ref"]
    record = {
        "schema_version": adapter.PORTABLE_RESULT_SCHEMA_VERSION,
        "campaign_id": "campaign_fixture",
        "request_sha256": binding["portable_request_sha256"],
        "plan_sha256": "c" * 64,
        "catalog_sha256": "d" * 64,
        "execution_window": {
            "started_at": "2026-07-18T04:01:00Z",
            "completed_at": "2026-07-18T04:02:00Z",
        },
        "status": "partial",
        "subject_outcomes": [
            {
                "seed_ref": ada_ref,
                "terminal_state": "analyzed",
                "x_account_refs": ["xacct_ada"],
                "reason": "fixture account resolved",
            },
            {
                "seed_ref": grace_ref,
                "terminal_state": "handle_resolution_required",
                "x_account_refs": [],
                "reason": "fixture handle missing",
            },
        ],
        "external_accounts": [
            {
                "x_account_ref": "xacct_ada",
                "platform_user_id": "1001",
                "current_handle": "ada_ai",
                "profile_url": "https://x.com/ada_ai",
                "identity_status": "stable_platform_id",
                "handle_history_proposals": [],
            }
        ],
        "cross_source_link_proposals": [
            {
                "proposal_id": "link_ada",
                "seed_ref": ada_ref,
                "x_account_ref": "xacct_ada",
                "status": "proposed",
                "evidence_refs": [ada_ref],
                "source_status": "source_bound",
                "human_review_required": True,
                "automatic_merge_authorized": False,
            }
        ],
        "discovery_origins": [],
        "surface_attempts": [],
        "semantic_recall_attempts": [],
        "semantic_recall_outcomes": [],
        "observations": [
            {
                "observation_id": "obs_ada_pretraining",
                "x_account_ref": "xacct_ada",
                "surface": "post",
            }
        ],
        "affiliation_results": [],
        "dimension_results": [
            {
                "x_account_ref": "xacct_ada",
                "question_id": "verify_training",
                "dimension_id": "research_workstream",
                "matched_label_ids": ["pretraining"],
                "relevance_state": "target_core",
                "target_activity_temporal_state": "historical",
                "evidence_refs": ["obs_ada_pretraining"],
            }
        ],
        "exploratory_findings": [],
        "experience_verification_queue": [],
        "optional_channel_attempts": [],
        "optional_channel_outcomes": [],
        "relationship_results": [],
        "coverage": {},
        "limitations": [],
        "authority": {
            "product_writes_allowed": False,
            "canonical_person_write_allowed": False,
            "automatic_cross_source_merge_allowed": False,
            "outreach_allowed": False,
        },
        "result_sha256": "",
    }
    record["result_sha256"] = adapter._content_sha256(record, "result_sha256")
    return record


class XFirstPortableAdapterTests(unittest.TestCase):
    def test_selected_projection_snapshot_exports_only_allowlisted_seed_context(self) -> None:
        selection = _selection()

        validate_subject_selection_artifact(selection)
        self.assertEqual(
            adapter.selection_contract_schema_sha256(),
            adapter.SELECTION_CONTRACT_SCHEMA_SHA256,
        )
        self.assertEqual(
            adapter.request_binding_contract_schema_sha256(),
            adapter.REQUEST_BINDING_CONTRACT_SCHEMA_SHA256,
        )
        self.assertEqual(
            adapter.import_preview_contract_schema_sha256(),
            adapter.IMPORT_PREVIEW_CONTRACT_SCHEMA_SHA256,
        )
        self.assertEqual(selection["snapshot"]["selected_candidate_count"], 2)
        self.assertFalse(selection["authority"]["provider_calls_allowed"])
        subjects = {row["source_record_ref"]: row for row in selection["subjects"]}
        self.assertEqual(subjects["linkedin:ada"]["source_kind"], "linkedin_profile")
        self.assertEqual(subjects["linkedin:ada"]["x_handle_proposals"][0]["handle"], "ada_ai")
        self.assertEqual(subjects["linkedin:grace"]["source_kind"], "name_only")
        serialized = adapter.canonical_json(selection)
        self.assertNotIn("primary_email", serialized)
        self.assertNotIn("must-not-cross", serialized)

    def test_selection_rejects_stale_or_tampered_member_binding(self) -> None:
        selection = _selection()
        tampered = copy.deepcopy(selection)
        tampered["subjects"][0]["name_text"] = "Changed"
        tampered["artifact_sha256"] = adapter._content_sha256(tampered, "artifact_sha256")
        with self.assertRaisesRegex(XFirstPortableAdapterError, "source_record_hash_mismatch"):
            validate_subject_selection_artifact(tampered)

        target = _bound_target()
        target["candidate_identity_keys"] = ["linkedin:ada", "linkedin:missing"]
        with self.assertRaisesRegex(XFirstPortableAdapterError, "member_set_mismatch"):
            build_subject_selection_artifact(
                bound_target_ref=target,
                members=[_members()[1]],
                exported_at="2026-07-18T04:00:00Z",
            )

    def test_import_preview_roundtrips_subjects_without_materializing_product_state(self) -> None:
        selection = _selection()
        binding = _binding(selection)
        result = _result(selection, binding)

        preview = build_verification_import_preview(
            selection=selection,
            request_binding=binding,
            portable_result=result,
        )

        rows = {row["source_record_ref"]: row for row in preview["subjects"]}
        self.assertEqual(rows["linkedin:ada"]["review_state"], "identity_review_required")
        self.assertEqual(rows["linkedin:ada"]["observation_refs"], ["obs_ada_pretraining"])
        self.assertEqual(
            rows["linkedin:ada"]["verification_summaries"][0]["matched_label_ids"],
            ["pretraining"],
        )
        self.assertEqual(rows["linkedin:grace"]["review_state"], "handle_resolution_required")
        self.assertFalse(preview["authority"]["product_writes_allowed"])
        self.assertFalse(preview["authority"]["automatic_cross_source_merge_allowed"])

    def test_import_preview_fails_closed_on_request_rebinding(self) -> None:
        selection = _selection()
        binding = _binding(selection)
        result = _result(selection, binding)
        result["request_sha256"] = "e" * 64
        result["result_sha256"] = adapter._content_sha256(result, "result_sha256")

        with self.assertRaisesRegex(XFirstPortableAdapterError, "result_request_mismatch"):
            build_verification_import_preview(
                selection=selection,
                request_binding=binding,
                portable_result=result,
            )

    def test_adapter_has_no_x_first_runtime_import(self) -> None:
        source = inspect.getsource(adapter)
        self.assertNotIn("from x_first", source)
        self.assertNotIn("import x_first", source)


if __name__ == "__main__":
    unittest.main()
