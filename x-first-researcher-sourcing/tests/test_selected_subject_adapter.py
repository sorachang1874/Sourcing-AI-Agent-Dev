from __future__ import annotations

import copy
import inspect
import unittest
from collections.abc import Callable

import x_first.selected_subject_adapter as adapter
from x_first.recall_pool_schema import contract_schema_sha256
from x_first.research_orchestration import (
    REQUEST_SCHEMA_FILE,
    build_campaign_plan,
    canonical_sha256,
    load_policy,
    project_root,
    strict_load_json,
)
from x_first.selected_subject_adapter import (
    SELECTION_SCHEMA_FILE,
    SelectedSubjectAdapterError,
    build_selected_subject_request_binding,
    selected_member_keys_sha256,
    validate_request_binding,
    validate_subject_selection,
)

ROOT = project_root()


def _content_sha256(value: dict, field: str) -> str:
    return canonical_sha256({key: item for key, item in value.items() if key != field})


def _selection() -> dict:
    subjects = [
        {
            "source_subject_ref": "selected_linkedin_subject",
            "source_record_ref": "fixture://product/snapshot/candidate-1",
            "source_record_sha256": "1" * 64,
            "source_public_summary_sha256": "3" * 64,
            "exported_seed_sha256": "",
            "source_status": "source_bound",
            "source_kind": "linkedin_profile",
            "source_profile_url": "https://www.linkedin.com/in/selected-researcher",
            "name_text": "Selected Researcher",
            "x_handle_proposals": [
                {
                    "handle": "selected_x",
                    "binding_status": "cross_source_link_proposed",
                    "evidence_ref": "fixture://product/snapshot/candidate-1#x-handle",
                }
            ],
            "professional_facts": [
                {
                    "fact_type": "affiliation",
                    "value": "Fixture AI Lab",
                    "temporal_state": "current",
                    "evidence_ref": "fixture://product/snapshot/candidate-1#affiliation",
                }
            ],
        },
        {
            "source_subject_ref": "selected_in_progress_subject",
            "source_record_ref": "fixture://product/snapshot/candidate-2",
            "source_record_sha256": "2" * 64,
            "source_public_summary_sha256": "4" * 64,
            "exported_seed_sha256": "",
            "source_status": "source_bound",
            "source_kind": "professional_profile",
            "source_profile_url": "https://example.com/selected-in-progress",
            "name_text": "Selected In Progress",
            "x_handle_proposals": [
                {
                    "handle": "progress_x",
                    "binding_status": "cross_source_link_proposed",
                    "evidence_ref": "fixture://product/snapshot/candidate-2#x-handle",
                }
            ],
            "professional_facts": [
                {
                    "fact_type": "role",
                    "value": "Research Engineer",
                    "temporal_state": "current",
                    "evidence_ref": "fixture://product/snapshot/candidate-2#role",
                }
            ],
        },
        {
            "source_subject_ref": "selected_name_only_subject",
            "source_record_ref": "fixture://product/snapshot/candidate-3",
            "source_record_sha256": "5" * 64,
            "source_public_summary_sha256": "6" * 64,
            "exported_seed_sha256": "",
            "source_status": "source_bound",
            "source_kind": "name_only",
            "source_profile_url": None,
            "name_text": "Selected Name Only",
            "x_handle_proposals": [],
            "professional_facts": [],
        },
    ]
    for subject in subjects:
        subject["exported_seed_sha256"] = canonical_sha256(adapter._portable_seed(subject))
    selection = {
        "schema_version": "sourcing.x_first.subject_selection.v1",
        "selection_id": "fixture_selected_subjects",
        "exported_at": "2026-07-18T07:00:00Z",
        "snapshot": {
            "workspace_ref": "fixture://product/workspace",
            "projection_ref": "fixture://product/projection/rev-7",
            "membership_revision": "7",
            "source_candidate_count": 10,
            "selected_candidate_count": len(subjects),
            "selected_member_keys_sha256": selected_member_keys_sha256(subjects),
        },
        "subjects": subjects,
        "authority": {
            "provider_calls_allowed": False,
            "product_writes_allowed": False,
            "canonical_person_merge_allowed": False,
            "outreach_allowed": False,
        },
        "contract_schema_sha256": contract_schema_sha256(SELECTION_SCHEMA_FILE),
        "artifact_sha256": "",
    }
    selection["artifact_sha256"] = _content_sha256(selection, "artifact_sha256")
    return selection


def _campaign_intent() -> dict:
    request = strict_load_json(ROOT / "fixtures" / "portable_research_campaign_request_fixture_v1.json")
    intent = {
        key: copy.deepcopy(value)
        for key, value in request.items()
        if key not in {"seed_inputs", "authority", "request_sha256"}
    }
    verification = next(
        row for row in intent["analysis_questions"] if row["analysis_mode"] == "verification"
    )
    verification["recall_execution_policy"] = "authored_surface_only"
    verification["adaptive_stop_policy"] = None
    intent["analysis_questions"] = [verification]
    intent["channel_overrides"] = []
    return intent


class SelectedSubjectAdapterTests(unittest.TestCase):
    def setUp(self) -> None:
        self.selection = _selection()
        self.catalog = strict_load_json(ROOT / "fixtures" / "research_scope_catalog_fixture_v1.json")
        self.policy = load_policy()
        self.intent = _campaign_intent()

    def _build(self) -> tuple[dict, dict]:
        return build_selected_subject_request_binding(
            selection=self.selection,
            campaign_intent=self.intent,
            catalog=self.catalog,
            policy=self.policy,
        )

    def test_round_trip_builds_authored_tasks_and_one_resolution_row(self) -> None:
        request, binding = self._build()
        validate_subject_selection(self.selection)
        validate_request_binding(binding, selection=self.selection, request=request)
        self.assertEqual(
            contract_schema_sha256(SELECTION_SCHEMA_FILE),
            adapter.SELECTION_CONTRACT_SCHEMA_SHA256,
        )
        self.assertEqual(
            contract_schema_sha256(adapter.BINDING_SCHEMA_FILE),
            adapter.BINDING_CONTRACT_SCHEMA_SHA256,
        )

        self.assertEqual(
            [row["seed_ref"] for row in request["seed_inputs"]],
            [
                "selected_in_progress_subject",
                "selected_linkedin_subject",
                "selected_name_only_subject",
            ],
        )
        self.assertTrue(all(row["source_status"] == "source_bound" for row in request["seed_inputs"]))
        self.assertEqual(
            binding["portable_request_contract_schema_sha256"],
            contract_schema_sha256(REQUEST_SCHEMA_FILE),
        )
        self.assertEqual(len(binding["subject_bindings"]), 3)

        plan = build_campaign_plan(request=request, catalog=self.catalog, policy=self.policy)
        self.assertEqual(
            [row["handle"] for row in plan["candidate_authored_tasks"]],
            ["progress_x", "selected_x"],
        )
        self.assertEqual(
            [row["seed_ref"] for row in plan["handle_resolution_queue"]],
            ["selected_name_only_subject"],
        )
        question = plan["analysis_contract"]["questions"][0]
        self.assertEqual(question["recall_execution_policy"], "authored_surface_only")
        self.assertFalse(plan["integration_boundary"]["runtime_import_allowed"])

    def test_selection_schema_hash_count_member_and_duplicate_tampering_fail_closed(self) -> None:
        cases: list[tuple[str, Callable[[dict], None], str]] = [
            (
                "contract_schema_sha",
                lambda value: value.__setitem__("contract_schema_sha256", "0" * 64),
                "subject_selection_contract_schema_sha256_mismatch",
            ),
            (
                "artifact_sha",
                lambda value: value.__setitem__("artifact_sha256", "0" * 64),
                "subject_selection_artifact_sha256_mismatch",
            ),
            (
                "count",
                lambda value: value["snapshot"].__setitem__("selected_candidate_count", 4),
                "subject_selection_count_invalid",
            ),
            (
                "member_set",
                lambda value: value["snapshot"].__setitem__("selected_member_keys_sha256", "0" * 64),
                "subject_selection_member_set_sha256_mismatch",
            ),
            (
                "subject_ref_duplicate",
                lambda value: value["subjects"][1].__setitem__(
                    "source_subject_ref", value["subjects"][0]["source_subject_ref"]
                ),
                "subject_selection_subject_ref_duplicate",
            ),
            (
                "record_ref_duplicate",
                lambda value: value["subjects"][1].__setitem__(
                    "source_record_ref", value["subjects"][0]["source_record_ref"]
                ),
                "subject_selection_record_ref_duplicate",
            ),
            (
                "source_record_hash",
                lambda value: value["subjects"][0].__setitem__("name_text", "Rebound Subject"),
                "subject_selection_exported_seed_sha256_mismatch",
            ),
        ]
        for name, mutate, expected in cases:
            with self.subTest(name=name):
                selection = copy.deepcopy(self.selection)
                mutate(selection)
                if name not in {"contract_schema_sha", "artifact_sha"}:
                    selection["artifact_sha256"] = _content_sha256(selection, "artifact_sha256")
                with self.assertRaisesRegex(SelectedSubjectAdapterError, expected):
                    validate_subject_selection(selection)

        selection = copy.deepcopy(self.selection)
        selection["schema_version"] = "sourcing.x_first.subject_selection.v2"
        selection["artifact_sha256"] = _content_sha256(selection, "artifact_sha256")
        with self.assertRaisesRegex(SelectedSubjectAdapterError, "subject_selection_schema_invalid"):
            validate_subject_selection(selection)

    def test_request_and_binding_tampering_fail_closed(self) -> None:
        request, binding = self._build()

        changed_request = copy.deepcopy(request)
        changed_request["seed_inputs"][0]["name_text"] = "Rebound Subject"
        changed_request["request_sha256"] = _content_sha256(changed_request, "request_sha256")
        with self.assertRaisesRegex(SelectedSubjectAdapterError, "selected_subject_seed_mapping_mismatch"):
            validate_request_binding(binding, selection=self.selection, request=changed_request)

        changed_binding = copy.deepcopy(binding)
        changed_binding["portable_request_contract_schema_sha256"] = "0" * 64
        changed_binding["binding_sha256"] = _content_sha256(changed_binding, "binding_sha256")
        with self.assertRaisesRegex(SelectedSubjectAdapterError, "selected_subject_binding_mismatch"):
            validate_request_binding(changed_binding, selection=self.selection, request=request)

        changed_binding = copy.deepcopy(binding)
        changed_binding["binding_sha256"] = "0" * 64
        with self.assertRaisesRegex(SelectedSubjectAdapterError, "selected_subject_binding_sha256_mismatch"):
            validate_request_binding(changed_binding, selection=self.selection, request=request)

    def test_adapter_has_no_product_runtime_import(self) -> None:
        source = inspect.getsource(adapter)
        self.assertNotIn("import sourcing_agent", source)
        self.assertNotIn("from sourcing_agent", source)


if __name__ == "__main__":
    unittest.main()
