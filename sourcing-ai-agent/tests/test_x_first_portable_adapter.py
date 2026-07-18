from __future__ import annotations

import copy
import inspect
import json
import unittest
from pathlib import Path

import sourcing_agent.x_first_portable_adapter as adapter
from sourcing_agent.x_first_portable_adapter import (
    ProjectionSelectionMemberSnapshot,
    XFirstPortableAdapterError,
    build_subject_selection_artifact,
    build_verification_import_preview,
    validate_subject_selection_artifact,
)
from sourcing_agent.x_first_portable_package import (
    ExpectedSelectionSnapshot,
    XFirstPortablePackageError,
    validate_x_first_portable_package,
)

ROOT = Path(__file__).resolve().parents[1]


def _trusted_package() -> dict:
    return json.loads(
        (ROOT / "tests/fixtures/x_first/selected_subject_fixture_simulate_package_v1.json").read_text()
    )


def _expected_snapshot(package: dict) -> ExpectedSelectionSnapshot:
    selection = package["artifacts"]["selection"]
    snapshot = selection["snapshot"]
    return ExpectedSelectionSnapshot(
        workspace_ref=snapshot["workspace_ref"],
        projection_ref=snapshot["projection_ref"],
        membership_revision=snapshot["membership_revision"],
        selection_artifact_sha256=selection["artifact_sha256"],
    )


def _bound_target() -> dict:
    return {
        "workspace_id": "workspace_fixture",
        "projection_id": "projection_fixture",
        "membership_revision": "revision_fixture_1",
        "source_candidate_count": 12,
        "candidate_identity_keys": ["linkedin:ada", "linkedin:grace"],
    }


def _members() -> list[ProjectionSelectionMemberSnapshot]:
    grace_summary = {
        "display_name": "Grace Example",
        "headline": "Research Engineer",
        "current_company": "Example AI",
        "experience_lines": ["2022~Present, Example AI, Research Engineer"],
        "education_lines": ["MS, Example University"],
        "primary_email": "must-not-cross@example.invalid",
        "raw_profile": {"must": "not cross"},
    }
    ada_summary = {
        "display_name": "Ada Example",
        "headline": "Pre-training Researcher",
        "current_company": "Example AI",
        "location": "San Francisco, CA",
        "linkedin_url": "https://www.linkedin.com/in/ada-example/",
        "summary": "Works on model training and evaluation.",
    }
    return [
        ProjectionSelectionMemberSnapshot(
            workspace_id="workspace_fixture",
            projection_id="projection_fixture",
            membership_revision="revision_fixture_1",
            candidate_identity_key="linkedin:grace",
            owner_row_sha256="1" * 64,
            public_summary_sha256=adapter.canonical_sha256(grace_summary),
            public_summary=grace_summary,
        ),
        ProjectionSelectionMemberSnapshot(
            workspace_id="workspace_fixture",
            projection_id="projection_fixture",
            membership_revision="revision_fixture_1",
            candidate_identity_key="linkedin:ada",
            owner_row_sha256="2" * 64,
            public_summary_sha256=adapter.canonical_sha256(ada_summary),
            public_summary=ada_summary,
        ),
    ]


def _selection() -> dict:
    return build_subject_selection_artifact(
        bound_target_ref=_bound_target(),
        members=_members(),
        exported_at="2026-07-18T04:00:00Z",
        x_handle_proposals_by_candidate={"linkedin:ada": ["ada_ai"]},
    )


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
        self.assertEqual(subjects["linkedin:grace"]["source_kind"], "professional_profile")
        self.assertIsNone(subjects["linkedin:grace"]["source_profile_url"])
        self.assertEqual(subjects["linkedin:grace"]["source_record_sha256"], "1" * 64)
        serialized = adapter.canonical_json(selection)
        self.assertNotIn("primary_email", serialized)
        self.assertNotIn("must-not-cross", serialized)

    def test_selection_rejects_stale_or_tampered_member_binding(self) -> None:
        selection = _selection()
        tampered = copy.deepcopy(selection)
        tampered["subjects"][0]["name_text"] = "Changed"
        tampered["artifact_sha256"] = adapter._content_sha256(tampered, "artifact_sha256")
        with self.assertRaisesRegex(XFirstPortableAdapterError, "exported_seed_hash_mismatch"):
            validate_subject_selection_artifact(tampered)

        target = _bound_target()
        target["candidate_identity_keys"] = ["linkedin:ada", "linkedin:missing"]
        with self.assertRaisesRegex(XFirstPortableAdapterError, "member_set_mismatch"):
            build_subject_selection_artifact(
                bound_target_ref=target,
                members=[_members()[1]],
                exported_at="2026-07-18T04:00:00Z",
            )

    def test_no_profile_url_with_handle_is_not_mislabeled_name_only(self) -> None:
        summary = {"display_name": "Handle Only Example"}
        member = ProjectionSelectionMemberSnapshot(
            workspace_id="workspace_fixture",
            projection_id="projection_fixture",
            membership_revision="revision_fixture_1",
            candidate_identity_key="linkedin:ada",
            owner_row_sha256="a" * 64,
            public_summary_sha256=adapter.canonical_sha256(summary),
            public_summary=summary,
        )
        artifact = build_subject_selection_artifact(
            bound_target_ref={
                **_bound_target(),
                "candidate_identity_keys": ["linkedin:ada"],
            },
            members=[member],
            exported_at="2026-07-18T04:00:00Z",
            x_handle_proposals_by_candidate={"linkedin:ada": ["handle_only"]},
        )

        self.assertEqual(artifact["subjects"][0]["source_kind"], "professional_profile")
        self.assertEqual(
            artifact["subjects"][0]["exported_seed_sha256"],
            adapter.canonical_sha256(adapter._portable_seed_from_subject(artifact["subjects"][0])),
        )

        stale_member = _members()[0]
        stale_member = ProjectionSelectionMemberSnapshot(
            workspace_id=stale_member.workspace_id,
            projection_id=stale_member.projection_id,
            membership_revision="revision_fixture_0",
            candidate_identity_key=stale_member.candidate_identity_key,
            owner_row_sha256=stale_member.owner_row_sha256,
            public_summary_sha256=stale_member.public_summary_sha256,
            public_summary=stale_member.public_summary,
        )
        with self.assertRaisesRegex(XFirstPortableAdapterError, "member_invalid"):
            build_subject_selection_artifact(
                bound_target_ref={
                    **_bound_target(),
                    "candidate_identity_keys": ["linkedin:grace"],
                },
                members=[stale_member],
                exported_at="2026-07-18T04:00:00Z",
            )

        changed_summary = dict(_members()[0].public_summary)
        changed_summary["headline"] = "Changed without owner digest"
        mismatched = ProjectionSelectionMemberSnapshot(
            workspace_id="workspace_fixture",
            projection_id="projection_fixture",
            membership_revision="revision_fixture_1",
            candidate_identity_key="linkedin:grace",
            owner_row_sha256="1" * 64,
            public_summary_sha256=_members()[0].public_summary_sha256,
            public_summary=changed_summary,
        )
        with self.assertRaisesRegex(XFirstPortableAdapterError, "member_invalid"):
            build_subject_selection_artifact(
                bound_target_ref={
                    **_bound_target(),
                    "candidate_identity_keys": ["linkedin:grace"],
                },
                members=[mismatched],
                exported_at="2026-07-18T04:00:00Z",
            )

    def test_import_preview_roundtrips_subjects_without_materializing_product_state(self) -> None:
        package = _trusted_package()
        validated = validate_x_first_portable_package(
            package,
            fixture_id="x_first_selected_people_fixture_v1",
            expected_snapshot=_expected_snapshot(package),
        )
        preview = build_verification_import_preview(validated_package=validated)

        rows = {row["source_record_ref"]: row for row in preview["subjects"]}
        analyzed = rows["fixture://product/snapshot/candidate-1"]
        in_progress = rows["fixture://product/snapshot/candidate-2"]
        unresolved = rows["fixture://product/snapshot/candidate-3"]
        self.assertEqual(analyzed["review_state"], "identity_review_required")
        self.assertEqual(
            analyzed["observation_refs"],
            ["obs_selected_x_post", "obs_selected_x_reply"],
        )
        self.assertTrue(
            all(
                row["source_status"] == "fixture_synthetic"
                for row in analyzed["observation_provenance"]
            )
        )
        self.assertEqual(
            analyzed["handle_resolution_provenance"][0]["source_status"],
            "fixture_synthetic",
        )
        self.assertEqual(
            analyzed["verification_summaries"][0]["source_status"],
            "fixture_synthetic",
        )
        self.assertEqual(in_progress["terminal_state"], "research_in_progress")
        self.assertEqual(in_progress["review_state"], "research_continuation_required")
        self.assertEqual(in_progress["verification_summaries"], [])
        self.assertEqual(unresolved["review_state"], "handle_resolution_required")
        self.assertFalse(preview["authority"]["product_writes_allowed"])
        self.assertFalse(preview["authority"]["automatic_cross_source_merge_allowed"])

    def test_import_preview_requires_internal_validated_package_capability(self) -> None:
        with self.assertRaisesRegex(
            XFirstPortablePackageError, "validated_package_capability_required"
        ):
            build_verification_import_preview(validated_package=_trusted_package())

    def test_adapter_has_no_x_first_runtime_import(self) -> None:
        source = inspect.getsource(adapter)
        self.assertNotIn("from x_first", source)
        self.assertNotIn("import x_first", source)


if __name__ == "__main__":
    unittest.main()
