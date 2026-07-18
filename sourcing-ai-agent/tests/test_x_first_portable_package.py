from __future__ import annotations

import copy
import json
import unittest
from pathlib import Path
from unittest import mock

import sourcing_agent.x_first_portable_package as package_module
from sourcing_agent.x_first_portable_package import (
    ExpectedSelectionSnapshot,
    XFirstPortablePackageError,
    validate_x_first_portable_package,
)

ROOT = Path(__file__).resolve().parents[1]
X_FIRST_ROOT = ROOT.parent / "x-first-researcher-sourcing"
FIXTURE_ID = "x_first_selected_people_fixture_v1"


def _package() -> dict:
    return json.loads(
        (ROOT / "tests/fixtures/x_first/selected_subject_fixture_simulate_package_v1.json").read_text()
    )


def _expected_snapshot(value: dict) -> ExpectedSelectionSnapshot:
    selection = value["artifacts"]["selection"]
    snapshot = selection["snapshot"]
    return ExpectedSelectionSnapshot(
        workspace_ref=snapshot["workspace_ref"],
        projection_ref=snapshot["projection_ref"],
        membership_revision=snapshot["membership_revision"],
        selection_artifact_sha256=selection["artifact_sha256"],
    )


class XFirstPortablePackageTests(unittest.TestCase):
    def test_trusted_fixture_package_validates_to_internal_capability(self) -> None:
        value = _package()

        validated = validate_x_first_portable_package(
            value,
            fixture_id=FIXTURE_ID,
            expected_snapshot=_expected_snapshot(value),
        )

        self.assertEqual(validated.trusted_fixture_id, FIXTURE_ID)
        self.assertEqual(validated.manifest["package_mode"], "fixture_simulate")
        self.assertEqual(
            validated.semantic_validation_receipt["effects"],
            {
                "simulated_external_attempt_count": 4,
                "simulated_external_evidence_count": 6,
                "provider_call_count": 0,
                "model_call_count": 0,
                "product_write_count": 0,
            },
        )

    def test_vendored_artifact_schemas_are_byte_identical_to_x_first_owner(self) -> None:
        for contract in package_module._ARTIFACT_CONTRACTS.values():
            with self.subTest(schema_version=contract.schema_version):
                product_bytes = (ROOT / contract.schema_path).read_bytes()
                x_first_bytes = (
                    X_FIRST_ROOT / "contracts" / contract.schema_path.name
                ).read_bytes()
                self.assertEqual(product_bytes, x_first_bytes)
        for filename in (
            "x.portable.research_campaign.package_manifest.v1.schema.json",
            "x.portable.research_campaign.semantic_validation_receipt.v1.schema.json",
        ):
            with self.subTest(filename=filename):
                self.assertEqual(
                    (ROOT / "contracts/external/x_first" / filename).read_bytes(),
                    (X_FIRST_ROOT / "contracts" / filename).read_bytes(),
                )

    def test_any_vendored_schema_byte_drift_fails_closed(self) -> None:
        value = _package()
        original_read_bytes = Path.read_bytes
        result_path = (
            ROOT
            / "contracts/external/x_first/x.portable.research_campaign.result.v1.schema.json"
        )

        def drift_one_schema(path: Path) -> bytes:
            payload = original_read_bytes(path)
            return payload + b"\n" if path == result_path else payload

        with mock.patch.object(Path, "read_bytes", autospec=True, side_effect=drift_one_schema):
            with self.assertRaisesRegex(
                XFirstPortablePackageError, "result_local_schema_mismatch"
            ):
                validate_x_first_portable_package(
                    value,
                    fixture_id=FIXTURE_ID,
                    expected_snapshot=_expected_snapshot(value),
                )

    def test_registry_not_caller_rehash_rejects_mutated_artifact_graph(self) -> None:
        value = _package()
        mutated = copy.deepcopy(value)
        mutated["artifacts"]["result"]["limitations"].append("caller changed result")
        result = mutated["artifacts"]["result"]
        result["result_sha256"] = package_module._content_sha256(result, "result_sha256")
        descriptor = mutated["manifest"]["artifacts"]["result"]
        descriptor["payload_sha256"] = package_module.canonical_sha256(result)
        descriptor["declared_content_sha256"] = result["result_sha256"]
        mutated["manifest"]["manifest_sha256"] = package_module._content_sha256(
            mutated["manifest"], "manifest_sha256"
        )
        receipt = mutated["semantic_validation_receipt"]
        receipt["manifest_sha256"] = mutated["manifest"]["manifest_sha256"]
        receipt["artifact_payload_sha256s"]["result"] = descriptor["payload_sha256"]
        receipt["receipt_sha256"] = package_module._content_sha256(receipt, "receipt_sha256")

        with self.assertRaisesRegex(XFirstPortablePackageError, "semantic_receipt_untrusted"):
            validate_x_first_portable_package(
                mutated,
                fixture_id=FIXTURE_ID,
                expected_snapshot=_expected_snapshot(mutated),
            )

    def test_snapshot_revision_and_fixture_id_are_product_owned(self) -> None:
        value = _package()
        stale = _expected_snapshot(value)
        stale = ExpectedSelectionSnapshot(
            workspace_ref=stale.workspace_ref,
            projection_ref=stale.projection_ref,
            membership_revision="stale-revision",
            selection_artifact_sha256=stale.selection_artifact_sha256,
        )
        with self.assertRaisesRegex(XFirstPortablePackageError, "selection_snapshot_rebound"):
            validate_x_first_portable_package(
                value,
                fixture_id=FIXTURE_ID,
                expected_snapshot=stale,
            )
        with self.assertRaisesRegex(XFirstPortablePackageError, "fixture_pin_missing_or_ambiguous"):
            validate_x_first_portable_package(
                value,
                fixture_id="caller_supplied_fixture",
                expected_snapshot=_expected_snapshot(value),
            )

    def test_nonfixture_handle_and_optional_evidence_are_rejected_by_fixture_scan(self) -> None:
        value = _package()
        result = copy.deepcopy(value["artifacts"]["result"])
        result["handle_resolution_evidence"][0]["source_status"] = "receipt_bound"
        with self.assertRaisesRegex(XFirstPortablePackageError, "fixture_execution_invalid"):
            package_module._validate_fixture_execution(result)

        result = copy.deepcopy(value["artifacts"]["result"])
        result["optional_channel_evidence"] = [
            {"source_status": "unverified", "receipt_ref": "fixture://receipt/not-enough"}
        ]
        with self.assertRaisesRegex(XFirstPortablePackageError, "fixture_execution_invalid"):
            package_module._validate_fixture_execution(result)


if __name__ == "__main__":
    unittest.main()
