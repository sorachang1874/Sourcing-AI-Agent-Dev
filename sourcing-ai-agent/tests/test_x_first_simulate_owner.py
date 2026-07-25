from __future__ import annotations

import copy
import json
import unittest
from pathlib import Path

from sourcing_agent.x_first_portable_package import ExpectedSelectionSnapshot
from sourcing_agent.x_first_simulate_owner import (
    XFirstSimulateOwnerError,
    run_x_first_package_simulation,
    validate_x_first_simulate_owner_result,
)

ROOT = Path(__file__).resolve().parents[1]
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


class XFirstSimulateOwnerTests(unittest.TestCase):
    def test_fixture_owner_returns_deterministic_effect_free_preview(self) -> None:
        value = _package()

        first = run_x_first_package_simulation(
            value,
            fixture_id=FIXTURE_ID,
            expected_snapshot=_expected_snapshot(value),
        )
        second = run_x_first_package_simulation(
            value,
            fixture_id=FIXTURE_ID,
            expected_snapshot=_expected_snapshot(value),
        )

        self.assertEqual(first, second)
        self.assertIsNotNone(first.import_preview)
        self.assertEqual(first.owner_result["simulation_state"], "preview_ready")
        self.assertEqual(
            first.owner_result["subject_terminal_counts"],
            {
                "selected": 3,
                "analyzed": 1,
                "research_in_progress": 1,
                "handle_resolution_required": 1,
                "no_verified_account": 0,
                "failed": 0,
            },
        )
        self.assertFalse(first.owner_result["served"])
        self.assertTrue(all(value == 0 for value in first.owner_result["effects"].values()))
        validate_x_first_simulate_owner_result(first.owner_result)

    def test_invalid_package_terminalizes_as_typed_rejection_with_zero_effects(self) -> None:
        value = _package()
        value["artifacts"]["result"]["limitations"].append("untrusted mutation")

        run = run_x_first_package_simulation(
            value,
            fixture_id=FIXTURE_ID,
            expected_snapshot=_expected_snapshot(value),
        )

        self.assertIsNone(run.import_preview)
        self.assertEqual(run.owner_result["simulation_state"], "rejected")
        self.assertEqual(
            run.owner_result["state_trace"],
            ["received", "validating_package", "rejected"],
        )
        self.assertEqual(run.owner_result["error_code"], "x_first_package_result_descriptor_invalid")
        self.assertTrue(all(value == 0 for value in run.owner_result["effects"].values()))
        self.assertTrue(
            all(value == 0 for value in run.owner_result["subject_terminal_counts"].values())
        )
        validate_x_first_simulate_owner_result(run.owner_result)

    def test_owner_result_rejects_count_or_hash_tampering(self) -> None:
        value = _package()
        run = run_x_first_package_simulation(
            value,
            fixture_id=FIXTURE_ID,
            expected_snapshot=_expected_snapshot(value),
        )
        record = copy.deepcopy(run.owner_result)
        record["subject_terminal_counts"]["research_in_progress"] = 0
        with self.assertRaisesRegex(XFirstSimulateOwnerError, "result_invalid"):
            validate_x_first_simulate_owner_result(record)


if __name__ == "__main__":
    unittest.main()
