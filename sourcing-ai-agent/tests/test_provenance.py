"""Test-provenance gate (contract: docs/governance/TEST_PROVENANCE.md, 2026-07-22).

Why: only 6% of test files carried any provenance marker at the 69b7423 census —
pitfalls became tests without recording WHY, making retirement decisions
forensic work (R-034 took a multi-hour dig). This gate enforces anchors on NEW
test modules only, ratchets the frozen grandfather baseline downward, and makes
deletion of a grandfathered file require a REGRESSION_INDEX tombstone.
"""

import sys
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "scripts"))

from check_test_provenance import TESTS_DIR, file_has_anchor  # noqa: E402
from tests.provenance_baseline import GRANDFATHERED, MAX_GRANDFATHERED  # noqa: E402


class NewTestModulesCarryAnchorsTest(unittest.TestCase):
    def test_every_non_grandfathered_test_module_has_a_provenance_anchor(self) -> None:
        missing = [
            path.name
            for path in sorted(TESTS_DIR.glob("test_*.py"))
            if path.name not in GRANDFATHERED and not file_has_anchor(path)
        ]
        self.assertEqual(
            missing,
            [],
            "New/changed test modules need a provenance anchor in the module or a "
            "top-level class docstring (date, R-id, docs path, or milestone id — "
            "see docs/governance/TEST_PROVENANCE.md). Honest fallback: "
            "'provenance unknown; characterization adopted YYYY-MM-DD'.",
        )


class GrandfatherRatchetTest(unittest.TestCase):
    def test_baseline_only_shrinks(self) -> None:
        self.assertLessEqual(
            len(GRANDFATHERED),
            MAX_GRANDFATHERED,
            "GRANDFATHERED grew — never add entries; new tests must carry anchors.",
        )

    def test_compliant_files_leave_the_baseline_in_the_same_change(self) -> None:
        now_compliant = sorted(
            name
            for name in GRANDFATHERED
            if (TESTS_DIR / name).is_file() and file_has_anchor(TESTS_DIR / name)
        )
        self.assertEqual(
            now_compliant,
            [],
            "These grandfathered files now carry anchors — remove them from "
            "tests/provenance_baseline.py and decrement MAX_GRANDFATHERED in this "
            "same change (shrink-only ratchet).",
        )

    def test_deleting_a_grandfathered_file_requires_a_tombstone(self) -> None:
        index_text = (
            REPO_ROOT / "docs" / "governance" / "REGRESSION_INDEX.md"
        ).read_text(encoding="utf-8")
        tombstones = index_text[index_text.index("## Tombstones") :]
        orphaned = sorted(
            name
            for name in GRANDFATHERED
            if not (TESTS_DIR / name).is_file() and name not in tombstones
        )
        self.assertEqual(
            orphaned,
            [],
            "Grandfathered test files were deleted without a Tombstones row in "
            "docs/governance/REGRESSION_INDEX.md — record what they protected and "
            "what superseded them, then remove them from the baseline.",
        )


if __name__ == "__main__":
    unittest.main()
