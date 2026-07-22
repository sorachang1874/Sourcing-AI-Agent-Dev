"""mypy ratchet gate logic (harness R0 batch, 2026-07-22).

Why: the '87 pre-existing errors' backend-ci comment silently drifted to 81
with no tracking artifact (fixes landed incidentally in 82d69a1) — exactly the
budget-drift class the residual-ledger ratchet rule exists for. These tests pin
the comparator semantics offline (no mypy run): regressions fail, improvements
fail until the committed baseline shrinks, cleared families fail until removed.
Contract doc: configs/mypy_ratchet_baseline.json _meta.policy; master plan WS4.
"""

import json
import sys
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "scripts"))

from check_mypy_ratchet import ERROR_LINE, compare  # noqa: E402


class RatchetComparatorTest(unittest.TestCase):
    BASELINE = {"src/a.py": {"assignment": 2, "arg-type": 1}}

    def test_exact_match_passes(self) -> None:
        self.assertEqual(compare(self.BASELINE, {"src/a.py": {"assignment": 2, "arg-type": 1}}), [])

    def test_regression_fails(self) -> None:
        violations = compare(self.BASELINE, {"src/a.py": {"assignment": 3, "arg-type": 1}})
        self.assertEqual(len(violations), 1)
        self.assertIn("REGRESSION", violations[0])

    def test_new_file_code_key_fails(self) -> None:
        violations = compare(self.BASELINE, {"src/a.py": {"assignment": 2, "arg-type": 1}, "src/b.py": {"index": 1}})
        self.assertTrue(any("REGRESSION src/b.py [index]" in v for v in violations))

    def test_improvement_without_baseline_shrink_fails_with_instruction(self) -> None:
        violations = compare(self.BASELINE, {"src/a.py": {"assignment": 1, "arg-type": 1}})
        self.assertEqual(len(violations), 1)
        self.assertIn("RATCHET", violations[0])
        self.assertIn("shrink", violations[0])

    def test_cleared_error_family_requires_baseline_removal(self) -> None:
        violations = compare(self.BASELINE, {"src/a.py": {"assignment": 2}})
        self.assertTrue(any("[arg-type]: 0 < budget 1" in v for v in violations))


class BaselineShapeTest(unittest.TestCase):
    def test_committed_baseline_parses_and_totals_match(self) -> None:
        doc = json.loads((REPO_ROOT / "configs" / "mypy_ratchet_baseline.json").read_text(encoding="utf-8"))
        total = sum(v for codes in doc["budgets"].values() for v in codes.values())
        self.assertEqual(total, doc["_meta"]["total_errors"])
        self.assertTrue(doc["_meta"]["tool_mypy_config_sha256_16"])

    def test_error_line_regex_matches_mypy_output_shape(self) -> None:
        line = "src/sourcing_agent/orchestrator.py:30770: error: Unsupported operand types [operator]"
        match = ERROR_LINE.search(line)
        assert match is not None
        self.assertEqual(match.group(2), "operator")


if __name__ == "__main__":
    unittest.main()
