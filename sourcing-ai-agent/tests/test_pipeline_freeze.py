"""test_pipeline.py freeze ratchet (WS3 Tier 3, operator decision 2026-07-22).

Why: tests/test_pipeline.py (42,416 lines / 461 tests) is the R-009 god-file —
zero PG fixtures, 31 dead store._lock/_connection whitebox references, never
runnable as a full module (100-connection PG exhaustion). The ratified
disposition is salvage-then-DELETE: port still-valid clusters to
module-boundary files (with old->new provenance in
docs/governance/REGRESSION_INDEX.md), then delete the file and close R-009.
This gate freezes the file meanwhile: no new tests may be added, and every
ported/deleted cluster must shrink the ratchet literals in the same change.
"""

import re
import unittest
from pathlib import Path

PIPELINE = Path(__file__).resolve().parent / "test_pipeline.py"

# Frozen at the 2026-07-22 salvage-decision baseline. Shrink-only.
MAX_TESTS = 83
MAX_LINES = 8365


class PipelineFreezeTest(unittest.TestCase):
    def test_no_new_tests_enter_the_frozen_god_file(self) -> None:
        if not PIPELINE.is_file():
            # Salvage complete: the file is deleted (R-009 closed). This gate
            # then guards against resurrection.
            return
        text = PIPELINE.read_text(encoding="utf-8")
        test_count = len(re.findall(r"^\s+def test_", text, re.M))
        line_count = text.count("\n")  # wc -l semantics
        self.assertLessEqual(
            test_count,
            MAX_TESTS,
            "test_pipeline.py is FROZEN (salvage-then-delete, master plan WS3 "
            "Tier 3): write new tests in the owning module's suite instead.",
        )
        self.assertLessEqual(
            line_count,
            MAX_LINES,
            "test_pipeline.py is FROZEN: the file may only shrink.",
        )

    def test_ported_clusters_shrink_the_ratchet(self) -> None:
        if not PIPELINE.is_file():
            return
        text = PIPELINE.read_text(encoding="utf-8")
        test_count = len(re.findall(r"^\s+def test_", text, re.M))
        self.assertGreaterEqual(
            test_count,
            MAX_TESTS,
            "Tests left test_pipeline.py — shrink MAX_TESTS/MAX_LINES in "
            "tests/test_pipeline_freeze.py in this same change and record the "
            "old->new port mapping in docs/governance/REGRESSION_INDEX.md.",
        )


if __name__ == "__main__":
    unittest.main()
