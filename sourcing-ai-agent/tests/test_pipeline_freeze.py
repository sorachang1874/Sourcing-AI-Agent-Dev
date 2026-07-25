"""R-009 resurrection guard — the god-file is DELETED and stays deleted.

History: tests/test_pipeline.py froze at 461 tests / 42,416 lines
(2026-07-22 morning), shrank through six tombstone batches (T-003..T-008)
and ~14 modern homes, and was deleted the same day (salvage-then-delete,
docs/REFACTOR_MASTER_PLAN.md WS3 Tier 3; evidence tables in
docs/governance/RECOVERY_BAND_OWNERSHIP_2026-07-22.md and
NONBAND_OWNERSHIP_R2_SHARD_{A,B}_2026-07-22.md; RESIDUAL_LEDGER R-009
closed). Per the shrink-only ratchet design, after deletion the gate guards
against resurrection: nobody may reintroduce a monolithic test_pipeline.py.
"""

import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]


class GodFileStaysDeletedTest(unittest.TestCase):
    def test_test_pipeline_never_returns(self) -> None:
        self.assertFalse(
            (REPO_ROOT / "tests" / "test_pipeline.py").exists(),
            "tests/test_pipeline.py was salvage-deleted (R-009); do not resurrect the god-file — "
            "new tests belong in the per-module homes listed in docs/governance/REGRESSION_INDEX.md",
        )


if __name__ == "__main__":
    unittest.main()
