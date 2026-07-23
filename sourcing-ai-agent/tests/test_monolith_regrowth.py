"""Anti-regrowth ratchet — the monoliths may only shrink.

Provenance: master plan docs/REFACTOR_MASTER_PLAN.md WS2 slice 3 (2026-07-22):
Track A shrank orchestrator.py 87k -> 74k across Phases 0-4, then +8.4k of
Track C/D work regrew inside the monolith in five weeks because the boundary
decision had no enforcement mechanism ("a boundary decision without an
enforcement mechanism is a wish" — playbook doc 26 §4). Same shrink-only
semantics as the retired god-file freeze ratchet (T-008): growing a listed
file fails REGRESSION; shrinking it without lowering the budget here fails
RATCHET (captured-improvement discipline, playbook doc 25 §1). New feature
code belongs in owner modules, not the monolith.

Budgets are LINE COUNTS (newline count), keyed on the files whose regrowth
burned us. Update a budget DOWNWARD in the same change that shrinks the file;
never upward without an operator-sanctioned re-baseline commit.
"""

import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]

# file -> (max_lines, slack). slack absorbs mechanical churn (imports,
# comments) without letting real regrowth hide; keep it small.
_MONOLITH_BUDGETS = {
    "src/sourcing_agent/orchestrator.py": 82300,
    "src/sourcing_agent/storage.py": 13000,
    "src/sourcing_agent/enrichment.py": 12800,
    "src/sourcing_agent/workflow_smoke.py": 10300,
    "src/sourcing_agent/acquisition.py": 7850,
    "src/sourcing_agent/harvest_connectors.py": 5350,
    "src/sourcing_agent/cli.py": 5200,
}

# When a file drops far below budget, force the budget down too (captured
# improvement): the gap between budget and actual may not exceed this.
_MAX_UNCAPTURED_SHRINK = 600


class MonolithRegrowthRatchetTest(unittest.TestCase):
    def test_monoliths_only_shrink(self) -> None:
        regressions: list[str] = []
        uncaptured: list[str] = []
        for relative, budget in _MONOLITH_BUDGETS.items():
            path = REPO_ROOT / relative
            self.assertTrue(path.exists(), f"budgeted monolith missing: {relative}")
            lines = path.read_text(encoding="utf-8").count("\n")
            if lines > budget:
                regressions.append(f"{relative}: {lines} > budget {budget}")
            elif budget - lines > _MAX_UNCAPTURED_SHRINK:
                uncaptured.append(
                    f"{relative}: {lines} is {budget - lines} under budget {budget} — "
                    "shrink the budget in tests/test_monolith_regrowth.py in this change"
                )
        self.assertFalse(
            regressions,
            "REGRESSION — monolith regrowth. New code belongs in owner modules "
            "(docs/SERVING_MESH_OWNERSHIP_BOUNDARY.md / recovery_phases / owner files), "
            f"not the monolith: {regressions}",
        )
        self.assertFalse(uncaptured, f"RATCHET — capture the improvement: {uncaptured}")


if __name__ == "__main__":
    unittest.main()
