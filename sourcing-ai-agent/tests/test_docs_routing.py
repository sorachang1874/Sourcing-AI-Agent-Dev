"""Routing-norm lints (harness reorg R5): broken-link gate + snapshot budgets.

Companions to tests/test_markdown_status.py (status-banner gate). Per playbook
Principle 28, a routing norm that exists only as prose is not implemented —
these tests are the machine feedback for docs/README.md's declared contracts.
"""

import importlib.util
import re
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
WORKSPACE_ROOT = REPO_ROOT.parent

_SPEC = importlib.util.spec_from_file_location(
    "check_markdown_links", REPO_ROOT / "scripts" / "check_markdown_links.py"
)
cml = importlib.util.module_from_spec(_SPEC)
assert _SPEC.loader is not None
_SPEC.loader.exec_module(cml)


class MarkdownLinkGateTest(unittest.TestCase):
    def test_all_first_party_local_markdown_links_resolve(self) -> None:
        missing = []
        for path in cml.iter_all():
            missing.extend(cml.check_file(path))
        self.assertEqual(missing, [], "broken local markdown links")

    def test_enforced_routing_surface_files_exist(self) -> None:
        gone = [rel for rel in cml.ENFORCED if not (WORKSPACE_ROOT / rel).exists()]
        self.assertEqual(gone, [], "enforced routing files missing")


class SnapshotBudgetTest(unittest.TestCase):
    # Budgets are declared inside each snapshot's own header block; these
    # constants must match those declarations.
    BUDGETS = {"PROGRESS.md": 200, "NEXT_TODO.md": 120}

    def test_workspace_snapshots_respect_their_declared_budgets(self) -> None:
        over = {}
        for name, budget in self.BUDGETS.items():
            lines = (WORKSPACE_ROOT / name).read_text(encoding="utf-8").count("\n") + 1
            if lines > budget:
                over[name] = (lines, budget)
        self.assertEqual(over, {}, "snapshot over budget: replace, don't append")

    def test_snapshots_declare_owner_and_refresh_date(self) -> None:
        for name in self.BUDGETS:
            text = (WORKSPACE_ROOT / name).read_text(encoding="utf-8")
            self.assertRegex(text, r"owner:", name)
            self.assertRegex(text, r"refreshed: \d{4}-\d{2}-\d{2}", name)


class RouterContractTest(unittest.TestCase):
    def test_router_gap_rows_each_name_a_resolution_target(self) -> None:
        text = (REPO_ROOT / "docs" / "README.md").read_text(encoding="utf-8")
        match = re.search(r"## Routing Gaps\n(.*?)(\n## |\Z)", text, re.S)
        self.assertIsNotNone(match, "router must keep a Routing Gaps section")
        rows = [
            line for line in match.group(1).splitlines()
            if line.startswith("|") and "---" not in line and "Resolution target" not in line
        ]
        for row in rows:
            cells = [c.strip() for c in row.strip("|").split("|")]
            self.assertEqual(len(cells), 4, f"gap row needs 4 cells: {row}")
            self.assertTrue(cells[3], f"gap row lacks a resolution target: {row}")


if __name__ == "__main__":
    unittest.main()
