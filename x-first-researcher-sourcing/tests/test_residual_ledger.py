from __future__ import annotations

import unittest

from scripts.check_residual_ledger import check_residual_ledger, project_root


class ResidualLedgerTests(unittest.TestCase):
    def test_checked_in_residual_ledger_is_lint_clean(self) -> None:
        self.assertEqual(check_residual_ledger(project_root() / "docs" / "RESIDUAL_LEDGER.md"), [])

    def test_missing_tripwire_fails_closed(self) -> None:
        import tempfile
        from pathlib import Path

        row = (
            "| R-001 | scope | what | why | fix |  | owner | 2026-07-18 | accepted |\n"
        )
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "RESIDUAL_LEDGER.md"
            path.write_text(
                "# Residual Ledger\n\n## Ledger\n\n"
                "| id | scope | what was accepted | why | correct fix | tripwire | owner | date | status |\n"
                "| --- | --- | --- | --- | --- | --- | --- | --- | --- |\n"
                + row,
                encoding="utf-8",
            )
            self.assertEqual(check_residual_ledger(path), ["residual_ledger_missing_tripwire:R-001"])


if __name__ == "__main__":
    unittest.main()
