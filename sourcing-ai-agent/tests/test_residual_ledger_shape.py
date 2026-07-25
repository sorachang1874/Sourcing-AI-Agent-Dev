"""Table-shape preflight for docs/RESIDUAL_LEDGER.md ledger rows.

The rerun6 FT2 review found R-032 carrying an extra empty cell, which
shifted the date into the status column and left the closure status in an
unlabeled extra column. Cells may legitimately contain `|` inside prose,
so the preflight anchors on trailing cells (date = second-to-last,
status = last) and forbids empty cells anywhere in the row.
"""

from __future__ import annotations

import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
LEDGER = REPO_ROOT / "docs" / "RESIDUAL_LEDGER.md"

_ROW_RE = re.compile(r"^\| R-\d{3} \|")
_DATE_RE = re.compile(r"\d{4}-\d{2}-\d{2}")


def _rows() -> list[tuple[str, list[str]]]:
    out: list[tuple[str, list[str]]] = []
    for line in LEDGER.read_text(encoding="utf-8").splitlines():
        if _ROW_RE.match(line):
            cells = [cell.strip() for cell in line.split("|")][1:-1]
            out.append((cells[0], cells))
    return out


def test_ledger_rows_have_no_empty_cells() -> None:
    offenders = [rid for rid, cells in _rows() if any(cell == "" for cell in cells)]
    assert not offenders, f"ledger rows with empty cells: {offenders}"


def test_ledger_rows_keep_date_and_status_in_the_trailing_columns() -> None:
    rows = _rows()
    assert rows, "ledger must contain R-xxx rows"
    bad_date = [(rid, cells[-2][:40]) for rid, cells in _rows() if not _DATE_RE.fullmatch(cells[-2] or "")]
    bad_status = [rid for rid, cells in _rows() if not cells[-1]]
    assert not bad_date, f"date column (second-to-last cell) invalid: {bad_date}"
    assert not bad_status, f"status column (last cell) empty: {bad_status}"
