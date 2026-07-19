"""Fail-closed lint for docs/RESIDUAL_LEDGER.md.

Follows the ai-assisted-engineering-playbook residual-ledger preflight: every
ledger row must carry a tripwire, every never-do row must carry ratified-by and
date, row ids must be unique, and statuses must be known. Stdlib only.
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path

LEDGER_STATUSES = {"accepted", "deferred", "frozen", "closed", "never"}
_DATE_RE = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}$")
_ROW_RE = re.compile(r"^\|(.+)\|\s*$")
_HEADING_RE = re.compile(r"^##\s+")


def project_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _table_rows(lines: list[str]) -> list[list[str]]:
    rows: list[list[str]] = []
    in_table = False
    for line in lines:
        match = _ROW_RE.match(line.strip())
        if not match:
            in_table = False
            continue
        cells = [cell.strip() for cell in match.group(1).split("|")]
        if cells and cells[0] == "id":
            in_table = True
            continue
        if not in_table:
            continue
        if all(set(cell) <= {"-", " ", ":"} for cell in cells):
            continue
        rows.append(cells)
    return rows


def _sections(lines: list[str]) -> dict[str, list[str]]:
    sections: dict[str, list[str]] = {"": []}
    current = ""
    for line in lines:
        if _HEADING_RE.match(line):
            current = line.strip()
            sections[current] = []
        else:
            sections.setdefault(current, []).append(line)
    return sections


def check_residual_ledger(path: Path) -> list[str]:
    errors: list[str] = []
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return [f"residual_ledger_unreadable:{path}"]
    sections = _sections(lines)
    ids: set[str] = set()

    ledger_lines = next((body for title, body in sections.items() if title.startswith("## Ledger")), [])
    ledger_rows = _table_rows(ledger_lines)
    if not ledger_rows:
        errors.append("residual_ledger_missing_ledger_rows")
    for row in ledger_rows:
        if len(row) != 9:
            errors.append(f"residual_ledger_row_width:{row[0] if row else '?'}")
            continue
        row_id, _, _, _, _, tripwire, owner, date, status = row
        if row_id in ids:
            errors.append(f"residual_ledger_duplicate_id:{row_id}")
        ids.add(row_id)
        if not tripwire:
            errors.append(f"residual_ledger_missing_tripwire:{row_id}")
        if not owner:
            errors.append(f"residual_ledger_missing_owner:{row_id}")
        if not _DATE_RE.fullmatch(date):
            errors.append(f"residual_ledger_bad_date:{row_id}")
        if status not in LEDGER_STATUSES:
            errors.append(f"residual_ledger_bad_status:{row_id}")

    never_lines = next((body for title, body in sections.items() if title.startswith("## Never-Do")), [])
    for row in _table_rows(never_lines):
        if len(row) != 6:
            errors.append(f"residual_ledger_never_row_width:{row[0] if row else '?'}")
            continue
        row_id, _, _, ratified_by, date, reopen = row
        if row_id in ids:
            errors.append(f"residual_ledger_duplicate_id:{row_id}")
        ids.add(row_id)
        if not ratified_by:
            errors.append(f"residual_ledger_never_missing_ratified_by:{row_id}")
        if not _DATE_RE.fullmatch(date):
            errors.append(f"residual_ledger_never_bad_date:{row_id}")
        if not reopen:
            errors.append(f"residual_ledger_never_missing_reopen_evidence:{row_id}")

    return errors


def main() -> int:
    errors = check_residual_ledger(project_root() / "docs" / "RESIDUAL_LEDGER.md")
    print(json.dumps({"errors": errors, "status": "valid" if not errors else "invalid"}))
    return 0 if not errors else 1


if __name__ == "__main__":
    sys.exit(main())
