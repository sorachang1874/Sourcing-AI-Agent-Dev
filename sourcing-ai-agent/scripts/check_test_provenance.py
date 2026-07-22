#!/usr/bin/env python3
"""Test-provenance anchor scanner (contract: docs/governance/TEST_PROVENANCE.md).

Scans tests/test_*.py module + top-level class docstrings for at least one
machine-checkable provenance anchor. Used by tests/test_provenance.py (gate)
and to (re)generate the grandfather baseline:

    .venv/bin/python scripts/check_test_provenance.py            # report
    .venv/bin/python scripts/check_test_provenance.py --generate-baseline
"""

from __future__ import annotations

import argparse
import ast
import re
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
TESTS_DIR = REPO_ROOT / "tests"
BASELINE_PATH = TESTS_DIR / "provenance_baseline.py"

ANCHOR_RE = re.compile(
    r"(20\d{2}-\d{2}-\d{2}"          # dated incident/decision token
    r"|R-\d{3}"                       # residual-ledger id
    r"|docs/[A-Za-z0-9_./-]+\.md"     # contract-doc path
    r"|\bC\d+\.\d+\b"                 # milestone ids (C2.1 ...)
    r"|\bD\d+[a-z]\d*[a-z]*\b"        # D1n / D3c1a ...
    r"|\bS\d+[a-z]\d*[a-z]*\b"        # S1e2b / S1f0c ...
    r"|\bFT\d+\b|\bM\d+\b"            # FT2 / M2 ...
    r"|\bPhase \d\b|\bTrack [A-D]\b"  # phases / tracks
    r"|harness reorg R\d"
    r"|provenance unknown; characterization adopted 20\d{2}-\d{2}-\d{2})"
)


def file_has_anchor(path: Path) -> bool:
    try:
        tree = ast.parse(path.read_text(encoding="utf-8"))
    except SyntaxError:
        return False
    docstrings = [ast.get_docstring(tree) or ""]
    for node in tree.body:
        if isinstance(node, ast.ClassDef):
            docstrings.append(ast.get_docstring(node) or "")
    return any(ANCHOR_RE.search(doc) for doc in docstrings if doc)


def scan() -> tuple[list[str], list[str]]:
    compliant: list[str] = []
    non_compliant: list[str] = []
    for path in sorted(TESTS_DIR.glob("test_*.py")):
        (compliant if file_has_anchor(path) else non_compliant).append(path.name)
    return compliant, non_compliant


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--generate-baseline", action="store_true")
    args = parser.parse_args()

    compliant, non_compliant = scan()
    if args.generate_baseline:
        body = ",\n".join(f'    "{name}"' for name in non_compliant)
        BASELINE_PATH.write_text(
            '"""Frozen test-provenance grandfather baseline '
            "(generated 2026-07-22; contract: docs/governance/TEST_PROVENANCE.md).\n\n"
            "Shrink-only: a file leaving this set (docstring added, or deleted WITH a\n"
            "REGRESSION_INDEX tombstone) must be removed here in the same change, and\n"
            "MAX_GRANDFATHERED must decrease. Never add entries; never regenerate\n"
            "wholesale after Phase 0 — that would launder new undocumented tests.\n"
            '"""\n\n'
            f"GRANDFATHERED = frozenset([\n{body},\n])\n\n"
            f"MAX_GRANDFATHERED = {len(non_compliant)}\n",
            encoding="utf-8",
        )
        print(f"baseline written: {len(non_compliant)} grandfathered / {len(compliant)} compliant")
        return 0

    print(f"compliant: {len(compliant)}  non-compliant: {len(non_compliant)}")
    for name in non_compliant:
        print(" -", name)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
