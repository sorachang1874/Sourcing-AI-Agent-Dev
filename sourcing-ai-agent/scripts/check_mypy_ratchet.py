#!/usr/bin/env python3
"""mypy shrink-only ratchet gate (operator decision 2026-07-22).

mypy previously gated nothing: the '87 pre-existing errors' CI comment silently
drifted to 81 with no artifact noticing (6 real fixes landed incidentally by
82d69a1). This gate pins the error budget per (file, error-code) in
``configs/mypy_ratchet_baseline.json`` and enforces:

- REGRESSION: any count above budget, or any new (file, error-code) key, fails.
- RATCHET: any count below budget also fails, with the instruction to shrink
  the committed baseline in the same change — counts only ever go down.
- IDENTITY: the [tool.mypy] config hash must match the baseline's; a config or
  scope change requires a sanctioned re-baseline (regenerate via --write).

Exit codes: 0 pass, 1 violations, 2 tooling error.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
BASELINE_PATH = REPO_ROOT / "configs" / "mypy_ratchet_baseline.json"
ERROR_LINE = re.compile(r"^(src/[^:]+):\d+: error: .*\[([a-z-]+)\]\s*$", re.M)


def _tool_mypy_config_hash() -> str:
    pyproject = (REPO_ROOT / "pyproject.toml").read_text(encoding="utf-8")
    section = pyproject[pyproject.index("[tool.mypy]") :]
    next_sec = re.search(r"\n\[(?!tool\.mypy)", section[1:])
    if next_sec:
        section = section[: next_sec.start() + 1]
    return hashlib.sha256(section.encode("utf-8")).hexdigest()[:16]


def _run_configured_mypy() -> str:
    proc = subprocess.run(
        ["bash", str(REPO_ROOT / "scripts" / "run_python_quality.sh"), "typecheck"],
        capture_output=True,
        text=True,
        cwd=REPO_ROOT,
        check=False,
    )
    output = proc.stdout + proc.stderr
    if "error:" not in output and proc.returncode not in (0, 1):
        print(output[-2000:])
        print(f"TOOLING ERROR: typecheck chain exited {proc.returncode} without mypy output")
        raise SystemExit(2)
    return output


def _parse_counts(output: str) -> dict[str, dict[str, int]]:
    counts: dict[str, dict[str, int]] = {}
    for match in ERROR_LINE.finditer(output):
        bucket = counts.setdefault(match.group(1), {})
        bucket[match.group(2)] = bucket.get(match.group(2), 0) + 1
    return counts


def compare(baseline: dict[str, dict[str, int]], current: dict[str, dict[str, int]]) -> list[str]:
    violations: list[str] = []
    for file, codes in sorted(current.items()):
        for code, count in sorted(codes.items()):
            budget = int(baseline.get(file, {}).get(code, 0))
            if count > budget:
                violations.append(
                    f"REGRESSION {file} [{code}]: {count} > budget {budget} — fix the new errors"
                )
            elif count < budget:
                violations.append(
                    f"RATCHET {file} [{code}]: {count} < budget {budget} — shrink "
                    f"configs/mypy_ratchet_baseline.json in this same change (counts only go down)"
                )
    for file, codes in sorted(baseline.items()):
        for code, budget in sorted(codes.items()):
            if int(budget) > 0 and code not in current.get(file, {}):
                violations.append(
                    f"RATCHET {file} [{code}]: 0 < budget {budget} — shrink the baseline (error family cleared)"
                )
    return violations


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--write",
        action="store_true",
        help="Sanctioned re-baseline: regenerate budgets + _meta from the current run",
    )
    args = parser.parse_args()

    output = _run_configured_mypy()
    current = _parse_counts(output)
    total = sum(v for codes in current.values() for v in codes.values())
    config_hash = _tool_mypy_config_hash()

    if args.write:
        baseline_doc = json.loads(BASELINE_PATH.read_text(encoding="utf-8"))
        baseline_doc["_meta"]["tool_mypy_config_sha256_16"] = config_hash
        baseline_doc["_meta"]["total_errors"] = total
        baseline_doc["budgets"] = {f: dict(sorted(c.items())) for f, c in sorted(current.items())}
        BASELINE_PATH.write_text(json.dumps(baseline_doc, indent=1, ensure_ascii=False) + "\n", encoding="utf-8")
        print(f"re-baselined: {total} errors across {len(current)} files (config {config_hash})")
        return 0

    baseline_doc = json.loads(BASELINE_PATH.read_text(encoding="utf-8"))
    if str(baseline_doc["_meta"].get("tool_mypy_config_sha256_16")) != config_hash:
        print(
            "IDENTITY MISMATCH: [tool.mypy] config changed since the baseline "
            f"({baseline_doc['_meta'].get('tool_mypy_config_sha256_16')} -> {config_hash}). "
            "If intentional, re-baseline with --write and say so in the commit message."
        )
        return 1

    violations = compare(baseline_doc.get("budgets", {}), current)
    if violations:
        print(f"mypy ratchet: {len(violations)} violation(s) (current total {total}):")
        for line in violations:
            print(" -", line)
        return 1
    print(f"mypy ratchet OK: {total} errors, all within (file, error-code) budgets")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
