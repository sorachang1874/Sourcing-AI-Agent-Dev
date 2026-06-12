"""Guard: git-tracked code must never import untracked modules.

Defect class (found by the async Codex reference review, 2026-06-12): the
long-unsynced worktree let tracked files import modules that existed only
locally, so a clean checkout of the branch could not even import the
package while every in-worktree verification stayed green. This test runs
the same fixpoint sweep that produced the 38-file closure commit and
fails loudly on any new tracked->untracked reference, including paths
referenced by the regression matrix.
"""

from __future__ import annotations

import re
import subprocess
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]

_IMPORT_PATTERN = re.compile(
    r"from \.(\w+) import|from sourcing_agent\.(\w+) import|from tests\.(\w+) import"
)
_MATRIX_PATH_PATTERN = re.compile(r'"((?:tests|src)/[\w/]+\.py)"')


def _tracked_files() -> set[str]:
    result = subprocess.run(
        ["git", "ls-files"],
        capture_output=True,
        text=True,
        check=True,
        cwd=REPO_ROOT,
    )
    return set(result.stdout.split())


def _import_targets(path: Path, *, is_src: bool) -> set[str]:
    text = path.read_text(errors="replace")
    targets: set[str] = set()
    for match in _IMPORT_PATTERN.finditer(text):
        relative_module, src_module, tests_module = match.groups()
        if tests_module:
            targets.add(f"tests/{tests_module}.py")
        elif src_module:
            targets.add(f"src/sourcing_agent/{src_module}.py")
        elif relative_module and is_src:
            targets.add(f"src/sourcing_agent/{relative_module}.py")
    return targets


class RepoImportCoherenceTest(unittest.TestCase):
    def test_tracked_code_only_imports_tracked_modules(self) -> None:
        tracked = _tracked_files()
        violations: list[str] = []
        for tracked_path in sorted(tracked):
            if not tracked_path.endswith(".py"):
                continue
            absolute = REPO_ROOT / tracked_path
            if not absolute.exists():
                # Tracked-but-locally-deleted files (retired modules pending a
                # deletion commit) cannot import anything in a checkout that
                # honors the deletion; skip rather than fail here.
                continue
            is_src = tracked_path.startswith("src/sourcing_agent/")
            for target in sorted(_import_targets(absolute, is_src=is_src)):
                if target in tracked:
                    continue
                if not (REPO_ROOT / target).exists():
                    # Import of a module that exists nowhere is an ordinary
                    # ImportError other suites will catch; this guard is about
                    # locally-present-but-untracked shadow dependencies.
                    continue
                violations.append(f"{tracked_path} imports untracked {target}")
        self.assertEqual(
            violations,
            [],
            "tracked code imports untracked local modules — a clean checkout "
            "would break; commit the dependency (file-scoped, with disclosure) "
            "or remove the reference:\n" + "\n".join(violations),
        )

    def test_regression_matrix_references_only_tracked_paths(self) -> None:
        tracked = _tracked_files()
        matrix_text = (REPO_ROOT / "src/sourcing_agent/regression_matrix.py").read_text(
            errors="replace"
        )
        violations = sorted(
            {
                path
                for path in _MATRIX_PATH_PATTERN.findall(matrix_text)
                if path not in tracked and (REPO_ROOT / path).exists()
            }
        )
        self.assertEqual(
            violations,
            [],
            "regression_matrix references locally-present but untracked paths:\n"
            + "\n".join(violations),
        )


if __name__ == "__main__":
    unittest.main()
