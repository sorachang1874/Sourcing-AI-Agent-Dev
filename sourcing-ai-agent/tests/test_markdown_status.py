from __future__ import annotations

import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
EXCLUDED_DIR_PARTS = {
    ".git",
    ".cache",
    ".pytest_cache",
    ".venv",
    ".venv-tests",
    "node_modules",
    "dist",
    "vendor",
}


def _is_first_party_markdown(path: Path) -> bool:
    relative = path.relative_to(REPO_ROOT)
    parts = set(relative.parts)
    if parts & EXCLUDED_DIR_PARTS:
        return False
    if relative.parts[:2] == ("frontend-demo", "public"):
        return False
    if relative.parts[:2] == ("src", "sourcing_ai_agent.egg-info"):
        return False
    return True


class MarkdownStatusTest(unittest.TestCase):
    def test_first_party_markdown_files_have_status_banner(self) -> None:
        markdown_files = sorted(
            path
            for path in REPO_ROOT.rglob("*.md")
            if path.is_file() and _is_first_party_markdown(path)
        )
        self.assertTrue(markdown_files, "expected at least one first-party Markdown file")
        missing_status = []
        for path in markdown_files:
            lines = path.read_text(encoding="utf-8").splitlines()
            if not any(line.startswith("> Status:") for line in lines[:8]):
                missing_status.append(str(path.relative_to(REPO_ROOT)))
        self.assertEqual(
            missing_status,
            [],
            f"missing status banner: {missing_status}",
        )
