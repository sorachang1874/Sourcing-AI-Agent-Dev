from __future__ import annotations

import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
CHECKPOINT_SCOPE_MARKER = "Required staged files for M0 checkpoint (git-root-relative):"
M0_CHECKPOINT_REQUIRED_FILES = [
    "sourcing-ai-agent/README.md",
    "sourcing-ai-agent/PROGRESS.md",
    "sourcing-ai-agent/docs/INDEX.md",
    "sourcing-ai-agent/docs/NEXT_TODO.md",
    "sourcing-ai-agent/docs/SERVICE_GRADE_ARCHITECTURE_PLAN.md",
    "sourcing-ai-agent/docs/AGENT_OPERATION_CONTRACT.md",
    "sourcing-ai-agent/docs/PRE_AGENT_CONTRACT_REVIEW.md",
    "sourcing-ai-agent/docs/DURABLE_EXECUTION_RUNTIME_CONTRACT.md",
    "sourcing-ai-agent/docs/MODEL_NATIVE_SEARCH_PROVIDER_CONTRACT.md",
    "sourcing-ai-agent/docs/INDEPENDENT_REVIEW_BRIEF.md",
    "sourcing-ai-agent/docs/INDEPENDENT_REVIEW_GATE.md",
]
M0_CHECKPOINT_FORBIDDEN_FILES = [
    "sourcing-ai-agent/frontend-demo/src/pages/OperationsPage.tsx",
    "sourcing-ai-agent/frontend-demo/src/lib/api.ts",
    "sourcing-ai-agent/frontend-demo/src/App.tsx",
    "sourcing-ai-agent/contracts/frontend_api_contract.ts",
    "sourcing-ai-agent/contracts/frontend_api_contract.schema.json",
    "sourcing-ai-agent/contracts/frontend_api_adapter.ts",
    "sourcing-ai-agent/docs/FRONTEND_API_CONTRACT.md",
    "sourcing-ai-agent/src/sourcing_agent/durable_runtime.py",
    "sourcing-ai-agent/src/sourcing_agent/operation_runtime.py",
    "sourcing-ai-agent/tests/test_pre_agent_contract_review.py",
]
EXCLUDED_DIR_PARTS = {
    ".git",
    ".cache",
    ".pytest_cache",
    ".venv",
    ".venv-tests",
    "node_modules",
    "dist",
    "logs",
    "vendor",
    "output",
    "runtime",
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


def _extract_m0_checkpoint_files(markdown: str) -> list[str]:
    lines = markdown.splitlines()
    for index, line in enumerate(lines):
        if CHECKPOINT_SCOPE_MARKER not in line:
            continue
        files: list[str] = []
        for candidate in lines[index + 1 :]:
            stripped = candidate.strip()
            if stripped.startswith("- `") and stripped.endswith("`"):
                files.append(stripped.removeprefix("- `").removesuffix("`"))
                continue
            if files and stripped:
                break
        return files
    return []


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

    def test_m0_checkpoint_file_scope_is_git_root_relative_and_consistent(self) -> None:
        service_plan = (REPO_ROOT / "docs" / "SERVICE_GRADE_ARCHITECTURE_PLAN.md").read_text(
            encoding="utf-8"
        )
        next_todo = (REPO_ROOT / "docs" / "NEXT_TODO.md").read_text(encoding="utf-8")

        service_scope = _extract_m0_checkpoint_files(service_plan)
        next_todo_scope = _extract_m0_checkpoint_files(next_todo)

        self.assertEqual(service_scope, M0_CHECKPOINT_REQUIRED_FILES)
        self.assertEqual(next_todo_scope, M0_CHECKPOINT_REQUIRED_FILES)
        self.assertEqual(service_scope, next_todo_scope)
        for path in service_scope:
            self.assertTrue(path.startswith("sourcing-ai-agent/"), path)
        for forbidden_path in M0_CHECKPOINT_FORBIDDEN_FILES:
            self.assertNotIn(forbidden_path, service_scope)
            self.assertNotIn(forbidden_path, next_todo_scope)
        self.assertIn("git diff --cached --name-only", service_plan)
        self.assertIn("git diff --cached --name-only", next_todo)
