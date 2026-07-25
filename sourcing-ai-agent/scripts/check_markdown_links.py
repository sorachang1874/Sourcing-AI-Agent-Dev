#!/usr/bin/env python3
"""Validate local Markdown links (vendored from ai-assisted-engineering-playbook,
workspace-adapted 2026-07-22).

External links are intentionally not checked to keep CI deterministic. Scope:

- default (no args): the ENFORCED routing surface — workspace entry chain, docs
  router + module indexes, governance, registries, deliverables manifest. This set
  must stay green in CI; grow it file-by-file as link debt is paid down.
- `--all`: every first-party Markdown file in the workspace (reporting mode; not
  wired to CI until the backlog is clean).
"""

from __future__ import annotations

import argparse
import pathlib
import re
import sys

PACKAGE_ROOT = pathlib.Path(__file__).resolve().parents[1]
WORKSPACE_ROOT = PACKAGE_ROOT.parent
LINK_RE = re.compile(r"\[[^\]]+\]\(([^)]+)\)")
EXCLUDED_DIR_PARTS = {
    ".git", ".cache", ".pytest_cache", ".venv", ".venv-tests", ".worktrees",
    "node_modules", "dist", "logs", "vendor", "output", "runtime",
    "ai-assisted-engineering-playbook", "archive",
}

ENFORCED = [
    "AGENTS.md", "CLAUDE.md", "README.md", "PROGRESS.md", "NEXT_TODO.md",
    "sourcing-ai-agent/docs/README.md",
    "sourcing-ai-agent/docs/INDEX.md",
    "sourcing-ai-agent/docs/modules/README.md",
    "sourcing-ai-agent/docs/modules/agent-runtime/README.md",
    "sourcing-ai-agent/docs/modules/crm-person-assets/README.md",
    "sourcing-ai-agent/docs/modules/planning-acquisition/README.md",
    "sourcing-ai-agent/docs/modules/platform-operations/README.md",
    "sourcing-ai-agent/docs/modules/provider-runtime/README.md",
    "sourcing-ai-agent/docs/modules/serving-product/README.md",
    "sourcing-ai-agent/docs/modules/workflow-runtime/README.md",
    "sourcing-ai-agent/docs/governance/DOCUMENTATION_MIGRATION_AND_RETIREMENT.md",
    "sourcing-ai-agent/docs/HARNESS_REORG_DESIGN.md",
    "sourcing-ai-agent/scripts/README.md",
    "sourcing-ai-agent/deliverables/MANIFEST.md",
    "sourcing-ai-agent/PROGRESS.md",
    "sourcing-ai-agent/docs/NEXT_TODO.md",
    "x-first-researcher-sourcing/README.md",
]


def is_external(target: str) -> bool:
    return target.startswith(("http://", "https://", "mailto:", "#"))


def check_file(path: pathlib.Path) -> list[tuple[str, str]]:
    missing: list[tuple[str, str]] = []
    text = path.read_text(encoding="utf-8", errors="replace")
    for match in LINK_RE.finditer(text):
        target = match.group(1).strip()
        if is_external(target) or not target:
            continue
        local = target.split("#", 1)[0]
        if local and not (path.parent / local).exists():
            missing.append((str(path.relative_to(WORKSPACE_ROOT)), target))
    return missing


def iter_all() -> list[pathlib.Path]:
    files = []
    for path in WORKSPACE_ROOT.rglob("*.md"):
        try:
            parts = set(path.relative_to(WORKSPACE_ROOT).parts)
        except ValueError:
            continue
        if parts & EXCLUDED_DIR_PARTS:
            continue
        files.append(path)
    return files


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--all", action="store_true", help="scan every first-party Markdown file")
    args = parser.parse_args()

    paths = iter_all() if args.all else [WORKSPACE_ROOT / rel for rel in ENFORCED]
    missing: list[tuple[str, str]] = []
    for path in paths:
        if not path.exists():
            missing.append((str(path.relative_to(WORKSPACE_ROOT)), "<enforced file missing>"))
            continue
        missing.extend(check_file(path))

    if missing:
        for source, target in missing:
            print(f"missing local markdown link: {source} -> {target}")
        print(f"{len(missing)} broken link(s)")
        return 1
    print(f"local markdown links ok ({len(paths)} files)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
