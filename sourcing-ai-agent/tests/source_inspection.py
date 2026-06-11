"""AST-based source resolver for guardrail tests.

Guardrail tests historically read ``orchestrator.py`` as raw text and sliced
between ``source.index("def A")`` and ``source.index("def B")`` to approximate
method bodies.  That breaks as soon as methods move between modules.  The
helpers here locate definitions by name anywhere under ``src/sourcing_agent``
(recursively, so future domain subpackages are covered) and return the exact
source segment of the definition, keeping token presence/absence guardrails
stable across file moves with zero test churn.

Stdlib only; no project imports.
"""

from __future__ import annotations

import ast
from functools import lru_cache
from pathlib import Path

SRC_ROOT = Path(__file__).resolve().parents[1] / "src" / "sourcing_agent"


@lru_cache(maxsize=None)
def _load(path: Path) -> tuple[str, ast.Module]:
    source = path.read_text(encoding="utf-8")
    return source, ast.parse(source, filename=str(path))


def all_source_files(root: Path = SRC_ROOT) -> list[Path]:
    """All ``*.py`` files under ``root`` (recursive), skipping ``__pycache__``."""
    return sorted(
        path
        for path in root.rglob("*.py")
        if path.is_file() and "__pycache__" not in path.parts
    )


def _iter_class_methods(tree: ast.Module, class_name: str | None):
    for node in ast.walk(tree):
        if not isinstance(node, ast.ClassDef):
            continue
        if class_name is not None and node.name != class_name:
            continue
        for child in node.body:
            if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef)):
                yield node.name, child


def find_class_method(
    method_name: str,
    class_name: str | None = "SourcingOrchestrator",
    root: Path = SRC_ROOT,
) -> tuple[Path, str]:
    """Locate a method by name inside ``class_name`` (any class when ``None``).

    Returns ``(file_path, exact source segment of the method)``.  Raises
    ``AssertionError`` when the method is missing or ambiguously defined.
    """
    matches: list[tuple[Path, str, str]] = []
    for path in all_source_files(root):
        source, tree = _load(path)
        for owner_name, node in _iter_class_methods(tree, class_name):
            if node.name != method_name:
                continue
            segment = ast.get_source_segment(source, node)
            assert segment is not None, (
                f"could not extract source segment for {owner_name}.{method_name} in {path}"
            )
            matches.append((path, owner_name, segment))
    scope = f"class {class_name}" if class_name is not None else "any class"
    if not matches:
        raise AssertionError(
            f"method {method_name!r} not found in {scope} under {root}"
        )
    if len(matches) > 1:
        locations = ", ".join(f"{path}::{owner}" for path, owner, _ in matches)
        raise AssertionError(
            f"method {method_name!r} matched {len(matches)} definitions in {scope} "
            f"under {root}: {locations}"
        )
    path, _, segment = matches[0]
    return path, segment


def find_module_def(name: str, root: Path = SRC_ROOT) -> tuple[Path, str]:
    """Locate a top-level (module-scope) function definition by name.

    Returns ``(file_path, exact source segment)``.  Raises ``AssertionError``
    when missing or ambiguous.
    """
    matches: list[tuple[Path, str]] = []
    for path in all_source_files(root):
        source, tree = _load(path)
        for node in tree.body:
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == name:
                segment = ast.get_source_segment(source, node)
                assert segment is not None, (
                    f"could not extract source segment for {name} in {path}"
                )
                matches.append((path, segment))
    if not matches:
        raise AssertionError(f"module-level function {name!r} not found under {root}")
    if len(matches) > 1:
        locations = ", ".join(str(path) for path, _ in matches)
        raise AssertionError(
            f"module-level function {name!r} matched {len(matches)} definitions "
            f"under {root}: {locations}"
        )
    return matches[0]


def find_def(
    name: str,
    class_name: str | None = "SourcingOrchestrator",
    root: Path = SRC_ROOT,
) -> tuple[Path, str]:
    """Locate ``name`` as a method first, falling back to a module-level def."""
    try:
        return find_class_method(name, class_name=class_name, root=root)
    except AssertionError as method_error:
        try:
            return find_module_def(name, root=root)
        except AssertionError as module_error:
            raise AssertionError(
                f"definition {name!r} not found under {root}: "
                f"{method_error}; {module_error}"
            ) from module_error
