"""Lane-manifest drift guard (harness R0 batch, 2026-07-22).

The contract lane was defined twice (workspace backend-ci.yml vs Makefile
CI_PRE_AGENT_CONTRACT_CMD) with no consistency check — splitting any lane file
could silently drop it from one consumer (the false-green class the 2026-07-09
skip-to-fail hardening targets). This guard parses BOTH consumers and asserts
they match tests/lane_manifest.py exactly. Lane changes edit the manifest first.
"""

import re
import unittest
from pathlib import Path

from tests.lane_manifest import GH_LANE_FULL, GH_ONLY_PARTIAL, LANE_PARTIAL, MAKE_LANE_FULL

REPO_ROOT = Path(__file__).resolve().parents[1]
WORKSPACE_ROOT = REPO_ROOT.parent
BACKEND_CI = WORKSPACE_ROOT / ".github" / "workflows" / "backend-ci.yml"
MAKEFILE = REPO_ROOT / "Makefile"


def _gh_lane_invocations() -> list[tuple[list[str], str]]:
    """Parse the Contract Test Lane run block into (files, k-expr) invocations."""
    text = BACKEND_CI.read_text(encoding="utf-8")
    start = text.index("Contract Test Lane")
    run_block = text[text.index("run: |", start) :]
    next_step = re.search(r"\n      - name:", run_block)
    if next_step:
        run_block = run_block[: next_step.start()]
    invocations: list[tuple[list[str], str]] = []
    for chunk in re.split(r"python -m pytest", run_block)[1:]:
        files = re.findall(r"(tests/test_[A-Za-z0-9_]+\.py)", chunk)
        k_match = re.search(r"-k '([^']+)'|-k ([A-Za-z0-9_]+)", chunk)
        k_expr = (k_match.group(1) or k_match.group(2)) if k_match else ""
        if files:
            invocations.append((files, " ".join(k_expr.split())))
    return invocations


def _make_lane_invocations() -> list[tuple[list[str], str]]:
    text = MAKEFILE.read_text(encoding="utf-8")
    line = next(l for l in text.splitlines() if l.startswith("CI_PRE_AGENT_CONTRACT_CMD"))
    invocations: list[tuple[list[str], str]] = []
    for chunk in re.split(r"-m pytest", line)[1:]:
        chunk = chunk.split("&&")[0]
        files = re.findall(r"(tests/test_[A-Za-z0-9_]+\.py)", chunk)
        k_match = re.search(r"-k '([^']+)'|-k ([A-Za-z0-9_]+)", chunk)
        k_expr = (k_match.group(1) or k_match.group(2)) if k_match else ""
        if files:
            invocations.append((files, " ".join(k_expr.split())))
    return invocations


class GhLaneMatchesManifestTest(unittest.TestCase):
    def test_full_file_members_match_in_order(self) -> None:
        invocations = _gh_lane_invocations()
        full_invocations = [files for files, k in invocations if not k]
        gh_full_files = [f for files in full_invocations for f in files]
        self.assertEqual(gh_full_files, [path for path, _why in GH_LANE_FULL])

    def test_partial_members_match_in_order(self) -> None:
        invocations = [(files, k) for files, k in _gh_lane_invocations() if k]
        expected = [
            (list(files), " ".join(k.split()))
            for files, k, _why in [*LANE_PARTIAL, *GH_ONLY_PARTIAL]
        ]
        self.assertEqual(invocations, expected)


class MakeLaneMatchesManifestTest(unittest.TestCase):
    def test_full_file_members_match_in_order(self) -> None:
        invocations = _make_lane_invocations()
        full_invocations = [files for files, k in invocations if not k]
        make_full_files = [f for files in full_invocations for f in files]
        self.assertEqual(make_full_files, [path for path, _why in MAKE_LANE_FULL])

    def test_partial_members_match_shared_set(self) -> None:
        invocations = [(files, k) for files, k in _make_lane_invocations() if k]
        expected = [(list(files), " ".join(k.split())) for files, k, _why in LANE_PARTIAL]
        self.assertEqual(invocations, expected)


class ManifestHygieneTest(unittest.TestCase):
    def test_every_manifest_entry_names_an_existing_test_file(self) -> None:
        for path, _why in [*GH_LANE_FULL, *MAKE_LANE_FULL]:
            self.assertTrue((REPO_ROOT / path).is_file(), path)
        for files, _k, _why in [*LANE_PARTIAL, *GH_ONLY_PARTIAL]:
            for path in files:
                self.assertTrue((REPO_ROOT / path).is_file(), path)

    def test_every_entry_carries_a_rationale(self) -> None:
        for _path, why in [*GH_LANE_FULL, *MAKE_LANE_FULL]:
            self.assertTrue(why.strip())
        for _files, _k, why in [*LANE_PARTIAL, *GH_ONLY_PARTIAL]:
            self.assertTrue(why.strip())


if __name__ == "__main__":
    unittest.main()
