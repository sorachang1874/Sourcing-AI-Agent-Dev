"""Serving-mesh boundary structural guards (Block a resolver extraction).

Provenance: docs/SERVING_MESH_OWNERSHIP_BOUNDARY.md §5 guards 1-2 (boundary
freeze 2026-06-14; Block (a) extraction pulled forward by operator decision
2026-07-22, REFACTOR_MASTER_PLAN.md WS2 slice 2). Adopted 2026-07-22.

Three invariants, all static-source (no PG):
1. Resolver module dependency direction — candidate_source_resolver.py never
   imports the orchestrator and never references projection command-band
   (plan/enqueue/process/drain) or read-model paging symbols.
2. Edge B stays broken — read-model paging (get_job_dashboard /
   get_job_candidate_page / the asset-population page builders) consumes
   resolver output via _build_job_results_context and never calls
   _resolve_job_candidate_source directly.
3. The orchestrator methods are delegating seams — a parallel in-class
   re-implementation of resolution is a regression.
"""

from __future__ import annotations

import inspect
import re
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
RESOLVER_SOURCE = (REPO_ROOT / "src" / "sourcing_agent" / "candidate_source_resolver.py").read_text(encoding="utf-8")

# Boundary doc §2b/§2c: paging members that must consume resolver output.
PAGING_METHOD_NAMES = (
    "get_job_dashboard",
    "get_job_candidate_page",
    "_build_job_asset_population_page_from_canonical_projection",
    "_build_public_board_runtime_projection",
)
# Boundary doc §2c command bands + paging: the resolver must never reach
# these. (No-orchestrator-import is enforced by the import-surface test below;
# the literal word appears legitimately in provenance docstrings.)
FORBIDDEN_IN_RESOLVER = (
    "_plan_run_scope_projection_finalize_command",
    "_drain_run_scope_projection_finalize_commands",
    "_plan_operation_native_projection_admission_command",
    "_drain_operation_native_projection_admission_commands",
    "_plan_projection_export_generate_command",
    "get_job_dashboard",
    "get_job_candidate_page",
    "serving_projection_writer",
)


class ResolverDependencyDirectionTest(unittest.TestCase):
    def test_resolver_module_never_touches_command_bands_or_paging(self) -> None:
        for symbol in FORBIDDEN_IN_RESOLVER:
            with self.subTest(symbol=symbol):
                self.assertNotIn(
                    symbol,
                    RESOLVER_SOURCE.replace("read-model\npaging", "").replace("read-model paging", ""),
                    f"candidate_source_resolver.py must not reference {symbol!r} "
                    "(SERVING_MESH_OWNERSHIP_BOUNDARY.md §2a dependency direction)",
                )

    def test_resolver_module_import_surface_is_downward_only(self) -> None:
        imports = re.findall(r"^from \.([a-z_]+) import", RESOLVER_SOURCE, flags=re.MULTILINE)
        self.assertEqual(
            sorted(set(imports)),
            ["domain", "retrieval_runtime"],
            "resolver may only import shared domain types and retrieval_runtime helpers",
        )


class EdgeBStaysBrokenTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        from sourcing_agent.orchestrator import SourcingOrchestrator

        cls.orchestrator_cls = SourcingOrchestrator

    def test_paging_never_reresolves_candidate_source(self) -> None:
        for name in PAGING_METHOD_NAMES:
            with self.subTest(method=name):
                body = inspect.getsource(getattr(self.orchestrator_cls, name))
                self.assertNotIn(
                    "_resolve_job_candidate_source(",
                    body,
                    f"{name} must consume _build_job_results_context output, "
                    "never re-resolve (Edge B, boundary doc §3/§5 guard 2)",
                )

    def test_orchestrator_methods_are_delegating_seams(self) -> None:
        resolve_body = inspect.getsource(self.orchestrator_cls._resolve_job_candidate_source)
        context_body = inspect.getsource(self.orchestrator_cls._build_job_results_context)
        self.assertIn("._candidate_source_resolver.resolve(", resolve_body)
        self.assertIn("._candidate_source_resolver.build_results_context(", context_body)
        # A regrown in-class implementation would need store/ladder calls again.
        for body, label in ((resolve_body, "_resolve_job_candidate_source"), (context_body, "_build_job_results_context")):
            with self.subTest(method=label):
                self.assertNotIn("self.store.", body, f"{label} must stay a thin delegate")


if __name__ == "__main__":
    unittest.main()
