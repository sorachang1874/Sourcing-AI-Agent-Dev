import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]


class FrontendResultsBoardPaginationTest(unittest.TestCase):
    def test_pagination_preserves_total_pages_while_backend_page_request_pending(self) -> None:
        source = (REPO_ROOT / "frontend-demo/src/components/ResultsBoardPanel.tsx").read_text(encoding="utf-8")

        self.assertIn("lastKnownFilteredCandidateCount", source)
        self.assertIn("setLastKnownFilteredCandidateCount(freshFilteredCandidateCount)", source)
        self.assertIn("waitingForBackendPage", source)
        self.assertIn("? lastKnownFilteredCandidateCount > 0", source)
        self.assertIn(": fallbackVisibleCandidates.length", source)
        self.assertIn("const totalPages = Math.max(1, Math.ceil(visibleCandidateCount / RESULTS_PAGE_SIZE));", source)

        reset_guard = source[source.index("useEffect(() => {\n    // Do not collapse") :]
        self.assertIn("if (waitingForBackendPage) {\n      return;\n    }", reset_guard)
        self.assertIn("setCurrentPage(totalPages)", reset_guard)

    def test_pagination_resets_to_one_when_filter_changes(self) -> None:
        source = (REPO_ROOT / "frontend-demo/src/components/ResultsBoardPanel.tsx").read_text(encoding="utf-8")
        filter_reset = source[source.index("Filter changed: clear last-known totals") :]

        self.assertIn("setCurrentPage(1)", source)
        self.assertIn("setLastKnownFilteredCandidateCount(0)", filter_reset)
        self.assertIn("selectedRecallBuckets", filter_reset)
        self.assertIn("selectedEmploymentStatuses", filter_reset)
        self.assertIn("selectedLayerStates", filter_reset)

    def test_pagination_falls_back_when_no_last_known_total(self) -> None:
        source = (REPO_ROOT / "frontend-demo/src/components/ResultsBoardPanel.tsx").read_text(encoding="utf-8")
        visible_count_block = source[source.index("const visibleCandidateCount = preservedFacetIntentDuringGap") :]

        self.assertIn("lastKnownFilteredCandidateCount > 0", visible_count_block)
        self.assertIn("? lastKnownFilteredCandidateCount", visible_count_block)
        self.assertIn(": fallbackVisibleCandidates.length", visible_count_block)
        self.assertNotIn("waitingForBackendPage\n      ? []", source)
        self.assertIn(": fallbackVisibleCandidates", source)

    def test_projection_filters_fail_closed_when_index_is_unavailable(self) -> None:
        source = (REPO_ROOT / "frontend-demo/src/components/ResultsBoardPanel.tsx").read_text(encoding="utf-8")

        self.assertIn("const filterControlsAvailable = !canonicalFacetUnavailable;", source)
        self.assertIn("disabled={!filterControlsAvailable}", source)
        self.assertIn("筛选索引准备中", source)
        backend_filter_block = source[source.index("const backendPageFilter = useMemo<DashboardCandidatePageFilter>") :]
        self.assertIn("filterControlsAvailable", backend_filter_block)
        self.assertIn('searchKeyword: ""', backend_filter_block)
        self.assertIn("recallBuckets: []", backend_filter_block)
        self.assertIn("auditStatuses: []", backend_filter_block)

    def test_projection_exact_scope_enables_canonical_filters(self) -> None:
        panel_source = (REPO_ROOT / "frontend-demo/src/components/ResultsBoardPanel.tsx").read_text(encoding="utf-8")
        api_source = (REPO_ROOT / "frontend-demo/src/lib/api.ts").read_text(encoding="utf-8")

        # Local asset / projection readers expose whole-projection filter counts
        # as exact_projection. The frontend must treat that as canonical rather
        # than waiting for the old job-board global_full_population scope.
        self.assertIn('facetSummaryScope === "exact_projection"', panel_source)
        self.assertIn('dashboard.boardRuntimeState.facetSummaryScope === "exact_projection"', panel_source)
        self.assertIn('return normalized === "global_full_population" || normalized === "exact_projection";', api_source)
        # Rerun3 finding 2: the summary scope is consumed from its backend
        # owners only (facet_summary_scope / facet_summary.count_scope) and is
        # NEVER minted via an `exact_projection` fallback default; the filter
        # contract's count scope comes independently from the projection's own
        # counts.facet_count_scope.
        self.assertNotIn('mapCandidateFacetSummaryScope(payload, "exact_projection")', api_source)
        self.assertIn('facetSummaryScopeEvidence(payload, "facet_summary_scope", false)', api_source)
        self.assertIn('facetSummaryScopeEvidence(payload.facet_summary, "count_scope", true)', api_source)
        self.assertIn('pickFirstString(counts, ["facet_count_scope"])', api_source)
        self.assertIn("facet_summary: payload.facet_summary", api_source)
        self.assertIn("facet_summary_scope: payload.facet_summary_scope,", api_source)


if __name__ == "__main__":
    unittest.main()
