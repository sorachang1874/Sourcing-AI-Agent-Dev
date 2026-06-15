from __future__ import annotations

import json
import shutil
import subprocess
import textwrap
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]


class FrontendDashboardHydrationContractTest(unittest.TestCase):
    def test_completed_polling_path_forces_fresh_dashboard_before_results_render(self) -> None:
        source = (REPO_ROOT / "frontend-demo/src/pages/SearchPage.tsx").read_text(encoding="utf-8")
        completed_branch_start = source.index('if (nextRunStatus.status === "completed")')
        completed_branch = source[completed_branch_start : source.index("const runningFlow", completed_branch_start)]

        self.assertIn("forceRefreshFirst: true", completed_branch)
        self.assertNotIn("setDashboard(mergedCachedDashboard)", completed_branch)
        self.assertNotIn("setDashboard(mergedFallbackDashboard)", completed_branch)

    def test_canonical_backend_paging_does_not_hydrate_full_large_board(self) -> None:
        source = (REPO_ROOT / "frontend-demo/src/hooks/useDashboardCandidateHydration.ts").read_text(encoding="utf-8")

        self.assertIn("backendCanonicalPagingAvailable", source)
        self.assertIn("filterContract?.backendFilteredPagingSupported", source)
        self.assertIn("!backendCanonicalPagingAvailable && finalTotalCandidates > 0", source)

    def test_projection_pages_use_backend_projection_filtering_not_full_board_hydration(self) -> None:
        api_source = (REPO_ROOT / "frontend-demo/src/lib/api.ts").read_text(encoding="utf-8")
        panel_source = (REPO_ROOT / "frontend-demo/src/components/ResultsBoardPanel.tsx").read_text(encoding="utf-8")

        self.assertIn("backendFilteredPagingSupported: true", api_source)
        self.assertIn("const filterQueryParams = candidatePageFilterQueryParams(options?.filter);", api_source)
        self.assertIn("...filterQueryParams", api_source)
        self.assertIn("filtered_candidate_count", api_source)
        self.assertIn("mapCandidatePageFilterContract(payload.filter_contract)", api_source)
        self.assertIn("filter: backendPageFilter", panel_source)
        self.assertIn("projectionOnlyReadOnly", panel_source)
        self.assertIn("CRM source-projection writer", panel_source)

    def _run_hydration_cases(self) -> list[dict]:
        if shutil.which("node") is None:
            self.skipTest("node is required for frontend TypeScript helper checks")
        script = textwrap.dedent(
            """
            const fs = require("fs");
            const path = require("path");
            const vm = require("vm");
            const ts = require("./frontend-demo/node_modules/typescript");
            const source = fs.readFileSync(
              path.join(process.cwd(), "frontend-demo/src/lib/dashboardHydration.ts"),
              "utf8",
            );
            const compiled = ts.transpileModule(source, {
              compilerOptions: { module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2020 },
            }).outputText;
            const module = { exports: {} };
            vm.runInNewContext(compiled, { module, exports: module.exports, require, console }, {
              filename: "dashboardHydration.js",
            });
            const {
              dashboardCandidateBoardBootstrapping,
              dashboardCandidateHydrationBannerVisible,
              dashboardCandidateHydrationPending,
              dashboardExpectedCandidateCount,
              dashboardHasPublicCandidateCount,
              dashboardHasRenderableCandidates,
            } = module.exports;
            const lifecycle = {
              state: "current_snapshot_serving",
              baselineSnapshotId: "20260414T120300",
              currentSnapshotId: "20260504T133419",
              servedSnapshotId: "20260504T133419",
              baselineCandidateCount: 300,
              servedCandidateCount: 597,
              expectedCandidateCount: 597,
              deltaProfileProgressApplicable: true,
              deltaProfileProgressReason: "linkedin_stage_1_progress",
              deltaProfileRequiredCount: 297,
              deltaProfileFetchedCount: 297,
              deltaProfileMaterializedCount: 297,
              deltaProfileBoardVisibleCount: 297,
              deltaProfilePendingCount: 0,
              deltaProfileQueuedCount: 0,
              deltaProfileRetryableCount: 0,
              backgroundSnapshotMaterializationStatus: "",
              outreachLayeringStatus: "completed",
            };
            const candidate = (index) => ({
              id: `candidate-${index}`,
              name: `Candidate ${index}`,
              headline: "",
              avatarUrl: "",
              team: "",
              employmentStatus: "current",
              confidence: "high",
              summary: "",
              outreachLayer: null,
              matchedKeywords: [],
              focusAreas: [],
              matchReasons: [],
              education: [],
              experience: [],
              evidence: [],
            });
            const baseDashboard = {
              title: "Dashboard",
              snapshotId: "20260504T133419",
              queryLabel: "OpenAI Agent",
              targetCompany: "OpenAI",
              intentKeywords: ["Agent"],
              resultMode: "asset_population",
              resultModeLabel: "公司级资产视图",
              rankedCandidateCount: 0,
              assetPopulationCount: 597,
              totalCandidates: 597,
              totalEvidence: 0,
              manualReviewCount: 0,
              layers: [],
              groups: [],
              candidates: [],
              resultViewLifecycle: lifecycle,
            };
            const emptyRows = { ...baseDashboard, totalCandidates: 0, assetPopulationCount: 0, candidates: [] };
            const partialRows = { ...baseDashboard, totalCandidates: 96, assetPopulationCount: 96, candidates: Array.from({ length: 96 }, (_, index) => candidate(index)) };
            const completeRows = { ...baseDashboard, candidates: Array.from({ length: 597 }, (_, index) => candidate(index)) };
            const cases = [emptyRows, partialRows, completeRows].map((dashboard) => ({
              expectedCount: dashboardExpectedCandidateCount(dashboard),
              hasPublicCount: dashboardHasPublicCandidateCount(dashboard),
              hasRenderableCandidates: dashboardHasRenderableCandidates(dashboard),
              hydrationPending: dashboardCandidateHydrationPending(dashboard),
              bannerVisibleWhenIdle: dashboardCandidateHydrationBannerVisible(dashboard, false),
              bannerVisibleWhenActive: dashboardCandidateHydrationBannerVisible(dashboard, true),
              bootstrapping: dashboardCandidateBoardBootstrapping(dashboard),
            }));
            console.log(JSON.stringify(cases));
            """
        )
        completed = subprocess.run(
            ["node", "-e", script],
            cwd=REPO_ROOT,
            check=True,
            text=True,
            capture_output=True,
        )
        return json.loads(completed.stdout)

    def test_lifecycle_count_drives_hydration_before_rows_are_renderable(self) -> None:
        empty_rows, partial_rows, complete_rows = self._run_hydration_cases()

        self.assertEqual(empty_rows["expectedCount"], 597)
        self.assertTrue(empty_rows["hasPublicCount"])
        self.assertFalse(empty_rows["hasRenderableCandidates"])
        self.assertTrue(empty_rows["hydrationPending"])
        self.assertTrue(empty_rows["bootstrapping"])

        self.assertEqual(partial_rows["expectedCount"], 597)
        self.assertTrue(partial_rows["hasRenderableCandidates"])
        self.assertTrue(partial_rows["hydrationPending"])
        self.assertFalse(partial_rows["bannerVisibleWhenIdle"])
        self.assertFalse(partial_rows["bannerVisibleWhenActive"])
        self.assertFalse(partial_rows["bootstrapping"])

        self.assertEqual(complete_rows["expectedCount"], 597)
        self.assertTrue(complete_rows["hasRenderableCandidates"])
        self.assertFalse(complete_rows["hydrationPending"])
        self.assertFalse(complete_rows["bannerVisibleWhenIdle"])
        self.assertFalse(complete_rows["bootstrapping"])

    def test_active_hydration_banner_remains_visible_before_terminal_serving(self) -> None:
        if shutil.which("node") is None:
            self.skipTest("node is required for frontend TypeScript helper checks")
        script = textwrap.dedent(
            """
            const fs = require("fs");
            const path = require("path");
            const vm = require("vm");
            const ts = require("./frontend-demo/node_modules/typescript");
            const source = fs.readFileSync(
              path.join(process.cwd(), "frontend-demo/src/lib/dashboardHydration.ts"),
              "utf8",
            );
            const compiled = ts.transpileModule(source, {
              compilerOptions: { module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2020 },
            }).outputText;
            const module = { exports: {} };
            vm.runInNewContext(compiled, { module, exports: module.exports, require, console }, {
              filename: "dashboardHydration.js",
            });
            const { dashboardCandidateHydrationBannerVisible } = module.exports;
            const dashboard = {
              totalCandidates: 597,
              assetPopulationCount: 597,
              candidates: Array.from({ length: 96 }, (_, index) => ({ id: `candidate-${index}` })),
              resultViewLifecycle: {
                state: "current_snapshot_materializing",
                servedCandidateCount: 300,
                expectedCandidateCount: 597,
              },
            };
            console.log(JSON.stringify({
              idle: dashboardCandidateHydrationBannerVisible(dashboard, false),
              active: dashboardCandidateHydrationBannerVisible(dashboard, true),
            }));
            """
        )
        completed = subprocess.run(
            ["node", "-e", script],
            cwd=REPO_ROOT,
            check=True,
            text=True,
            capture_output=True,
        )
        payload = json.loads(completed.stdout)
        self.assertTrue(payload["idle"])
        self.assertTrue(payload["active"])

    def test_board_runtime_requires_display_ready_cards_for_renderability(self) -> None:
        if shutil.which("node") is None:
            self.skipTest("node is required for frontend TypeScript helper checks")
        script = textwrap.dedent(
            """
            const fs = require("fs");
            const path = require("path");
            const vm = require("vm");
            const ts = require("./frontend-demo/node_modules/typescript");
            const source = fs.readFileSync(
              path.join(process.cwd(), "frontend-demo/src/lib/dashboardHydration.ts"),
              "utf8",
            );
            const compiled = ts.transpileModule(source, {
              compilerOptions: { module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2020 },
            }).outputText;
            const module = { exports: {} };
            vm.runInNewContext(compiled, { module, exports: module.exports, require, console }, {
              filename: "dashboardHydration.js",
            });
            const {
              dashboardCandidateBoardBootstrapping,
              dashboardHasRenderableCandidates,
            } = module.exports;
            const candidate = (index) => ({ id: `candidate-${index}` });
            const base = {
              totalCandidates: 5,
              assetPopulationCount: 5,
              candidates: [candidate(1), candidate(2)],
              boardRuntimeState: {
                publicationStatus: "partial",
                expectedCandidateCount: 5,
                publishedCandidateCount: 2,
                rowHydrationTargetCount: 2,
                displayReadyCandidateCount: 0,
                previewCandidateCount: 2,
              },
            };
            const displayReady = {
              ...base,
              boardRuntimeState: {
                ...base.boardRuntimeState,
                displayReadyCandidateCount: 2,
                previewCandidateCount: 0,
              },
            };
            console.log(JSON.stringify({
              previewRenderable: dashboardHasRenderableCandidates(base),
              previewBootstrapping: dashboardCandidateBoardBootstrapping(base),
              displayReadyRenderable: dashboardHasRenderableCandidates(displayReady),
              displayReadyBootstrapping: dashboardCandidateBoardBootstrapping(displayReady),
            }));
            """
        )
        completed = subprocess.run(
            ["node", "-e", script],
            cwd=REPO_ROOT,
            check=True,
            text=True,
            capture_output=True,
        )
        payload = json.loads(completed.stdout)
        self.assertTrue(payload["previewRenderable"])
        self.assertFalse(payload["previewBootstrapping"])
        self.assertTrue(payload["displayReadyRenderable"])
        self.assertFalse(payload["displayReadyBootstrapping"])

    def test_post_result_layering_uses_terminal_serving_hydration_contract(self) -> None:
        if shutil.which("node") is None:
            self.skipTest("node is required for frontend TypeScript helper checks")
        script = textwrap.dedent(
            """
            const fs = require("fs");
            const path = require("path");
            const vm = require("vm");
            const ts = require("./frontend-demo/node_modules/typescript");
            const source = fs.readFileSync(
              path.join(process.cwd(), "frontend-demo/src/lib/dashboardHydration.ts"),
              "utf8",
            );
            const compiled = ts.transpileModule(source, {
              compilerOptions: { module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2020 },
            }).outputText;
            const module = { exports: {} };
            vm.runInNewContext(compiled, { module, exports: module.exports, require, console }, {
              filename: "dashboardHydration.js",
            });
            const { dashboardCandidateHydrationBannerVisible } = module.exports;
            const dashboard = {
              totalCandidates: 597,
              assetPopulationCount: 597,
              candidates: Array.from({ length: 96 }, (_, index) => ({ id: `candidate-${index}` })),
              resultViewLifecycle: {
                state: "post_result_layering",
                servedCandidateCount: 597,
                expectedCandidateCount: 597,
                outreachLayeringStatus: "running",
              },
            };
            console.log(JSON.stringify({
              idle: dashboardCandidateHydrationBannerVisible(dashboard, false),
              active: dashboardCandidateHydrationBannerVisible(dashboard, true),
            }));
            """
        )
        completed = subprocess.run(
            ["node", "-e", script],
            cwd=REPO_ROOT,
            check=True,
            text=True,
            capture_output=True,
        )
        payload = json.loads(completed.stdout)
        self.assertFalse(payload["idle"])
        self.assertFalse(payload["active"])

    def test_board_runtime_row_hydration_target_ignores_legacy_published_candidate_count(self) -> None:
        if shutil.which("node") is None:
            self.skipTest("node is required for frontend TypeScript helper checks")
        script = textwrap.dedent(
            """
            const fs = require("fs");
            const path = require("path");
            const vm = require("vm");
            const ts = require("./frontend-demo/node_modules/typescript");
            const source = fs.readFileSync(
              path.join(process.cwd(), "frontend-demo/src/lib/dashboardHydration.ts"),
              "utf8",
            );
            const compiled = ts.transpileModule(source, {
              compilerOptions: { module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2020 },
            }).outputText;
            const module = { exports: {} };
            vm.runInNewContext(compiled, { module, exports: module.exports, require, console }, {
              filename: "dashboardHydration.js",
            });
            const {
              dashboardCandidateHydrationPending,
              dashboardExpectedCandidateCount,
              dashboardRowHydrationTargetCount,
            } = module.exports;
            const candidate = (index) => ({ id: `candidate-${index}` });
            const dashboard = (loadedCount) => ({
              totalCandidates: 597,
              assetPopulationCount: 322,
              candidates: Array.from({ length: loadedCount }, (_, index) => candidate(index)),
              boardRuntimeState: {
                publicationStatus: "partial",
                expectedCandidateCount: 597,
                publishedCandidateCount: 322,
                rowHydrationTargetCount: 0,
              },
            });
            console.log(JSON.stringify({
              expected: dashboardExpectedCandidateCount(dashboard(96)),
              rowHydrationTarget: dashboardRowHydrationTargetCount(dashboard(96)),
              partialPending: dashboardCandidateHydrationPending(dashboard(96)),
            }));
            """
        )
        completed = subprocess.run(
            ["node", "-e", script],
            cwd=REPO_ROOT,
            check=True,
            text=True,
            capture_output=True,
        )
        payload = json.loads(completed.stdout)
        self.assertEqual(payload["expected"], 597)
        self.assertEqual(payload["rowHydrationTarget"], 0)
        self.assertFalse(payload["partialPending"])

    def test_board_runtime_served_count_ignores_legacy_published_candidate_count(self) -> None:
        if shutil.which("node") is None:
            self.skipTest("node is required for frontend TypeScript helper checks")
        script = textwrap.dedent(
            """
            const fs = require("fs");
            const path = require("path");
            const vm = require("vm");
            const ts = require("./frontend-demo/node_modules/typescript");
            const source = fs.readFileSync(
              path.join(process.cwd(), "frontend-demo/src/lib/dashboardHydration.ts"),
              "utf8",
            );
            const compiled = ts.transpileModule(source, {
              compilerOptions: { module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2020 },
            }).outputText;
            const module = { exports: {} };
            vm.runInNewContext(compiled, { module, exports: module.exports, require, console }, {
              filename: "dashboardHydration.js",
            });
            const { dashboardServedCandidateCount } = module.exports;
            const dashboard = {
              totalCandidates: 322,
              assetPopulationCount: 322,
              candidates: [],
              boardRuntimeState: {
                publicationStatus: "partial",
                expectedCandidateCount: 300,
                servedCandidateCount: 0,
                publishedCandidateCount: 322,
                rowHydrationTargetCount: 322,
              },
            };
            console.log(JSON.stringify({
              served: dashboardServedCandidateCount(dashboard),
            }));
            """
        )
        completed = subprocess.run(
            ["node", "-e", script],
            cwd=REPO_ROOT,
            check=True,
            text=True,
            capture_output=True,
        )
        payload = json.loads(completed.stdout)
        self.assertEqual(payload["served"], 0)

    def test_board_runtime_expected_count_overrides_stale_legacy_totals(self) -> None:
        if shutil.which("node") is None:
            self.skipTest("node is required for frontend TypeScript helper checks")
        script = textwrap.dedent(
            """
            const fs = require("fs");
            const path = require("path");
            const vm = require("vm");
            const ts = require("./frontend-demo/node_modules/typescript");
            const source = fs.readFileSync(
              path.join(process.cwd(), "frontend-demo/src/lib/dashboardHydration.ts"),
              "utf8",
            );
            const compiled = ts.transpileModule(source, {
              compilerOptions: { module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2020 },
            }).outputText;
            const module = { exports: {} };
            vm.runInNewContext(compiled, { module, exports: module.exports, require, console }, {
              filename: "dashboardHydration.js",
            });
            const { dashboardExpectedCandidateCount, dashboardRowHydrationTargetCount } = module.exports;
            const dashboard = {
              totalCandidates: 1110,
              assetPopulationCount: 1110,
              candidates: [],
              resultViewLifecycle: {
                servedCandidateCount: 1110,
                expectedCandidateCount: 1110,
              },
              boardRuntimeState: {
                publicationStatus: "partial",
                expectedCandidateCount: 597,
                rowHydrationTargetCount: 300,
              },
            };
            console.log(JSON.stringify({
              expected: dashboardExpectedCandidateCount(dashboard),
              rowHydrationTarget: dashboardRowHydrationTargetCount(dashboard),
            }));
            """
        )
        completed = subprocess.run(
            ["node", "-e", script],
            cwd=REPO_ROOT,
            check=True,
            text=True,
            capture_output=True,
        )
        payload = json.loads(completed.stdout)
        self.assertEqual(payload["expected"], 597)
        self.assertEqual(payload["rowHydrationTarget"], 300)

    def test_board_runtime_expected_count_stays_canonical_when_row_hydration_target_is_higher(self) -> None:
        if shutil.which("node") is None:
            self.skipTest("node is required for frontend TypeScript helper checks")
        script = textwrap.dedent(
            """
            const fs = require("fs");
            const path = require("path");
            const vm = require("vm");
            const ts = require("./frontend-demo/node_modules/typescript");
            const source = fs.readFileSync(
              path.join(process.cwd(), "frontend-demo/src/lib/dashboardHydration.ts"),
              "utf8",
            );
            const compiled = ts.transpileModule(source, {
              compilerOptions: { module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2020 },
            }).outputText;
            const module = { exports: {} };
            vm.runInNewContext(compiled, { module, exports: module.exports, require, console }, {
              filename: "dashboardHydration.js",
            });
            const { dashboardExpectedCandidateCount, dashboardRowHydrationTargetCount } = module.exports;
            const dashboard = {
              totalCandidates: 322,
              assetPopulationCount: 322,
              candidates: [],
              resultViewLifecycle: {
                state: "current_snapshot_serving",
                servedCandidateCount: 322,
                expectedCandidateCount: 322,
              },
              boardRuntimeState: {
                publicationStatus: "partial",
                expectedCandidateCount: 300,
                servedCandidateCount: 300,
                rowHydrationTargetCount: 322,
                displayReadyCandidateCount: 120,
              },
            };
            console.log(JSON.stringify({
              expected: dashboardExpectedCandidateCount(dashboard),
              rowHydrationTarget: dashboardRowHydrationTargetCount(dashboard),
            }));
            """
        )
        completed = subprocess.run(
            ["node", "-e", script],
            cwd=REPO_ROOT,
            check=True,
            text=True,
            capture_output=True,
        )
        payload = json.loads(completed.stdout)
        self.assertEqual(payload["expected"], 300)
        self.assertEqual(payload["rowHydrationTarget"], 322)


if __name__ == "__main__":
    unittest.main()
