from __future__ import annotations

import json
import shutil
import subprocess
import textwrap
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]


class FrontendCandidateFiltersTest(unittest.TestCase):
    def test_excel_intake_job_scoped_marker_becomes_recall_bucket(self) -> None:
        if shutil.which("node") is None:
            self.skipTest("node is required for frontend TypeScript helper checks")
        script = textwrap.dedent(
            """
            const fs = require("fs");
            const path = require("path");
            const vm = require("vm");
            const ts = require("./frontend-demo/node_modules/typescript");
            const source = fs.readFileSync(
              path.join(process.cwd(), "frontend-demo/src/lib/candidateFilters.ts"),
              "utf8",
            );
            const compiled = ts.transpileModule(source, {
              compilerOptions: { module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2020 },
            }).outputText;
            const module = { exports: {} };
            vm.runInNewContext(compiled, { module, exports: module.exports, require, console }, {
              filename: "candidateFilters.js",
            });
            const {
              CURRENT_EXCEL_INTAKE_RECALL_BUCKET_ID,
              buildRecallBucketOptions,
              filterCandidatesByFacets,
            } = module.exports;
            const candidates = [
              {
                id: "openai-import-1",
                name: "Ada Import",
                headline: "Research Engineer",
                summary: "",
                currentCompany: "OpenAI",
                notesSnippet: "",
                team: "Unknown",
                focusAreas: [],
                matchReasons: [],
                education: [],
                experience: [],
                matchedKeywords: ["本次Excel导入"],
                sourceMatches: [
                  {
                    field: "job_scoped_candidate_marker",
                    matched_on: "本次Excel导入",
                    source_type: "excel_intake:current_job",
                  },
                ],
                employmentStatus: "current",
                outreachLayer: null,
                functionIds: [],
              },
              {
                id: "openai-baseline-1",
                name: "Grace Baseline",
                headline: "Agent researcher",
                summary: "",
                currentCompany: "OpenAI",
                notesSnippet: "",
                team: "Unknown",
                focusAreas: [],
                matchReasons: [],
                education: [],
                experience: [],
                matchedKeywords: ["Agent"],
                sourceMatches: [],
                employmentStatus: "current",
                outreachLayer: null,
                functionIds: [],
              },
            ];
            const options = buildRecallBucketOptions(candidates, ["Agent"]);
            const excelOption = options.find((option) => option.id === CURRENT_EXCEL_INTAKE_RECALL_BUCKET_ID);
            const filtered = filterCandidatesByFacets(
              candidates,
              {
                layers: [],
                recallBuckets: [CURRENT_EXCEL_INTAKE_RECALL_BUCKET_ID],
                employmentStatuses: [],
                locations: [],
                functionBuckets: [],
                searchKeyword: "",
              },
              ["Agent"],
            );
            console.log(JSON.stringify({
              excelOption,
              filteredIds: filtered.map((candidate) => candidate.id),
            }));
            """
        )
        completed = subprocess.run(
            ["node", "-e", script],
            cwd=REPO_ROOT,
            text=True,
            capture_output=True,
            check=True,
        )
        payload = json.loads(completed.stdout)
        self.assertEqual(
            payload["excelOption"],
            {
                "id": "job_scoped_marker:excel_intake:current_job",
                "label": "本次Excel导入",
                "count": 1,
            },
        )
        self.assertEqual(payload["filteredIds"], ["openai-import-1"])

    def test_normalize_facet_selection_preserves_user_choice_when_hydrated_counts_shift(self) -> None:
        if shutil.which("node") is None:
            self.skipTest("node is required for frontend TypeScript helper checks")
        script = textwrap.dedent(
            """
            const fs = require("fs");
            const path = require("path");
            const vm = require("vm");
            const ts = require("./frontend-demo/node_modules/typescript");
            const source = fs.readFileSync(
              path.join(process.cwd(), "frontend-demo/src/lib/candidateFilters.ts"),
              "utf8",
            );
            const compiled = ts.transpileModule(source, {
              compilerOptions: { module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2020 },
            }).outputText;
            const module = { exports: {} };
            vm.runInNewContext(compiled, { module, exports: module.exports, require, console }, {
              filename: "candidateFilters.js",
            });
            const { normalizeFacetSelection, preserveEditedFacetSelection } = module.exports;
            const normalized = normalizeFacetSelection(
              ["research"],
              [
                { id: "research", label: "Researcher", count: 0 },
                { id: "engineering", label: "Engineer", count: 96 },
              ],
              ["engineering"],
            );
            const preservedEdited = preserveEditedFacetSelection(
              ["current"],
              [
                { id: "current", label: "在职", count: 527 },
                { id: "former", label: "已离职", count: 70 },
              ],
            );
            const staleEdited = preserveEditedFacetSelection(
              ["current"],
              [
                { id: "former", label: "已离职", count: 70 },
              ],
            );
            console.log(JSON.stringify({ normalized, preservedEdited, staleEdited }));
            """
        )
        completed = subprocess.run(
            ["node", "-e", script],
            cwd=REPO_ROOT,
            text=True,
            capture_output=True,
            check=True,
        )
        payload = json.loads(completed.stdout)
        self.assertEqual(payload["normalized"], ["research"])
        self.assertEqual(payload["preservedEdited"], ["current"])
        self.assertEqual(payload["staleEdited"], [])

    def test_summarize_selected_facet_treats_all_concrete_options_as_all(self) -> None:
        if shutil.which("node") is None:
            self.skipTest("node is required for frontend TypeScript helper checks")
        script = textwrap.dedent(
            """
            const fs = require("fs");
            const path = require("path");
            const vm = require("vm");
            const ts = require("./frontend-demo/node_modules/typescript");
            const source = fs.readFileSync(
              path.join(process.cwd(), "frontend-demo/src/lib/candidateFilters.ts"),
              "utf8",
            );
            const compiled = ts.transpileModule(source, {
              compilerOptions: { module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2020 },
            }).outputText;
            const module = { exports: {} };
            vm.runInNewContext(compiled, { module, exports: module.exports, require, console }, {
              filename: "candidateFilters.js",
            });
            const { summarizeSelectedFacet } = module.exports;
            const summary = summarizeSelectedFacet(
              ["us", "other", "unknown"],
              [
                { id: "us", label: "美国", count: 248 },
                { id: "other", label: "其他", count: 0 },
                { id: "unknown", label: "未提供地区信息", count: 300 },
              ],
              "全量",
            );
            console.log(JSON.stringify({ summary }));
            """
        )
        completed = subprocess.run(
            ["node", "-e", script],
            cwd=REPO_ROOT,
            text=True,
            capture_output=True,
            check=True,
        )
        payload = json.loads(completed.stdout)
        self.assertEqual(payload["summary"], "全量")

    def test_results_board_uses_canonical_facet_summary_for_global_filters(self) -> None:
        source = (REPO_ROOT / "frontend-demo/src/components/ResultsBoardPanel.tsx").read_text(encoding="utf-8")
        self.assertIn("candidateFacetSummary?.employment", source)
        self.assertIn(
            "summary={facetSummaryLabel(employmentFacetOptions, selectedEmploymentStatuses",
            source,
        )
        self.assertIn("options={employmentFacetOptions}", source)
        self.assertIn("fallback: defaultOpenSelection(employmentFacetOptions)", source)
        self.assertIn("dashboard.boardRuntimeState.facetSummaryStatus === \"complete\"", source)
        self.assertIn('dashboard.boardRuntimeState.facetSummaryScope === "global_full_population"', source)
        self.assertIn("hasCanonicalFacetSummaryForServedPopulation(dashboard, expectedCandidateCount)", source)
        self.assertIn('facetSummaryScope === "global_full_population"', source)
        self.assertIn('facetSummaryScope === "exact_projection"', source)
        self.assertIn("layerZeroCount === expectedCandidateCount", source)
        self.assertIn("summaryCandidateCount >= expectedCandidateCount", source)
        self.assertIn('showCounts={hasGlobalFacetSummary}', source)
        self.assertIn("preserveEditedFacetSelection(current, employmentFacetOptions)", source)
        self.assertIn("canonicalBoardFacetOptions(", source)
        self.assertIn("return hasBoardRuntimeState ? [] : fallback", source)
        self.assertIn("canonicalFacetUnavailableMessage", source)
        self.assertIn("统计未生成", source)
        self.assertIn("selectedBackendFacetFilterIds(selectedEmploymentStatuses, employmentFacetOptions)", source)
        self.assertIn("selectedBackendFacetFilterIds(selectedFunctionBuckets, functionOptions)", source)
        self.assertIn("selectedBackendFacetFilterIds(selectedLocations, locationOptions)", source)
        self.assertIn("resultsBoardFacetSessionState", source)
        self.assertIn("readResultsBoardFacetSessionState(resultsContextKey)", source)
        self.assertIn("showCounts={hasGlobalFacetSummary}", source)
        self.assertIn('[historyId || "no-history", jobId || "no-job", projectionId || "no-projection"].join(":")', source)
        self.assertIn("getCandidateDetailsBatch(missingCandidateIds, jobId)", source)
        self.assertIn("pagedDisplayCandidates", source)
        self.assertIn("groupCandidatesByAuditStatus(pagedDisplayCandidates, reviewStatusMap)", source)
        self.assertIn("currentPageDetailLoading", source)
        self.assertIn("getDashboardCandidatePage(jobId, {", source)
        self.assertIn("filter: backendPageFilter", source)
        self.assertIn("waitingForBackendPage", source)
        self.assertIn("backendCandidatePage?.filteredCandidateCount", source)
        self.assertIn("筛选与分页由后端 canonical 候选人集合计算", source)
        self.assertIn("筛选、计数与分页继续使用已物化的 canonical 看板行", source)
        self.assertNotIn('id !== "needs_profile_completion"', source)
        self.assertIn("return AUDIT_STATUS_OPTIONS.map((item) => item.id);", source)
        self.assertNotIn("if (waitingForBackendPage) {\n      return [];\n    }", source)
        self.assertIn('reviewStatusMap[candidate.id] || candidateAutoReviewStatus(candidate) || "no_review_needed"', source)
        self.assertNotIn("getCandidateDetailsBatch(dashboard.candidates", source)
        self.assertNotIn("getCandidateDetailsBatch(visibleCandidates", source)
        results_page_source = (REPO_ROOT / "frontend-demo/src/pages/ResultsPage.tsx").read_text(encoding="utf-8")
        self.assertIn(
            'key={[context.historyId || "no-history", context.jobId || "no-job", effectiveProjectionId || "no-projection"].join(":")}',
            results_page_source,
        )
        self.assertNotIn(
            'key={[context.historyId || "no-history", context.jobId || "no-job", dashboard.snapshotId',
            results_page_source,
        )
        search_flow_source = (REPO_ROOT / "frontend-demo/src/components/SearchFlow.tsx").read_text(encoding="utf-8")
        self.assertNotIn('dashboard?.snapshotId || "no-snapshot", phase].join(":")', search_flow_source)
        self.assertIn("resultPanelContextWithUserState === panelContextKey", search_flow_source)
        self.assertIn("dashboardHasRenderableCandidates(dashboard)", search_flow_source)
        self.assertFalse((REPO_ROOT / "frontend-demo/src/components/CandidateBoard.tsx").exists())
        self.assertIn(
            "candidateFacetSummarySignature(current) !== candidateFacetSummarySignature(next)",
            (REPO_ROOT / "frontend-demo/src/hooks/useDashboardCandidateHydration.ts").read_text(encoding="utf-8"),
        )
        self.assertNotIn(
            "[dashboard?.resultMode, dashboard?.snapshotId, jobId, onDashboardChange]",
            (REPO_ROOT / "frontend-demo/src/hooks/useDashboardCandidateHydration.ts").read_text(encoding="utf-8"),
        )
        api_source = (REPO_ROOT / "frontend-demo/src/lib/api.ts").read_text(encoding="utf-8")
        self.assertIn("candidateFacetSummaryMatchesCanonicalBoard(", api_source)
        self.assertIn("page.candidateFacetSummaryScope,", api_source)
        self.assertIn("mergedBoardRuntimeState,", api_source)
        self.assertIn("pickCanonicalCandidateFacetSummary(pageFacetSummary, currentFacetSummary)", api_source)
        self.assertIn("mapCandidateFacetSummaryScope(", api_source)
        self.assertIn("asString(payload.facet_summary_scope)", api_source)
        self.assertIn("DashboardCandidatePageFilter", api_source)
        self.assertIn("dashboardCandidatePageFilterSignature", api_source)
        self.assertIn("function_buckets: normalized.functionBuckets.length > 0", api_source)
        self.assertIn("filteredCandidateCount", api_source)
        self.assertIn("mapCandidatePageFilterContract(payload.filter_contract)", api_source)
        self.assertIn('candidate_identity_key: pickFirstString(member, ["candidate_identity_key"])', api_source)
        self.assertIn('pickFirstString(record, ["candidate_identity_key", "person_identity_key", "profile_url_key"])', api_source)
        self.assertIn("deltaProfileDenominatorPromoted", api_source)
        self.assertIn("delta_profile_denominator_promoted", api_source)
        self.assertNotIn("dashboardPromiseCache.delete(cacheKey)", api_source)
        self.assertNotIn("dashboardCandidatePagePromiseCache.delete(cacheKey)", api_source)
        self.assertIn("candidateFacetSummary ? rawCandidateFacetSummaryScope : \"\"", api_source)
        hydration_source = (REPO_ROOT / "frontend-demo/src/hooks/useDashboardCandidateHydration.ts").read_text(
            encoding="utf-8",
        )
        self.assertIn("candidateFacetSummaryScope: refreshedDashboard.candidateFacetSummaryScope", hydration_source)
        self.assertNotIn('candidateFacetSummaryScope: "global_full_population"', hydration_source)

    def test_search_page_preserves_rendered_dashboard_on_transient_result_refresh_failure(self) -> None:
        source = (REPO_ROOT / "frontend-demo/src/pages/SearchPage.tsx").read_text(encoding="utf-8")
        self.assertIn("dashboardBoardRuntimePublicationComplete", source)
        self.assertIn("requireCompleteBoard?: boolean", source)
        self.assertIn("requireCompleteBoard: true", source)
        self.assertIn("const dashboardRef = useRef<DashboardData | null>(dashboard)", source)
        self.assertIn("const latestRenderableDashboardForJob = (jobId: string): DashboardData | null =>", source)
        self.assertIn("const fallbackDashboard = latestRenderableDashboardForJob(jobId)", source)
        self.assertIn("displayReadyCandidateCount > 0", source)
        self.assertIn("hasLegacyStage1PreviewOnly", source)
        self.assertNotIn("publishedCandidateCount > 0", source)
        self.assertIn("persistFlow(", source)
        self.assertIn("setDashboard(fallbackDashboard)", source)
        self.assertIn("fallbackDashboard,", source)
        self.assertIn("setDashboard(null)", source)
        self.assertIn("setErrorMessage(message)", source)

    def test_search_flow_only_auto_advance_results_once_after_renderable_rows(self) -> None:
        source = (REPO_ROOT / "frontend-demo/src/components/SearchFlow.tsx").read_text(encoding="utf-8")
        self.assertIn("const hasRenderableDashboard = dashboardHasRenderableCandidates(dashboard)", source)
        self.assertIn("const resultsTabAutoOpenedRef = useRef(false)", source)
        self.assertIn("defaultActiveStep(phase, hasRenderableDashboard)", source)
        self.assertIn('phase === "running" && awaitingUserAction === "continue_stage2"', source)
        self.assertIn('const shouldAutoOpenResults = hasRenderableDashboard && (phase === "results" || phase === "running");', source)
        self.assertIn("const workflowKey = useMemo(", source)
        self.assertIn("resultsTabAutoOpenedRef.current = false", source)
        self.assertIn("const hasRenderableDashboardRef = useRef(hasRenderableDashboard)", source)
        self.assertIn("hasRenderableDashboardRef.current = hasRenderableDashboard", source)
        self.assertIn("[workflowKey]", source)
        self.assertIn("setActiveStep(defaultActiveStep(phase, hasRenderableDashboardRef.current));", source)
        self.assertNotIn("const resetKey = useMemo(", source)
        self.assertNotIn("[phase, resetKey]", source)
        self.assertNotIn('phase === "results" || (phase === "running" && hasRenderableDashboard)', source)
        self.assertNotIn("[hasRenderableDashboard, phase, resetKey, selectedCandidateId]", source)
        self.assertNotIn("dashboardHasPublicCandidateCount", source)
        self.assertNotIn("stage1PreviewReady", source)

    def test_search_page_refreshes_dashboard_from_any_new_board_patch_batch(self) -> None:
        source = (REPO_ROOT / "frontend-demo/src/pages/SearchPage.tsx").read_text(encoding="utf-8")
        self.assertIn("patchLog.returnedCount <= 0", source)
        self.assertNotIn("patch.displayReadyCandidateCount > 0 || patch.servedCandidateCount > 0", source)
        self.assertNotIn("boardRuntimeHasCards", source)

    def test_global_facet_summary_page_merge_replaces_stale_partial_summary(self) -> None:
        if shutil.which("node") is None:
            self.skipTest("node is required for frontend TypeScript helper checks")
        script = textwrap.dedent(
            """
            const fs = require("fs");
            const path = require("path");
            const vm = require("vm");
            const ts = require("./frontend-demo/node_modules/typescript");

            function loadApiModule() {
              const source = fs
                .readFileSync(path.join(process.cwd(), "frontend-demo/src/lib/api.ts"), "utf8")
                .replaceAll("import.meta.env", "({})");
              const compiled = ts.transpileModule(source, {
                compilerOptions: { module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2020 },
              }).outputText;
              const module = { exports: {} };
              const localRequire = (specifier) => {
                if (specifier === "../data/mockData") {
                  return {
                    mockCandidateDetails: {},
                    mockDashboard: {},
                    mockManualReviewItems: [],
                    mockPlan: {},
                    mockRunStatus: {},
                  };
                }
                if (specifier === "./dashboardHydration") {
                  return {
                    dashboardExpectedCandidateCount: (dashboard) => Math.max(
                      Number(dashboard?.totalCandidates || 0) || 0,
                      Number(dashboard?.assetPopulationCount || 0) || 0,
                      Number(dashboard?.resultViewLifecycle?.expectedCandidateCount || 0) || 0,
                      Number(dashboard?.resultViewLifecycle?.servedCandidateCount || 0) || 0,
                      Number(dashboard?.candidates?.length || 0) || 0,
                    ),
                    dashboardHasRenderableCandidates: (dashboard) => Number(dashboard?.candidates?.length || 0) > 0,
                  };
                }
                if (specifier === "./resultViewLifecycle") {
                  return {
                    lifecycleEffectiveDeltaMaterializedCount: (lifecycle) => {
                      if (!lifecycle || lifecycle.deltaProfileProgressApplicable === false) {
                        return 0;
                      }
                      const materializedCount = Math.max(0, Number(lifecycle.deltaProfileMaterializedCount || 0) || 0);
                      const boardVisibleCount = Math.max(0, Number(lifecycle.deltaProfileBoardVisibleCount || 0) || 0);
                      const currentServing =
                        lifecycle.state === "current_snapshot_serving" ||
                        lifecycle.state === "post_result_layering" ||
                        lifecycle.servingProjectionPhase === "current_snapshot_serving";
                      if (
                        currentServing &&
                        lifecycle.currentSnapshotId &&
                        lifecycle.servedSnapshotId === lifecycle.currentSnapshotId &&
                        Number(lifecycle.servedCandidateCount || 0) >= Number(lifecycle.expectedCandidateCount || 0) &&
                        materializedCount >= Number(lifecycle.deltaProfileRequiredCount || 0)
                      ) {
                        return materializedCount;
                      }
                      return typeof lifecycle.deltaProfileBoardVisibleCount === "number"
                        ? boardVisibleCount
                        : materializedCount;
                    },
                  };
                }
                return require(specifier);
              };
              vm.runInNewContext(
                compiled,
                {
                  module,
                  exports: module.exports,
                  require: localRequire,
                  console,
                  crypto: { randomUUID: () => "test-id" },
                },
                { filename: "api.js" },
              );
              return module.exports;
            }

            const { mergeDashboardCandidatePage } = loadApiModule();
            const dashboard = {
              title: "OpenAI Agent",
              snapshotId: "current",
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
              candidateFacetSummary: {
                candidateCount: 548,
                locations: [
                  { id: "us", label: "美国", count: 248 },
                  { id: "other", label: "其他", count: 0 },
                  { id: "unknown", label: "未提供地区信息", count: 300 },
                ],
              },
              resultViewLifecycle: {
                state: "current_snapshot_serving",
                servedCandidateCount: 597,
                expectedCandidateCount: 597,
              },
            };
            const page = {
              jobId: "job-openai",
              resultMode: "asset_population",
              offset: 0,
              limit: 96,
              returnedCount: 96,
              totalCandidates: 597,
              hasMore: true,
              nextOffset: 96,
              candidates: [],
              candidateFacetSummaryScope: "global_full_population",
              candidateFacetSummary: {
                candidateCount: 597,
                locations: [
                  { id: "us", label: "美国", count: 93 },
                  { id: "other", label: "其他", count: 0 },
                  { id: "unknown", label: "未提供地区信息", count: 504 },
                ],
              },
            };
            const merged = mergeDashboardCandidatePage(dashboard, page);
            console.log(JSON.stringify({
              candidateCount: merged.candidateFacetSummary.candidateCount,
              locations: merged.candidateFacetSummary.locations,
              scope: merged.candidateFacetSummaryScope,
            }));
            """
        )
        completed = subprocess.run(
            ["node", "-e", script],
            cwd=REPO_ROOT,
            text=True,
            capture_output=True,
            check=True,
        )
        payload = json.loads(completed.stdout)
        self.assertEqual(payload["candidateCount"], 597)
        self.assertEqual(
            payload["locations"],
            [
                {"id": "us", "label": "美国", "count": 93},
                {"id": "other", "label": "其他", "count": 0},
                {"id": "unknown", "label": "未提供地区信息", "count": 504},
            ],
        )
        self.assertEqual(payload["scope"], "global_full_population")

    def test_progress_merge_does_not_regress_complete_board_runtime_contract(self) -> None:
        if shutil.which("node") is None:
            self.skipTest("node is required for frontend TypeScript helper checks")
        script = textwrap.dedent(
            """
            const fs = require("fs");
            const path = require("path");
            const vm = require("vm");
            const ts = require("./frontend-demo/node_modules/typescript");
            const source = fs
              .readFileSync(path.join(process.cwd(), "frontend-demo/src/lib/api.ts"), "utf8")
              .replaceAll("import.meta.env", "({})");
            const compiled = ts.transpileModule(source, {
              compilerOptions: { module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2020 },
            }).outputText;
            const module = { exports: {} };
            const localRequire = (specifier) => {
              if (specifier === "../data/mockData") {
                return {
                  mockCandidateDetails: {},
                  mockDashboard: {},
                  mockManualReviewItems: [],
                  mockPlan: {},
                  mockRunStatus: {},
                };
              }
              if (specifier === "./dashboardHydration") {
                return {
                  dashboardExpectedCandidateCount: (dashboard) => Math.max(
                    Number(dashboard?.boardRuntimeState?.expectedCandidateCount || 0) || 0,
                    Number(dashboard?.totalCandidates || 0) || 0,
                    Number(dashboard?.assetPopulationCount || 0) || 0,
                    Number(dashboard?.resultViewLifecycle?.expectedCandidateCount || 0) || 0,
                    Number(dashboard?.resultViewLifecycle?.servedCandidateCount || 0) || 0,
                    Number(dashboard?.candidates?.length || 0) || 0,
                  ),
                  dashboardHasRenderableCandidates: (dashboard) =>
                    Number(dashboard?.boardRuntimeState?.displayReadyCandidateCount || 0) > 0 ||
                    Number(dashboard?.candidates?.length || 0) > 0,
                  dashboardRowHydrationTargetCount: (dashboard) =>
                    Number(dashboard?.boardRuntimeState?.rowHydrationTargetCount || 0) || 0,
                };
              }
              if (specifier === "./resultViewLifecycle") {
                return {
                  lifecycleEffectiveDeltaMaterializedCount: (lifecycle) =>
                    Number(lifecycle?.deltaProfileBoardVisibleCount ?? lifecycle?.deltaProfileMaterializedCount ?? 0) || 0,
                };
              }
              return require(specifier);
            };
            vm.runInNewContext(
              compiled,
              {
                module,
                exports: module.exports,
                require: localRequire,
                console,
                crypto: { randomUUID: () => "test-id" },
              },
              { filename: "api.js" },
            );
            const { mergeDashboardRuntimeProgress } = module.exports;
            const completeBoard = {
              schemaVersion: 1,
              jobId: "job-openai-agent",
              resultMode: "asset_population",
              phase: "current_snapshot_serving",
              publicationStatus: "complete",
              expectedCandidateCount: 597,
              servedCandidateCount: 597,
              publishedCandidateCount: 597,
              displayReadyCandidateCount: 597,
              previewCandidateCount: 0,
              profileDetailCandidateCount: 597,
              explicitProfileCaptureCandidateCount: 297,
              needsProfileCompletionCandidateCount: 0,
              lowProfileRichnessCandidateCount: 0,
              cardMaterializationQualityFieldsAvailable: true,
              rowHydrationTargetCount: 597,
              baselineCandidateCount: 300,
              deltaProfileRequiredCount: 297,
              deltaProfileFetchedCount: 297,
              deltaProfileMaterializedCount: 297,
              deltaProfileBoardVisibleCount: 297,
              rowPublicationSequence: 10,
              rowPublicationTier: "current_snapshot_serving",
              rowPublicationWatermark: "snapshot|10|597|completed",
              rowPublicationUpdatedAt: "2026-05-05T17:29:51Z",
              facetSummaryStatus: "complete",
              facetSummaryScope: "global_full_population",
              facetSummaryCandidateCount: 597,
              layeringStatus: "completed",
              syncStatusText: "597/597",
              profileFetchStatusText: "新增 LinkedIn Profile 已取回 297/297",
              cardMaterializationStatusText: "卡片详情已合入看板 297/297",
              noteText: "",
            };
            const dashboard = {
              title: "OpenAI Agent",
              snapshotId: "snapshot",
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
              boardRuntimeState: completeBoard,
            };
            const staleProgress = {
              boardRuntimeState: {
                ...completeBoard,
                phase: "post_result_layering",
                facetSummaryStatus: "pending",
                facetSummaryScope: "",
                facetSummaryCandidateCount: 0,
                layeringStatus: "running",
                rowPublicationWatermark: "snapshot|10|597|running",
              },
            };
            const merged = mergeDashboardRuntimeProgress(dashboard, staleProgress);
            console.log(JSON.stringify(merged.boardRuntimeState));
            """
        )
        completed = subprocess.run(
            ["node", "-e", script],
            cwd=REPO_ROOT,
            text=True,
            capture_output=True,
            check=True,
        )
        board_runtime = json.loads(completed.stdout)
        self.assertEqual(board_runtime["phase"], "current_snapshot_serving")
        self.assertEqual(board_runtime["facetSummaryStatus"], "complete")
        self.assertEqual(board_runtime["facetSummaryScope"], "global_full_population")
        self.assertEqual(board_runtime["facetSummaryCandidateCount"], 597)
        self.assertEqual(board_runtime["layeringStatus"], "completed")

    def test_board_runtime_final_projection_outranks_newer_partial_sequence(self) -> None:
        if shutil.which("node") is None:
            self.skipTest("node is required for frontend TypeScript helper checks")
        script = textwrap.dedent(
            """
            const fs = require("fs");
            const path = require("path");
            const vm = require("vm");
            const ts = require("./frontend-demo/node_modules/typescript");
            const source = fs
              .readFileSync(path.join(process.cwd(), "frontend-demo/src/lib/api.ts"), "utf8")
              .replaceAll("import.meta.env", "({})");
            const compiled = ts.transpileModule(source, {
              compilerOptions: { module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2020 },
            }).outputText;
            const module = { exports: {} };
            const localRequire = (specifier) => {
              if (specifier === "../data/mockData") {
                return { mockCandidateDetails: {}, mockDashboard: {}, mockManualReviewItems: [], mockPlan: {}, mockRunStatus: {} };
              }
              if (specifier === "./dashboardHydration") {
                return {
                  dashboardExpectedCandidateCount: (dashboard) => Number(dashboard?.boardRuntimeState?.expectedCandidateCount || 0) || 0,
                  dashboardHasRenderableCandidates: (dashboard) => Number(dashboard?.boardRuntimeState?.displayReadyCandidateCount || 0) > 0,
                  dashboardRowHydrationTargetCount: (dashboard) => Number(dashboard?.boardRuntimeState?.rowHydrationTargetCount || 0) || 0,
                };
              }
              if (specifier === "./resultViewLifecycle") {
                return { lifecycleEffectiveDeltaMaterializedCount: (lifecycle) => Number(lifecycle?.deltaProfileBoardVisibleCount || 0) || 0 };
              }
              return require(specifier);
            };
            vm.runInNewContext(
              compiled,
              { module, exports: module.exports, require: localRequire, console, crypto: { randomUUID: () => "test-id" } },
              { filename: "api.js" },
            );
            const { mergeDashboardRuntimeProgress } = module.exports;
            const cachedBoard = {
              schemaVersion: 1,
              jobId: "job-openai",
              resultMode: "asset_population",
              phase: "current_snapshot_serving",
              publicationStatus: "complete",
              expectedCandidateCount: 597,
              servedCandidateCount: 597,
              publishedCandidateCount: 597,
              displayReadyCandidateCount: 597,
              previewCandidateCount: 0,
              profileDetailCandidateCount: 597,
              explicitProfileCaptureCandidateCount: 297,
              needsProfileCompletionCandidateCount: 0,
              lowProfileRichnessCandidateCount: 0,
              cardMaterializationQualityFieldsAvailable: true,
              rowHydrationTargetCount: 597,
              baselineCandidateCount: 300,
              rowPublicationSequence: 10,
              rowPublicationTier: "current_snapshot_serving",
              rowPublicationWatermark: "snapshot|10|597|completed",
              facetSummaryStatus: "complete",
              facetSummaryScope: "global_full_population",
              facetSummaryCandidateCount: 597,
              layeringStatus: "completed",
            };
            const correctedBoard = {
              ...cachedBoard,
              phase: "partial_serving",
              publicationStatus: "partial",
              expectedCandidateCount: 300,
              servedCandidateCount: 300,
              publishedCandidateCount: 300,
              displayReadyCandidateCount: 120,
              rowHydrationTargetCount: 300,
              rowPublicationSequence: 11,
              rowPublicationTier: "partial_patch",
              rowPublicationWatermark: "snapshot|11|300|running",
              facetSummaryStatus: "pending",
              facetSummaryCandidateCount: 300,
              layeringStatus: "running",
            };
            const dashboard = {
              title: "OpenAI Agent",
              snapshotId: "snapshot",
              queryLabel: "OpenAI Agent",
              targetCompany: "OpenAI",
              resultMode: "asset_population",
              resultModeLabel: "company assets",
              rankedCandidateCount: 0,
              assetPopulationCount: 597,
              totalCandidates: 597,
              totalEvidence: 0,
              manualReviewCount: 0,
              layers: [],
              groups: [],
              candidates: [],
              boardRuntimeState: cachedBoard,
            };
            const merged = mergeDashboardRuntimeProgress(dashboard, { boardRuntimeState: correctedBoard });
            console.log(JSON.stringify({
              expected: merged.boardRuntimeState.expectedCandidateCount,
              phase: merged.boardRuntimeState.phase,
              total: merged.totalCandidates,
            }));
            """
        )
        completed = subprocess.run(
            ["node", "-e", script],
            cwd=REPO_ROOT,
            text=True,
            capture_output=True,
            check=True,
        )
        payload = json.loads(completed.stdout)
        self.assertEqual(payload["expected"], 597)
        self.assertEqual(payload["phase"], "current_snapshot_serving")
        self.assertEqual(payload["total"], 597)

    def test_board_runtime_total_candidates_stays_canonical_when_published_is_higher(self) -> None:
        if shutil.which("node") is None:
            self.skipTest("node is required for frontend TypeScript helper checks")
        script = textwrap.dedent(
            """
            const fs = require("fs");
            const path = require("path");
            const vm = require("vm");
            const ts = require("./frontend-demo/node_modules/typescript");

            function loadApiModule() {
              const source = fs
                .readFileSync(path.join(process.cwd(), "frontend-demo/src/lib/api.ts"), "utf8")
                .replaceAll("import.meta.env", "({})");
              const compiled = ts.transpileModule(source, {
                compilerOptions: { module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2020 },
              }).outputText;
              const module = { exports: {} };
              const localRequire = (specifier) => {
                if (specifier === "../data/mockData") {
                  return {
                    mockCandidateDetails: {},
                    mockDashboard: {},
                    mockManualReviewItems: [],
                    mockPlan: {},
                    mockRunStatus: {},
                  };
                }
                if (specifier === "./dashboardHydration") {
                  return {
                    dashboardExpectedCandidateCount: (dashboard) => Math.max(
                      Number(dashboard?.boardRuntimeState?.expectedCandidateCount || 0) || 0,
                      Number(dashboard?.totalCandidates || 0) || 0,
                      Number(dashboard?.assetPopulationCount || 0) || 0,
                      Number(dashboard?.resultViewLifecycle?.expectedCandidateCount || 0) || 0,
                      Number(dashboard?.resultViewLifecycle?.servedCandidateCount || 0) || 0,
                      Number(dashboard?.candidates?.length || 0) || 0,
                    ),
                    dashboardHasRenderableCandidates: (dashboard) => Number(dashboard?.candidates?.length || 0) > 0,
                    dashboardRowHydrationTargetCount: (dashboard) =>
                      Number(dashboard?.boardRuntimeState?.rowHydrationTargetCount || 0) || 0,
                  };
                }
                if (specifier === "./resultViewLifecycle") {
                  return {
                    lifecycleEffectiveDeltaMaterializedCount: (lifecycle) =>
                      Number(lifecycle?.deltaProfileBoardVisibleCount ?? lifecycle?.deltaProfileMaterializedCount ?? 0) || 0,
                  };
                }
                return require(specifier);
              };
              vm.runInNewContext(
                compiled,
                {
                  module,
                  exports: module.exports,
                  require: localRequire,
                  console,
                  crypto: { randomUUID: () => "test-id" },
                },
                { filename: "api.js" },
              );
              return module.exports;
            }

            const { mergeDashboardCandidatePage, mergeDashboardRuntimeProgress } = loadApiModule();
            const boardRuntimeState = {
              schemaVersion: 1,
              jobId: "job-openai",
              resultMode: "asset_population",
              phase: "partial_serving",
              publicationStatus: "partial",
              expectedCandidateCount: 300,
              servedCandidateCount: 300,
              publishedCandidateCount: 322,
              displayReadyCandidateCount: 120,
              previewCandidateCount: 202,
              profileDetailCandidateCount: 120,
              explicitProfileCaptureCandidateCount: 120,
              needsProfileCompletionCandidateCount: 202,
              lowProfileRichnessCandidateCount: 0,
              cardMaterializationQualityFieldsAvailable: true,
              rowHydrationTargetCount: 322,
              baselineCandidateCount: 0,
              deltaProfileRequiredCount: 0,
              deltaProfileFetchedCount: 0,
              deltaProfileMaterializedCount: 0,
              deltaProfileBoardVisibleCount: 0,
              rowPublicationSequence: 7,
              rowPublicationWatermark: "snapshot|7|322|partial",
              rowPublicationUpdatedAt: "2026-05-06T00:00:00Z",
              facetSummaryStatus: "pending",
              facetSummaryScope: "global_full_population",
              facetSummaryCandidateCount: 300,
              layeringStatus: "running",
              syncStatusText: "120/300",
              profileFetchStatusText: "本次 LinkedIn Profile 已取回 120/300",
              cardMaterializationStatusText: "卡片详情已合入看板 120/300",
              noteText: "",
            };
            const dashboard = {
              title: "OpenAI Agent",
              snapshotId: "snapshot",
              queryLabel: "OpenAI Agent",
              targetCompany: "OpenAI",
              intentKeywords: ["Agent"],
              resultMode: "asset_population",
              resultModeLabel: "公司级资产视图",
              rankedCandidateCount: 0,
              assetPopulationCount: 300,
              totalCandidates: 300,
              totalEvidence: 0,
              manualReviewCount: 0,
              layers: [],
              groups: [],
              candidates: [],
              boardRuntimeState,
            };
            const page = {
              jobId: "job-openai",
              resultMode: "asset_population",
              offset: 0,
              limit: 24,
              returnedCount: 24,
              totalCandidates: 322,
              filteredCandidateCount: 322,
              hasMore: true,
              nextOffset: 24,
              candidates: [],
              boardRuntimeState,
            };
            const mergedPage = mergeDashboardCandidatePage(dashboard, page);
            const mergedProgress = mergeDashboardRuntimeProgress(dashboard, { boardRuntimeState });
            console.log(JSON.stringify({
              pageTotal: mergedPage.totalCandidates,
              pageAsset: mergedPage.assetPopulationCount,
              progressTotal: mergedProgress.totalCandidates,
              progressAsset: mergedProgress.assetPopulationCount,
            }));
            """
        )
        completed = subprocess.run(
            ["node", "-e", script],
            cwd=REPO_ROOT,
            text=True,
            capture_output=True,
            check=True,
        )
        payload = json.loads(completed.stdout)
        self.assertEqual(payload["pageTotal"], 300)
        self.assertEqual(payload["pageAsset"], 300)
        self.assertEqual(payload["progressTotal"], 300)
        self.assertEqual(payload["progressAsset"], 300)

    def test_board_runtime_drops_stale_partial_facet_summary(self) -> None:
        if shutil.which("node") is None:
            self.skipTest("node is required for frontend TypeScript helper checks")
        script = textwrap.dedent(
            """
            const fs = require("fs");
            const path = require("path");
            const vm = require("vm");
            const ts = require("./frontend-demo/node_modules/typescript");

            function loadApiModule() {
              const source = fs
                .readFileSync(path.join(process.cwd(), "frontend-demo/src/lib/api.ts"), "utf8")
                .replaceAll("import.meta.env", "({})");
              const compiled = ts.transpileModule(source, {
                compilerOptions: { module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2020 },
              }).outputText;
              const module = { exports: {} };
              const localRequire = (specifier) => {
                if (specifier === "../data/mockData") {
                  return {
                    mockCandidateDetails: {},
                    mockDashboard: {},
                    mockManualReviewItems: [],
                    mockPlan: {},
                    mockRunStatus: {},
                  };
                }
                if (specifier === "./dashboardHydration") {
                  return {
                    dashboardExpectedCandidateCount: (dashboard) => Math.max(
                      Number(dashboard?.boardRuntimeState?.expectedCandidateCount || 0) || 0,
                      Number(dashboard?.totalCandidates || 0) || 0,
                      Number(dashboard?.assetPopulationCount || 0) || 0,
                      Number(dashboard?.resultViewLifecycle?.expectedCandidateCount || 0) || 0,
                      Number(dashboard?.resultViewLifecycle?.servedCandidateCount || 0) || 0,
                      Number(dashboard?.candidates?.length || 0) || 0,
                    ),
                    dashboardHasRenderableCandidates: (dashboard) => Number(dashboard?.candidates?.length || 0) > 0,
                    dashboardRowHydrationTargetCount: (dashboard) =>
                      Number(dashboard?.boardRuntimeState?.rowHydrationTargetCount || 0) || 0,
                  };
                }
                if (specifier === "./resultViewLifecycle") {
                  return {
                    lifecycleEffectiveDeltaMaterializedCount: (lifecycle) =>
                      Number(lifecycle?.deltaProfileBoardVisibleCount ?? lifecycle?.deltaProfileMaterializedCount ?? 0) || 0,
                  };
                }
                return require(specifier);
              };
              vm.runInNewContext(
                compiled,
                {
                  module,
                  exports: module.exports,
                  require: localRequire,
                  console,
                  crypto: { randomUUID: () => "test-id" },
                },
                { filename: "api.js" },
              );
              return module.exports;
            }

            const { mergeDashboardCandidatePage, mergeDashboardRuntimeProgress } = loadApiModule();
            const boardRuntimeState = {
              schemaVersion: 1,
              jobId: "job-openai",
              resultMode: "asset_population",
              phase: "current_snapshot_serving",
              publicationStatus: "complete",
              expectedCandidateCount: 597,
              servedCandidateCount: 597,
              publishedCandidateCount: 597,
              displayReadyCandidateCount: 597,
              rowHydrationTargetCount: 597,
              facetSummaryStatus: "complete",
              facetSummaryScope: "global_full_population",
              facetSummaryCandidateCount: 597,
              layeringStatus: "completed",
              syncStatusText: "597/597",
              rowPublicationTier: "current_snapshot_serving",
              rowPublicationSequence: 5,
            };
            const staleFacetSummary = {
              candidateCount: 297,
              layers: [
                { id: "layer_0", label: "Layer 0", count: 297 },
                { id: "layer_1", label: "Layer 1", count: 0 },
              ],
              recall: [
                { id: "all", label: "全量", count: 297 },
                { id: "keyword:agent", label: "Agent", count: 297 },
              ],
            };
            const rawLovableFacetSummary = {
              candidateCount: 145,
              layers: [
                { id: "layer_0", label: "Layer 0", count: 127 },
                { id: "layer_1", label: "Layer 1", count: 3 },
                { id: "layer_2", label: "Layer 2", count: 7 },
                { id: "layer_3", label: "Layer 3", count: 8 },
              ],
            };
            const canonicalFacetSummary = {
              candidateCount: 597,
              layers: [
                { id: "layer_0", label: "Layer 0", count: 597 },
                { id: "layer_1", label: "Layer 1", count: 0 },
              ],
              recall: [
                { id: "all", label: "全量", count: 597 },
                { id: "keyword:agent", label: "Agent", count: 297 },
              ],
            };
            const dashboard = {
              title: "OpenAI Agent",
              snapshotId: "snapshot",
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
              layers: staleFacetSummary.layers,
              groups: [],
              candidates: [],
              boardRuntimeState,
              candidateFacetSummary: staleFacetSummary,
              candidateFacetSummaryScope: "global_full_population",
            };
            const mergedProgress = mergeDashboardRuntimeProgress(dashboard, { boardRuntimeState });
            const stalePage = mergeDashboardCandidatePage(mergedProgress, {
              jobId: "job-openai",
              resultMode: "asset_population",
              offset: 0,
              limit: 24,
              returnedCount: 24,
              totalCandidates: 597,
              filteredCandidateCount: 597,
              hasMore: true,
              nextOffset: 24,
              candidates: [],
              boardRuntimeState,
              candidateFacetSummary: rawLovableFacetSummary,
              candidateFacetSummaryScope: "global_full_population",
            });
            const canonicalPage = mergeDashboardCandidatePage(stalePage, {
              jobId: "job-openai",
              resultMode: "asset_population",
              offset: 0,
              limit: 24,
              returnedCount: 24,
              totalCandidates: 597,
              filteredCandidateCount: 597,
              hasMore: true,
              nextOffset: 24,
              candidates: [],
              boardRuntimeState,
              candidateFacetSummary: canonicalFacetSummary,
              candidateFacetSummaryScope: "global_full_population",
            });
            console.log(JSON.stringify({
              afterProgressHasFacet: Boolean(mergedProgress.candidateFacetSummary),
              afterStalePageHasFacet: Boolean(stalePage.candidateFacetSummary),
              canonicalLayer0: canonicalPage.candidateFacetSummary.layers.find((option) => option.id === "layer_0").count,
              canonicalAgent: canonicalPage.candidateFacetSummary.recall.find((option) => option.id === "keyword:agent").count,
            }));
            """
        )
        completed = subprocess.run(
            ["node", "-e", script],
            cwd=REPO_ROOT,
            text=True,
            capture_output=True,
            check=True,
        )
        payload = json.loads(completed.stdout)
        self.assertFalse(payload["afterProgressHasFacet"])
        self.assertFalse(payload["afterStalePageHasFacet"])
        self.assertEqual(payload["canonicalLayer0"], 597)
        self.assertEqual(payload["canonicalAgent"], 297)

    def test_results_board_function_filter_all_summary_is_full_population(self) -> None:
        source = (REPO_ROOT / "frontend-demo/src/components/ResultsBoardPanel.tsx").read_text(encoding="utf-8")
        self.assertIn(
            'summary={facetSummaryLabel(functionOptions, selectedFunctionBuckets, "全量")}',
            source,
        )
        self.assertNotIn(
            'summary={summarizeSelectedFacet(selectedFunctionBuckets, functionOptions, "Researcher、Engineer")}',
            source,
        )

    def test_search_page_merges_board_patch_runtime_before_dashboard_reload(self) -> None:
        source = (REPO_ROOT / "frontend-demo/src/pages/SearchPage.tsx").read_text(encoding="utf-8")
        self.assertIn("mergeDashboardBoardPatchRuntime(baseDashboard, patchLog)", source)
        self.assertIn("mergeDashboardBoardPatchRuntime(nextDashboard, patchLog)", source)
        self.assertLess(
            source.index("mergeDashboardBoardPatchRuntime(baseDashboard, patchLog)"),
            source.index("const nextDashboard = await getDashboard(jobId, { forceRefresh: true })"),
        )

    def test_partial_hydration_filter_empty_state_is_not_final_empty_result(self) -> None:
        source = (REPO_ROOT / "frontend-demo/src/components/ResultsBoardPanel.tsx").read_text(encoding="utf-8")
        self.assertIn(
            'waitingForBackendPage',
            source,
        )
        self.assertIn(
            '"正在装载当前筛选条件下的候选人。"',
            source,
        )
        self.assertIn(
            '"筛选与分页由后端 canonical 候选人集合计算。"',
            source,
        )
        self.assertIn(
            '"已加载候选片段中暂时没有筛选命中。"',
            source,
        )
        self.assertIn(
            '"系统正在继续装载剩余候选人；这不是最终空结果。"',
            source,
        )
        self.assertIn('visibleCandidateHitLabel', source)
        self.assertIn('backendFilteredPagingSupported || localRowWindowComplete ? "当前筛选命中" : "已加载窗口筛选命中"', source)
        self.assertIn('loadedRowWindowText', source)
        self.assertNotIn("本地已缓存", source)
        self.assertNotIn("当前筛选条件下没有候选人。系统正在继续装载剩余候选人", source)

    def test_target_candidate_export_uses_projection_export_not_legacy_archive(self) -> None:
        panel_source = (REPO_ROOT / "frontend-demo/src/components/TargetCandidatesPanel.tsx").read_text(encoding="utf-8")
        backend_source = (REPO_ROOT / "frontend-demo/src/lib/sourcingBackend.ts").read_text(encoding="utf-8")

        self.assertIn("exportProjectionCandidatesArchive", panel_source)
        self.assertIn("canonicalExportScope.complete", panel_source)
        self.assertNotIn("exportTargetCandidatesArchive", panel_source)
        self.assertIn("getRunProjectionId(jobId)", backend_source)
        self.assertIn("exportProjectionCandidatesArchive", backend_source)
        self.assertNotIn("exportTargetCandidatesArchive", backend_source)

    def test_target_candidate_store_uses_crm_normal_path_not_legacy_target_write(self) -> None:
        store_source = (REPO_ROOT / "frontend-demo/src/lib/targetCandidatesStore.ts").read_text(encoding="utf-8")
        api_source = (REPO_ROOT / "frontend-demo/src/lib/api.ts").read_text(encoding="utf-8")
        panel_source = (REPO_ROOT / "frontend-demo/src/components/TargetCandidatesPanel.tsx").read_text(encoding="utf-8")

        self.assertIn("addProjectionCandidateToCrm", store_source)
        self.assertIn("projectionId", store_source)
        self.assertIn("candidateIdentityKey", store_source)
        self.assertIn('"/api/crm/records"', api_source)
        self.assertIn("sourceCollectionId", store_source)
        self.assertIn("source_collection_id: options?.sourceCollectionId", api_source)
        self.assertIn("targetCandidateScope.sourceCollectionId && scopedRecordIds.length === 0", panel_source)
        self.assertIn("setPublicWebState(EMPTY_PUBLIC_WEB_STATE)", panel_source)
        self.assertIn('method: "PATCH"', api_source)
        self.assertIn("`/api/crm/records/${encodeURIComponent(payload.id)}`", api_source)
        target_store_write_section = store_source[store_source.index("export async function upsertTargetCandidate") :]
        self.assertNotIn('"/api/target-candidates"', target_store_write_section)

    def test_target_candidate_public_web_uses_crm_normal_path_not_legacy_target_path(self) -> None:
        api_source = (REPO_ROOT / "frontend-demo/src/lib/api.ts").read_text(encoding="utf-8")
        panel_source = (REPO_ROOT / "frontend-demo/src/components/TargetCandidatesPanel.tsx").read_text(encoding="utf-8")

        self.assertIn('"/api/crm/records/public-web-search"', api_source)
        self.assertIn('"/api/crm/records/public-web-search/poll"', api_source)
        self.assertIn('"/api/crm/records/public-web-export"', api_source)
        self.assertIn("`/api/crm/records/${encodeURIComponent(recordId)}/profile`", api_source)
        self.assertIn("`/api/crm/records/${encodeURIComponent(recordId)}/public-web-search`", api_source)
        self.assertIn("`/api/crm/records/${encodeURIComponent(payload.recordId)}/public-web-promotions`", api_source)
        self.assertNotIn("/api/target-candidates/public-web", api_source)
        self.assertNotIn("/api/target-candidates/${", api_source)
        self.assertNotIn("/api/target-candidates/public-web", panel_source)

    def test_target_candidate_public_web_detail_uses_reviewable_signal_sections(self) -> None:
        api_source = (REPO_ROOT / "frontend-demo/src/lib/api.ts").read_text(encoding="utf-8")
        panel_source = (REPO_ROOT / "frontend-demo/src/components/TargetCandidatesPanel.tsx").read_text(encoding="utf-8")
        page_source = (REPO_ROOT / "frontend-demo/src/pages/TargetCandidatesPage.tsx").read_text(encoding="utf-8")
        e2e_source = (REPO_ROOT / "frontend-demo/scripts/run_target_public_web_promotion_export_e2e.mjs").read_text(encoding="utf-8")
        styles_source = (REPO_ROOT / "frontend-demo/src/styles.css").read_text(encoding="utf-8")
        frontend_contract = (REPO_ROOT / "docs/FRONTEND_API_CONTRACT.md").read_text(encoding="utf-8")

        self.assertIn("window.confirm(", panel_source)
        self.assertIn("确认对 ${recordIds.length} 位候选人搜索公开信息", panel_source)
        self.assertIn("公开信息搜索任务", panel_source)
        self.assertIn("确认重新搜索公开信息", panel_source)
        self.assertIn("getTargetCandidatePublicWebDetail", panel_source)
        self.assertNotIn("getTargetCandidateProfile", panel_source)
        self.assertIn('useState<PublicWebExportMode>("promoted_only")', panel_source)
        self.assertIn('data-testid="target-candidates-public-web-export-mode"', panel_source)
        self.assertIn("Web Search 默认只导出已人工确认", panel_source)
        self.assertIn("确认导出 ${publicWebExportRecordIds.length} 位候选人的 Web Search 信息", panel_source)
        export_handler_source = panel_source[
            panel_source.index("const handleExportPublicWebArchive") : panel_source.index(
                "const handlePromotePublicWebSignal"
            )
        ]
        self.assertIn("mode: publicWebExportMode", export_handler_source)
        self.assertNotIn('mode: "promoted_and_publishable"', export_handler_source)
        self.assertIn("interface PublicWebDetailCacheEntry", panel_source)
        self.assertIn("latestRunId: string;", panel_source)
        self.assertIn("publicWebDetailCacheMatches", panel_source)
        self.assertIn("return Boolean(expectedRunId) && entry.latestRunId === expectedRunId", panel_source)
        self.assertIn("publicWebSignalsForRun", panel_source)
        self.assertIn("return [];", panel_source)
        self.assertIn("function publicWebReviewableSignals", panel_source)
        self.assertIn('publicWebSignalReviewStatus(signal) === "review"', panel_source)
        self.assertIn("const activePublicWebReviewRunId =", panel_source)
        self.assertIn("activePublicWebExpectedRunId && !activePublicWebDetailIsStale", panel_source)
        self.assertIn("if (!activePublicWebReviewRunId)", panel_source)
        self.assertIn("createdAt: string;", (REPO_ROOT / "frontend-demo/src/types.ts").read_text(encoding="utf-8"))
        self.assertIn("workspaceId: string;", (REPO_ROOT / "frontend-demo/src/types.ts").read_text(encoding="utf-8"))
        self.assertIn('workspaceId: pickFirstString(record, ["workspace_id", "workspaceId"])', api_source)
        self.assertNotIn('workspaceId: pickFirstString(record, ["workspace_id", "workspaceId"]) || "default"', api_source)
        self.assertIn("function requireTargetCandidatePublicWebWorkspaceId", api_source)
        self.assertIn("Public Web Search 操作缺少后端 workspace_id，已阻止本次请求。", api_source)
        public_web_api_source = api_source[
            api_source.index("export async function exportTargetCandidatePublicWebArchive")
            : api_source.index("export async function promoteTargetCandidatePublicWebSignal")
        ]
        self.assertIn("const workspaceId = requireTargetCandidatePublicWebWorkspaceId(payload?.workspaceId);", public_web_api_source)
        self.assertIn("const workspaceId = requireTargetCandidatePublicWebWorkspaceId(options?.workspaceId);", public_web_api_source)
        self.assertIn("const workspaceId = requireTargetCandidatePublicWebWorkspaceId(payload.workspaceId);", public_web_api_source)
        self.assertIn("workspace_id: workspaceId", public_web_api_source)
        self.assertNotIn('workspace_id: payload.workspaceId || "default"', public_web_api_source)
        self.assertNotIn('workspace_id: options?.workspaceId || "default"', public_web_api_source)
        self.assertNotIn('workspace_id: payload?.workspaceId || "default"', public_web_api_source)
        self.assertIn("function uniqueWorkspaceIdForRecords", panel_source)
        self.assertIn("workspaceIds.some((workspaceId) => !workspaceId)", panel_source)
        workspace_fail_closed_source = panel_source[
            panel_source.index("if (scopedRecordIds.length > 0 && !scopedWorkspaceId)")
            : panel_source.index("const state = await getTargetCandidatePublicWebSearches")
        ]
        self.assertIn("setPublicWebState(EMPTY_PUBLIC_WEB_STATE);", workspace_fail_closed_source)
        self.assertIn("setPublicWebDetailsByRecordId({});", workspace_fail_closed_source)
        self.assertIn("setPublicWebDetailErrors({});", workspace_fail_closed_source)
        self.assertIn("setPublicWebDetailLoadingIds(new Set());", workspace_fail_closed_source)
        self.assertIn('setActivePublicWebDetailRecordId("");', workspace_fail_closed_source)
        self.assertIn("Public Web Search 缺少 workspace 或暂不支持跨 workspace 批量提交", panel_source)
        self.assertIn("workspaceId: selectedRecordsWorkspaceId", panel_source)
        self.assertIn('const workspaceId = (run.workspaceId || "").trim();', panel_source)
        self.assertIn("Public Web Search run 缺少 workspace，已阻止取消请求。", panel_source)
        self.assertIn("Public Web Search run 缺少 workspace，已阻止重试请求。", panel_source)
        self.assertNotIn('workspaceId: run.workspaceId || "default"', panel_source)
        self.assertIn("workspaceId: exportScopeWorkspaceId", panel_source)
        self.assertIn('createdAt: pickFirstString(record, ["created_at", "createdAt"])', api_source)
        created_at_compare_source = panel_source[
            panel_source.index("function publicWebCreatedAtTimestamp")
            : panel_source.index("function publicWebRunAllowedActions")
        ]
        self.assertIn("Date.parse(value || \"\")", created_at_compare_source)
        self.assertIn("if (leftTimestamp === null || rightTimestamp === null)", created_at_compare_source)
        self.assertIn("return 0;", created_at_compare_source)
        self.assertIn("function comparePublicWebRunsByCurrentOrder", created_at_compare_source)
        self.assertIn("function comparePublicWebBatchesByCurrentOrder", created_at_compare_source)
        run_sort_source = panel_source[
            panel_source.index("function comparePublicWebRunsByCurrentOrder")
            : panel_source.index("function comparePublicWebBatchesByCurrentOrder")
        ]
        self.assertIn("comparePublicWebCreatedAt(left.createdAt, right.createdAt)", run_sort_source)
        self.assertNotIn("run.startedAt", run_sort_source)
        self.assertNotIn("run.completedAt", run_sort_source)
        self.assertNotIn("run.updatedAt", run_sort_source)
        batch_sort_source = panel_source[
            panel_source.index("function comparePublicWebBatchesByCurrentOrder")
            : panel_source.index("function publicWebRunAllowedActions")
        ]
        self.assertIn("comparePublicWebCreatedAt(left.createdAt, right.createdAt)", batch_sort_source)
        self.assertNotIn("batch.updatedAt", batch_sort_source)
        self.assertIn("const batchesById = new Map(next.batches.map((batch) => [batch.batchId, batch]));", panel_source)
        self.assertIn("const runsById = new Map(next.runs.map((run) => [run.runId, run]));", panel_source)
        self.assertIn("batches: Array.from(batchesById.values()).sort(comparePublicWebBatchesByCurrentOrder)", panel_source)
        self.assertIn("runs: Array.from(runsById.values()).sort(comparePublicWebRunsByCurrentOrder)", panel_source)
        self.assertIn(".sort(comparePublicWebRunsByCurrentOrder)", panel_source)
        self.assertIn(".sort(comparePublicWebBatchesByCurrentOrder)", panel_source)
        self.assertNotIn("publicWebBatchSortValue", panel_source)
        self.assertNotIn("publicWebRunSortValue", panel_source)
        self.assertNotIn("const candidates = [run.updatedAt", panel_source)
        self.assertNotIn("Date.parse(right.updatedAt || right.createdAt", panel_source)
        self.assertIn("DataForSEO 仍在处理", panel_source)
        self.assertIn("AI 判定失败，未生成新的可审核公开信息候选", panel_source)
        self.assertNotIn("activePublicWebDetailRunId", panel_source)
        self.assertIn("不属于本次搜索", panel_source)
        self.assertIn("公开信息详情正在更新", panel_source)
        public_web_card_metric_source = panel_source[
            panel_source.index('const entryLinkCount = numberFromSummary(publicWebSummary, "entry_link_count")') : panel_source.index(
                "const editDraft = targetCandidateDraftsByRecordId"
            )
        ]
        self.assertIn('"email_signal_materialized_count"', public_web_card_metric_source)
        self.assertNotIn('"email_candidate_count"', public_web_card_metric_source)
        self.assertNotIn('"promotion_recommended_email_count"', public_web_card_metric_source)
        self.assertIn("可审核邮箱 {reviewableEmailSignalCount}", panel_source)
        self.assertNotIn("<span>Email {", panel_source)
        self.assertNotIn("推荐 {recommendedEmailCount}", panel_source)
        store_detail_source = panel_source[
            panel_source.index("const storePublicWebDetail") : panel_source.index("const loadPublicWebDetail")
        ]
        stale_detail_branch = store_detail_source[
            store_detail_source.index("if (expectedRunId && detailRunId && detailRunId !== expectedRunId)") : store_detail_source.index(
                'setPublicWebDetailErrors((previous) => ({'
            )
        ]
        self.assertIn("latestRunId: detailRunId", stale_detail_branch)
        self.assertNotIn("delete next[recordId]", stale_detail_branch)
        self.assertNotIn("detailRunId || expectedRunId", store_detail_source)
        self.assertIn("latestRunId: detailRunId,", store_detail_source)
        load_detail_source = panel_source[
            panel_source.index("const loadPublicWebDetail") : panel_source.index("const refreshPublicWebDetail")
        ]
        self.assertNotIn("if (!expectedRunId) {\n        setPublicWebDetailsByRecordId", load_detail_source)
        self.assertIn("已确认公开信息", panel_source)
        self.assertIn("重新搜索只刷新下方本次待审核候选", panel_source)
        self.assertNotIn("最新 run", panel_source)
        self.assertNotIn("latest run", panel_source)
        self.assertNotIn("Public Web run", panel_source)
        self.assertNotIn("候选人级 run", panel_source)
        self.assertNotIn("长期资产", panel_source)
        self.assertIn('data-testid="public-web-confirmed-assets-section"', panel_source)
        self.assertIn('className="public-web-review-section__title-row"', panel_source)
        self.assertIn('className="public-web-section-help"', panel_source)
        self.assertIn('label={`${title}说明`}', panel_source)
        self.assertIn('label="已确认公开信息说明"', panel_source)
        self.assertIn('label="公开信息详情说明"', panel_source)
        self.assertIn('label="只读证据来源说明"', panel_source)
        self.assertNotIn("public-web-review-contract-note", panel_source)
        self.assertIn(".page > .local-asset-tab-shell", styles_source)
        self.assertIn(".local-asset-tab-shell + .target-candidates-panel > .panel:first-child", styles_source)
        self.assertIn(
            ".target-candidate-detail-drawer .target-candidate-public-web-signal-row:not(.public-web-review-signal)",
            styles_source,
        )
        self.assertIn("target-candidate-headline-scroll", panel_source)
        self.assertIn(".target-candidate-headline-scroll", styles_source)
        self.assertIn("target-candidate-public-web-progress", panel_source)
        self.assertIn("target-candidate-public-web-progress-copy", panel_source)
        self.assertIn("target-candidate-public-web-review-help", panel_source)
        self.assertIn('aria-label="已确认公开主页"', panel_source)
        self.assertIn(".target-candidate-public-web-progress", styles_source)
        self.assertIn(".target-candidate-public-web-progress-copy", styles_source)
        self.assertIn(".target-candidate-public-web-links", styles_source)
        self.assertIn("max-height: calc(1.35em * 3 + 2px);", styles_source)
        self.assertIn("max-height: 38px;", styles_source)
        self.assertIn(".public-web-review-signal__topline > div:first-child", styles_source)
        self.assertIn(".public-web-section-help .help-tooltip", styles_source)
        detail_cache_prune_source = panel_source[
            panel_source.index("setPublicWebDetailsByRecordId((previous) => {") : panel_source.index(
                "const followUpFacetOptions"
            )
        ]
        self.assertIn("const activeRecordIds = new Set(records.map((record) => record.id));", detail_cache_prune_source)
        self.assertNotIn("expectedRunId && entry.latestRunId !== expectedRunId", detail_cache_prune_source)
        self.assertNotIn("latestPublicWebRunByRecordId", detail_cache_prune_source)
        start_search_source = panel_source[
            panel_source.index("const handleStartPublicWebSearch") : panel_source.index(
                "const handleOpenPublicWebDetail"
            )
        ]
        self.assertNotIn("setPublicWebDetailsByRecordId", start_search_source)
        self.assertNotIn("delete next[recordId]", start_search_source)
        self.assertIn("latestPublicWebRunIdForRecord(result.runs, activePublicWebDetailRecordId)", start_search_source)
        self.assertIn("refreshPublicWebDetail(activePublicWebDetailRecordId, expectedRunId)", start_search_source)
        detail_latest_run_source = panel_source[
            panel_source.index("function publicWebDetailLatestRunId") : panel_source.index(
                "function publicWebDetailCacheMatches"
            )
        ]
        self.assertIn("const latestRun = detail?.latestRun;", detail_latest_run_source)
        self.assertIn('return "";', detail_latest_run_source)
        self.assertNotIn("detail?.signals.find", detail_latest_run_source)
        self.assertIn("phaseCommandDisplayLine: string;", (REPO_ROOT / "frontend-demo/src/types.ts").read_text(encoding="utf-8"))
        self.assertIn("runControlState: Record<string, unknown>;", (REPO_ROOT / "frontend-demo/src/types.ts").read_text(encoding="utf-8"))
        self.assertIn("runDisplayContract: Record<string, unknown>;", (REPO_ROOT / "frontend-demo/src/types.ts").read_text(encoding="utf-8"))
        self.assertIn('phaseCommandDisplayLine: pickFirstString(record, ["phase_command_display_line", "phaseCommandDisplayLine"])', api_source)
        self.assertIn("runControlState: asRecord(record.run_control_state || record.runControlState)", api_source)
        self.assertIn("runDisplayContract: asRecord(record.run_display_contract || record.runDisplayContract)", api_source)
        self.assertIn("return run?.phaseCommandDisplayLine || \"\";", panel_source)
        self.assertIn("const allowedActions = run?.runControlState?.allowed_actions;", panel_source)
        self.assertIn('publicWebRunAllows(publicWebRun, "cancel")', panel_source)
        self.assertIn('publicWebRunAllows(publicWebRun, "retry")', panel_source)
        self.assertNotIn("function publicWebCommandTypeLabel", panel_source)
        self.assertNotIn("function publicWebCommandStatusLabel", panel_source)
        self.assertNotIn("function publicWebRunCanCancel", panel_source)
        self.assertNotIn("function publicWebRunCanRetry", panel_source)
        self.assertNotIn("phaseCommands.phase_order", panel_source)
        self.assertNotIn("current.command_type", panel_source)
        self.assertNotIn("current.status", panel_source)
        self.assertNotIn("阶段命令 ${completedCount}/${commandCount}", panel_source)
        self.assertNotIn("source projection provenance", panel_source)
        self.assertNotIn("target-candidates legacy export", panel_source)
        self.assertNotIn("CRM/target migration", panel_source)
        self.assertNotIn("服务门禁", panel_source)
        self.assertNotIn("provider/fetch", panel_source)
        self.assertNotIn("终态物化违规", panel_source)
        self.assertNotIn("物化缺口", panel_source)
        self.assertNotIn("物化", panel_source)
        self.assertNotIn("Public Web signal", panel_source)
        self.assertNotIn("target-candidates-public-web-guardrails", panel_source)
        self.assertIn("target-candidates-public-web-issues", panel_source)
        self.assertIn("公开信息处理有异常", panel_source)
        public_web_card_status_source = panel_source[
            panel_source.index("function publicWebStatusLabel") : panel_source.index("function publicWebPhaseCommandLine")
        ]
        for leaked_fragment in ("物化", "Materialize", "CRM Public Web", "model_safe", "crm.public_web"):
            self.assertNotIn(leaked_fragment, public_web_card_status_source)
        self.assertIn("分析完成，正在保存公开信息结果", public_web_card_status_source)
        self.assertIn("AI 判定完成，待生成可审核公开信息", public_web_card_status_source)
        self.assertIn("已生成可审核公开信息", public_web_card_status_source)
        self.assertNotIn("批量导出 Projection", panel_source)
        self.assertNotIn("当前选择中有候选人暂不支持这类资料打包", panel_source)
        self.assertIn("target-candidates-export-controls", panel_source)
        self.assertIn("target-candidates-button-with-help", panel_source)
        self.assertIn('<HelpBadge label="人选资料导出说明">', panel_source)
        self.assertIn("缺少可打包的人选资料来源时，该按钮会禁用", panel_source)
        self.assertNotIn("Projection 人选信息", api_source)
        self.assertIn('title: "联系邮箱候选"', panel_source)
        self.assertIn('title: "公开主页候选"', panel_source)
        self.assertIn("signals: activePublicWebReviewableEmailSignals", panel_source)
        self.assertIn("signals: activePublicWebReviewableProfileSignals", panel_source)
        self.assertNotIn("signals: activePublicWebEmailSignals", panel_source)
        self.assertNotIn("signals: activePublicWebProfileSignals", panel_source)
        self.assertIn("只读证据来源", panel_source)
        self.assertIn('data-testid={testId}', panel_source)
        self.assertIn('testId: "public-web-email-review-section"', panel_source)
        self.assertIn('testId: "public-web-profile-review-section"', panel_source)
        self.assertIn('data-testid="public-web-evidence-audit-section"', panel_source)
        self.assertIn('data-testid="public-web-signal-review-select"', panel_source)
        self.assertIn('<option value="review" disabled={reviewStatus !== "review"}>', panel_source)
        self.assertIn("当前版本不支持把已人工判断的公开信息候选回退到待复核", panel_source)
        self.assertIn("公开信息候选已人工确认", panel_source)
        self.assertIn('<option value="confirmed">已确认并导出</option>', panel_source)
        self.assertIn('<option value="excluded">已排除</option>', panel_source)
        self.assertNotIn("Composed Profile", panel_source)
        self.assertNotIn("候选人资料完整度", panel_source)
        self.assertIn("<h3>跟进工作台</h3>", panel_source)
        self.assertNotIn("<h3>目标候选人</h3>", panel_source)
        self.assertIn("管理候选人状态、备注、公开信息审核和导出。", panel_source)
        self.assertIn('<LocalAssetTabs active="targets" collectionId={collectionId} />', page_source)
        self.assertNotIn("持续跟进", page_source)
        self.assertNotIn("沉淀候选人跟进状态、质量评价、备注和公开信息审核。", page_source)
        self.assertIn('getByTestId("public-web-email-review-section")', e2e_source)
        self.assertIn('selectOption("confirmed")', e2e_source)
        self.assertIn('getByTestId("target-candidates-public-web-export-mode").selectOption("promoted_and_publishable")', e2e_source)
        self.assertIn('page.once("dialog"', e2e_source)
        self.assertIn(".public-web-review-signal.is-secondary", styles_source)
        self.assertIn(".public-web-review-select", styles_source)
        self.assertIn("三类内容分开", frontend_contract)
        self.assertIn("Public Web detail cache 必须区分长期人工确认资产和 latest-run 待审核信号", frontend_contract)
        self.assertIn("latest run id 变化不得删除整个 detail cache", frontend_contract)
        self.assertIn("public_web_signal_not_latest_run", frontend_contract)
        self.assertIn("v1 不支持把已人工确认/排除的 signal 回退为待复核", frontend_contract)
        self.assertIn("不要展示没有 owner/公式的“资料完整度”这类合成分数", frontend_contract)
        self.assertIn("首次多选 Public Web Search 与单人 retry 都应提示", frontend_contract)
        self.assertIn("PG normal path 也必须对 batch/run 非空 idempotency key 建唯一约束", frontend_contract)
        self.assertIn("summary.phase_metrics.email_signal_materialized_count", frontend_contract)
        self.assertIn("summary.email_candidate_count", frontend_contract)
        self.assertIn("只作为 raw/adjudication 诊断字段", frontend_contract)
        self.assertIn("尚未人工确认/排除", frontend_contract)
        self.assertIn("不再重复出现在“本次待审核候选”列表", frontend_contract)

    def test_board_patch_polling_uses_sequence_cursor_and_maps_card_quality_counts(self) -> None:
        search_page_source = (REPO_ROOT / "frontend-demo/src/pages/SearchPage.tsx").read_text(encoding="utf-8")
        api_source = (REPO_ROOT / "frontend-demo/src/lib/api.ts").read_text(encoding="utf-8")
        self.assertIn("boardPatchSequenceByJobRef", search_page_source)
        self.assertIn("const afterSequence = boardPatchSequenceByJobRef.current[jobId] || 0;", search_page_source)
        self.assertIn("getDashboardBoardPatches(jobId, { afterPublishedAt, afterSequence, limit: 50 })", search_page_source)
        self.assertIn("boardPatchSequenceByJobRef.current[jobId] = patchLog.latestSequenceIndex;", search_page_source)
        self.assertIn("needsProfileCompletionCandidateCount", api_source)
        self.assertIn("lowProfileRichnessCandidateCount", api_source)
        self.assertIn("qualityFieldsAvailable", api_source)
        self.assertIn("needs_profile_completion_candidate_count", api_source)
        self.assertIn("low_profile_richness_candidate_count", api_source)
        self.assertIn("quality_fields_available", api_source)

    def test_get_dashboard_candidate_page_reuses_inflight_force_refresh_request(self) -> None:
        if shutil.which("node") is None:
            self.skipTest("node is required for frontend TypeScript helper checks")
        script = textwrap.dedent(
            """
            const fs = require("fs");
            const path = require("path");
            const vm = require("vm");
            const ts = require("./frontend-demo/node_modules/typescript");

            function loadApiModule(fetchStub) {
              const source = fs
                .readFileSync(path.join(process.cwd(), "frontend-demo/src/lib/api.ts"), "utf8")
                .replaceAll("import.meta.env", "({ VITE_API_BASE_URL: 'http://127.0.0.1:8765' })");
              const compiled = ts.transpileModule(source, {
                compilerOptions: { module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2020 },
              }).outputText;
              const module = { exports: {} };
              const localRequire = (specifier) => {
                if (specifier === "../data/mockData") {
                  return {
                    mockCandidateDetails: {},
                    mockDashboard: {},
                    mockManualReviewItems: [],
                    mockPlan: {},
                    mockRunStatus: {},
                  };
                }
                if (specifier === "./dashboardHydration") {
                  return {
                    dashboardExpectedCandidateCount: (dashboard) => Math.max(
                      Number(dashboard?.boardRuntimeState?.expectedCandidateCount || 0) || 0,
                      Number(dashboard?.totalCandidates || 0) || 0,
                      Number(dashboard?.assetPopulationCount || 0) || 0,
                      Number(dashboard?.resultViewLifecycle?.expectedCandidateCount || 0) || 0,
                      Number(dashboard?.resultViewLifecycle?.servedCandidateCount || 0) || 0,
                      Number(dashboard?.candidates?.length || 0) || 0,
                    ),
                    dashboardHasRenderableCandidates: (dashboard) =>
                      Number(dashboard?.boardRuntimeState?.displayReadyCandidateCount || 0) > 0 ||
                      Number(dashboard?.candidates?.length || 0) > 0,
                    dashboardRowHydrationTargetCount: (dashboard) =>
                      Number(dashboard?.boardRuntimeState?.rowHydrationTargetCount || 0) || 0,
                  };
                }
                if (specifier === "./resultViewLifecycle") {
                  return {
                    lifecycleEffectiveDeltaMaterializedCount: (lifecycle) =>
                      Number(lifecycle?.deltaProfileBoardVisibleCount ?? lifecycle?.deltaProfileMaterializedCount ?? 0) || 0,
                  };
                }
                return require(specifier);
              };
              vm.runInNewContext(
                compiled,
                {
                  module,
                  exports: module.exports,
                  require: localRequire,
                  console,
                  fetch: fetchStub,
                  Headers: globalThis.Headers,
                  AbortController: globalThis.AbortController,
                  setTimeout,
                  clearTimeout,
                  URL,
                  URLSearchParams,
                  crypto: { randomUUID: () => "test-id" },
                },
                { filename: "api.js" },
              );
              return module.exports;
            }

            function makeResponse(payload) {
              return {
                ok: true,
                status: 200,
                headers: {
                  get: () => "application/json",
                },
                text: async () => JSON.stringify(payload),
              };
            }

            const candidatePagePayload = {
              job_id: "job-1",
              offset: 24,
              limit: 24,
              returned_count: 1,
              total_candidates: 25,
              filtered_candidate_count: 0,
              has_more: false,
              next_offset: null,
              candidates: [
                {
                  id: "cand-1",
                  name: "Candidate One",
                  team: "Research",
                },
              ],
              board_runtime_state: {
                job_id: "job-1",
                result_mode: "ranked_results",
                phase: "partial_board_visible",
                publication_status: "partial",
                expected_candidate_count: 25,
                served_candidate_count: 25,
                published_candidate_count: 25,
                display_ready_candidate_count: 12,
                preview_candidate_count: 13,
                profile_detail_candidate_count: 12,
                explicit_profile_capture_candidate_count: 0,
                needs_profile_completion_candidate_count: 1,
                low_profile_richness_candidate_count: 0,
                card_materialization_quality_fields_available: true,
                row_hydration_target_count: 25,
                baseline_candidate_count: 0,
                delta_profile_required_count: 0,
                delta_profile_fetched_count: 0,
                delta_profile_materialized_count: 0,
                delta_profile_board_visible_count: 0,
                delta_profile_denominator_promoted: true,
                row_publication_sequence: 7,
                row_publication_watermark: "2026-05-06T00:00:00Z",
                row_publication_updated_at: "2026-05-06T00:00:00Z",
                facet_summary_status: "complete",
                facet_summary_scope: "global_full_population",
                facet_summary_candidate_count: 25,
                layering_status: "completed",
                filter_contract: {
                  source: "dashboard",
                  facet_count_scope: "global",
                  row_filter_scope: "global",
                  backend_filtered_paging_supported: true,
                },
                sync_status_text: "候选人同步 12/25",
                profile_fetch_status_text: "新增 LinkedIn Profile 已取回 12/25",
                card_materialization_status_text: "卡片详情已合入看板 12/25",
                note_text: "",
              },
            };

            async function runCase() {
              let fetchCalls = 0;
              let releaseFetch;
              const fetchGate = new Promise((resolve) => {
                releaseFetch = resolve;
              });
              const fetchStub = async () => {
                fetchCalls += 1;
                await fetchGate;
                return makeResponse(candidatePagePayload);
              };
              const api = loadApiModule(fetchStub);
              const first = api.getDashboardCandidatePage("job-1", {
                offset: 24,
                limit: 24,
                forceRefresh: true,
                lightweight: true,
                filter: { searchKeyword: "candidate" },
              });
              const second = api.getDashboardCandidatePage("job-1", {
                offset: 24,
                limit: 24,
                forceRefresh: true,
                lightweight: true,
                filter: { searchKeyword: "candidate" },
              });
              releaseFetch();
              const [firstResult, secondResult] = await Promise.all([first, second]);
              const projectionResult = await api.getProjectionCandidatePage("projection-1", {
                offset: 24,
                limit: 24,
                forceRefresh: true,
                filter: { searchKeyword: "candidate" },
              });
              return {
                fetchCalls,
                firstPromoted: Boolean(firstResult.boardRuntimeState?.deltaProfileDenominatorPromoted),
                secondPromoted: Boolean(secondResult.boardRuntimeState?.deltaProfileDenominatorPromoted),
                firstJobId: String(firstResult.boardRuntimeState?.jobId || ""),
                secondJobId: String(secondResult.boardRuntimeState?.jobId || ""),
                firstFilteredCandidateCount: Number(firstResult.filteredCandidateCount),
                secondFilteredCandidateCount: Number(secondResult.filteredCandidateCount),
                projectionFilteredCandidateCount: Number(projectionResult.filteredCandidateCount),
              };
            }

            runCase()
              .then((payload) => {
                console.log(JSON.stringify(payload));
              })
              .catch((error) => {
                console.error(error instanceof Error ? error.stack || error.message : String(error));
                process.exit(1);
              });
            """
        )
        completed = subprocess.run(
            ["node", "-e", script],
            cwd=REPO_ROOT,
            text=True,
            capture_output=True,
            check=True,
        )
        payload = json.loads(completed.stdout)
        self.assertEqual(payload["fetchCalls"], 2, payload)
        self.assertTrue(payload["firstPromoted"], payload)
        self.assertTrue(payload["secondPromoted"], payload)
        self.assertEqual(payload["firstJobId"], "job-1", payload)
        self.assertEqual(payload["secondJobId"], "job-1", payload)
        self.assertEqual(payload["firstFilteredCandidateCount"], 0, payload)
        self.assertEqual(payload["secondFilteredCandidateCount"], 0, payload)
        self.assertEqual(payload["projectionFilteredCandidateCount"], 0, payload)
