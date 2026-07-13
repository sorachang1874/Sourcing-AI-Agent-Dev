from __future__ import annotations

import json
import shutil
import subprocess
import textwrap
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]


class FrontendPlanContractTest(unittest.TestCase):
    def _run_contract_cases(self) -> dict:
        if shutil.which("node") is None:
            self.skipTest("node is required for frontend TypeScript helper checks")
        script = textwrap.dedent(
            """
            const fs = require("fs");
            const path = require("path");
            const vm = require("vm");
            const ts = require("./frontend-demo/node_modules/typescript");

            function loadModule(relativePath, overrides = {}) {
              const source = fs
                .readFileSync(path.join(process.cwd(), relativePath), "utf8")
                .replaceAll("import.meta.env", "({})");
              const compiled = ts.transpileModule(source, {
                compilerOptions: { module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2020 },
              }).outputText;
              const module = { exports: {} };
              const localRequire = (specifier) => {
                if (Object.prototype.hasOwnProperty.call(overrides, specifier)) {
                  return overrides[specifier];
                }
                if (specifier === "./workflowStatus") {
                  return {
                    normalizeWorkflowStatus: () => "failed",
                    resolveWorkflowStatus: () => ({ status: "failed", terminal: true }),
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
                  crypto: { randomUUID: () => "test-plan-id" },
                },
                { filename: path.basename(relativePath) },
              );
              return module.exports;
            }

            const api = loadModule("frontend-demo/src/lib/api.ts", {
              "../data/mockData": {
                mockCandidateDetails: {},
                mockDashboard: {},
                mockManualReviewItems: [],
                mockPlan: {},
                mockRunStatus: {},
              },
              "./dashboardHydration": {
                dashboardExpectedCandidateCount: () => 0,
                dashboardHasRenderableCandidates: () => false,
              },
              "./resultViewLifecycle": {
                lifecycleEffectiveDeltaMaterializedCount: () => 0,
              },
            });
            const historyRecovery = loadModule("frontend-demo/src/lib/historyRecovery.ts", {
              "./historySummary": { summarizeSearchQuery: (value) => value },
              "./workflow": {
                buildReusedCompletedTimelineSteps: () => [],
                isReusedCompletedHistory: () => false,
              },
            });

            const staleLovableShape = {
              effective_execution_semantics: {
                execution_strategy_label: "全量本地资产复用",
                effective_acquisition_mode: "full_local_asset_reuse",
              },
              asset_reuse_plan: {
                planner_mode: "reuse_snapshot_only",
                baseline_reuse_available: true,
                requires_delta_acquisition: false,
                baseline_full_company_coverage_proven: true,
              },
              organization_execution_profile: {
                default_acquisition_mode: "scoped_search_roster",
                current_lane_default: "reuse_baseline",
              },
              dispatch_preview: { strategy: "reuse_completed" },
              plan: { acquisition_strategy: { strategy_type: "scoped_search_roster" } },
            };
            const fallbackFullReuseShape = {
              effective_execution_semantics: {},
              asset_reuse_plan: {
                planner_mode: "reuse_snapshot_only",
                baseline_reuse_available: true,
                requires_delta_acquisition: false,
                baseline_full_company_coverage_proven: true,
              },
              organization_execution_profile: {
                default_acquisition_mode: "scoped_search_roster",
                current_lane_default: "reuse_baseline",
              },
              dispatch_preview: { strategy: "reuse_completed" },
            };
            const dispatchOnlyReuseShape = {
              effective_execution_semantics: {},
              asset_reuse_plan: {},
              organization_execution_profile: {
                default_acquisition_mode: "scoped_search_roster",
              },
              dispatch_preview: { strategy: "reuse_completed" },
            };
            const localCachedHistory = {
              id: "history-lovable-old-local-cache",
              phase: "plan",
              jobId: "",
              reviewId: "123",
              errorMessage: "",
              plan: { acquisitionStrategy: "Baseline 复用 + 缺口增量" },
              historyMetadata: {
                effective_execution_semantics: {
                  execution_strategy_label: "Baseline 复用 + 缺口增量",
                },
              },
            };
            const backendRecoveredHistory = {
              ...localCachedHistory,
              historyMetadata: {
                ...localCachedHistory.historyMetadata,
                frontend_history_recovery_source: "backend",
              },
            };
            const recovered = historyRecovery.historyItemFromRecoveryEnvelope({
              historyId: "history-lovable-backend",
              queryText: "帮我找Lovable的全部成员",
              reviewId: "456",
              jobId: "",
              phase: "plan",
              plan: { acquisitionStrategy: "全量本地资产复用" },
              metadata: {},
              raw: {},
            });

            console.log(JSON.stringify({
              canonicalLabel: api.__testResolvePlanAcquisitionStrategyLabel(staleLovableShape),
              fallbackLabel: api.__testResolvePlanAcquisitionStrategyLabel(fallbackFullReuseShape),
              dispatchOnlyLabel: api.__testResolvePlanAcquisitionStrategyLabel(dispatchOnlyReuseShape),
              genericKeywordSuppressedPlan: api.__testMapPlanPayloadToDemoPlan({
                request_preview: {
                  target_company: "Lovable",
                  keywords: [],
                  organization_keywords: [],
                },
                plan: {
                  acquisition_strategy: {
                    search_seed_queries: ["Lovable Employee", "Lovable Linkedin Employee"],
                  },
                },
              }, "帮我找Lovable的全部成员").keywords,
              manifestKeywordPlan: api.__testMapPlanPayloadToDemoPlan({
                request_preview: {
                  target_company: "OpenAI",
                  keywords: [],
                  organization_keywords: [],
                },
                plan: {
                  acquisition_strategy: {
                    search_seed_queries: ["OpenAI Employee"],
                    provider_execution_manifest: {
                      lanes: [
                        {
                          lane_id: "current_profile_search",
                          employment_status: "current",
                          provider: "harvest_profile_search",
                          operation: "profile_search",
                          query_texts: ["Agent"],
                          company_filters: { current_companies: ["OpenAI"] },
                          provider_facing_query: true,
                          display_label: "Harvest profile search",
                        },
                      ],
                    },
                  },
                },
              }, "帮我找OpenAI做Agent方向的人"),
              metadataManifestPlan: api.__testMapPlanPayloadToDemoPlan({
                request_preview: {
                  target_company: "Lovable",
                  keywords: [],
                  organization_keywords: [],
                },
                metadata: {
                  provider_execution_manifest: {
                    lanes: [
                      {
                        lane_id: "current_company_employees",
                        employment_status: "current",
                        provider: "harvest_company_employees",
                        operation: "company_employees",
                        query_texts: [],
                        company_filters: { current_companies: ["Lovable"] },
                        provider_facing_query: false,
                        display_label: "Harvest company employees",
                      },
                      {
                        lane_id: "former_past_company_search",
                        employment_status: "former",
                        provider: "harvest_profile_search",
                        operation: "profile_search",
                        query_texts: [],
                        company_filters: { past_companies: ["Lovable"] },
                        provider_facing_query: false,
                        display_label: "Harvest profile search",
                      },
                    ],
                  },
                },
                plan: {
                  acquisition_strategy: {
                    search_seed_queries: ["Lovable Employee", "Lovable Linkedin Employee"],
                  },
                },
              }, "帮我找Lovable的全部成员"),
              baselineDeltaScope: api.__testMapPlanPayloadToDemoPlan({
                effective_execution_semantics: {
                  effective_acquisition_mode: "baseline_reuse_with_delta",
                },
                plan: { acquisition_strategy: { strategy_type: "scoped_search_roster" } },
              }, "帮我找OpenAI做Agent方向的人").projectScope,
              scopedLiveScope: api.__testMapPlanPayloadToDemoPlan({
                effective_execution_semantics: {
                  effective_acquisition_mode: "scoped_live_search",
                },
                plan: { acquisition_strategy: { strategy_type: "scoped_search_roster" } },
              }, "帮我找OpenAI做Agent方向的人").projectScope,
              fullLiveScope: api.__testMapPlanPayloadToDemoPlan({
                effective_execution_semantics: {
                  effective_acquisition_mode: "full_live_roster",
                },
                plan: { acquisition_strategy: { strategy_type: "full_company_roster" } },
              }, "帮我找Lovable的全部成员").projectScope,
              shouldRecoverLocal: historyRecovery.shouldRecoverHistoryFromBackend(localCachedHistory),
              shouldRecoverBackend: historyRecovery.shouldRecoverHistoryFromBackend(backendRecoveredHistory),
              recoveredMarker: recovered.historyMetadata.frontend_history_recovery_source,
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
        return json.loads(completed.stdout)

    def test_plan_strategy_label_uses_canonical_backend_contract(self) -> None:
        result = self._run_contract_cases()

        self.assertEqual(result["canonicalLabel"], "全量本地资产复用")
        self.assertEqual(result["fallbackLabel"], "全量本地资产复用")
        self.assertEqual(result["dispatchOnlyLabel"], "定向搜索 roster")
        self.assertEqual(result["genericKeywordSuppressedPlan"], [])
        self.assertEqual(result["manifestKeywordPlan"]["keywords"], ["Agent"])
        self.assertEqual(result["manifestKeywordPlan"]["providerExecutionLanes"][0]["queryTexts"], ["Agent"])
        self.assertEqual(result["metadataManifestPlan"]["keywords"], [])
        self.assertEqual(
            result["metadataManifestPlan"]["providerExecutionLanes"][0]["provider"],
            "harvest_company_employees",
        )
        self.assertEqual(result["baselineDeltaScope"], "Baseline 全量 + 定向增量范围")
        self.assertEqual(result["scopedLiveScope"], "目标公司定向搜索范围")
        self.assertEqual(result["fullLiveScope"], "目标公司全量范围")

    def test_history_cache_is_not_strategy_source_of_truth(self) -> None:
        result = self._run_contract_cases()

        self.assertTrue(result["shouldRecoverLocal"])
        self.assertFalse(result["shouldRecoverBackend"])
        self.assertEqual(result["recoveredMarker"], "backend")

    def test_provider_manifest_is_advanced_only_developer_context(self) -> None:
        source = (REPO_ROOT / "frontend-demo/src/components/PlanCard.tsx").read_text(encoding="utf-8")
        public_grid_section = source.split('<section className="plan-review-section">', 1)[0]
        advanced_section = source.split('<section className="plan-review-section">', 1)[1]

        self.assertNotIn("<dt>实际 Provider 参数</dt>", public_grid_section)
        self.assertIn('title="实际 Provider 参数"', advanced_section)
        self.assertIn("开发/排障参考", advanced_section)
        self.assertIn("plan.providerExecutionLanes?.length", source)

    def test_default_full_company_project_scope_is_hidden(self) -> None:
        source = (REPO_ROOT / "frontend-demo/src/components/PlanCard.tsx").read_text(encoding="utf-8")

        self.assertIn("showProjectScope", source)
        self.assertIn('plan.projectScope.trim() !== "目标公司全量范围"', source)
        self.assertIn("{showProjectScope ? (", source)


if __name__ == "__main__":
    unittest.main()
