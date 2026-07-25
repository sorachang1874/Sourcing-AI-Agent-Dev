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
                  TextEncoder,
                  crypto: { randomUUID: () => "test-plan-id" },
                },
                { filename: path.basename(relativePath) },
              );
              return module.exports;
            }

            const cohortSelection = loadModule("frontend-demo/src/lib/cohortSelection.ts");
            const runtimeContract = loadModule("contracts/frontend_api_runtime_contract.ts");
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
              "./cohortSelection": cohortSelection,
              "../../../contracts/frontend_api_runtime_contract": runtimeContract,
            });
            const historyRecovery = loadModule("frontend-demo/src/lib/historyRecovery.ts", {
              "./historySummary": { summarizeSearchQuery: (value) => value },
              "./workflow": {
                buildReusedCompletedTimelineSteps: () => [],
                isReusedCompletedHistory: () => false,
              },
              "./cohortSelection": cohortSelection,
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
            const captureError = (callback) => {
              try {
                callback();
                return "";
              } catch (error) {
                return String(error?.message || error || "");
              }
            };
            const optionsPayload = {
              schema_version: "cohort_selection.v1",
              registry_version: "cohort_selection.registry.v1",
              registry_digest: "9f2c1ab4d5e6478091a2b3c4d5e6f708192a3b4c5d6e7f8091a2b3c4d5e6f708",
              role_buckets: [
                { id: "engineering", label: "Engineer", order: 20 },
                { id: "research", label: "Researcher", order: 10 },
                { id: "product_management", label: "Product Manager", order: 30 },
              ],
              employment_statuses: [
                { id: "former", label: "Former employees", order: 20 },
                { id: "current", label: "Current employees", order: 10 },
              ],
              role_match_options: [
                { id: "all", label: "Match all selected roles", order: 20 },
                { id: "any", label: "Match any selected role", order: 10 },
              ],
              defaults: { role_match: "any" },
            };
            const parsedOptions = cohortSelection.parseCohortSelectionOptionsPayload(optionsPayload);
            const defaultCohort = cohortSelection.createDefaultCohortSelection(parsedOptions);
            const explicitCohort = {
              schema_version: "cohort_selection.v1",
              role_bucket_ids: ["research", "engineering"],
              employment_statuses: ["current", "former"],
              role_match: "any",
              source: "user_explicit",
            };
            const exactCohortPlan = api.__testMapPlanPayloadToDemoPlan({
              request: { raw_user_request: "find people", cohort_selection: explicitCohort },
              request_preview: { cohort_selection: explicitCohort },
              plan: {
                acquisition_strategy: {
                  provider_execution_manifest: {
                    registry_version: "cohort_selection.registry.v1",
                    registry_digest: "9f2c1ab4d5e6478091a2b3c4d5e6f708192a3b4c5d6e7f8091a2b3c4d5e6f708",
                  },
                },
              },
            }, "find people");
            const exactCohortReview = api.planReviewDecisionToApiPayload({
              confirmedCompanyScope: [],
              extraSourceFamilies: [],
              cohortSelection: explicitCohort,
            }, []);
            const clonedReviewDecision = historyRecovery.cloneReviewDecision({
              reviewDecisionDefaults: {
                confirmedCompanyScope: [],
                extraSourceFamilies: [],
                cohortSelection: explicitCohort,
              },
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
              initialPlanSubmitPayload: api.__testBuildPlanSubmitPayload("find researchers"),
              revisionPlanSubmitPayload: api.__testBuildPlanSubmitPayload(
                "find researchers with revision",
                "history-server-owned-1",
              ),
              initialResolvedHistoryId: api.__testResolvePlanSubmitHistoryId(
                "",
                "history-server-owned-1",
              ),
              missingInitialHistoryIdError: captureError(() =>
                api.__testResolvePlanSubmitHistoryId("", ""),
              ),
              mismatchedRevisionHistoryIdError: captureError(() =>
                api.__testResolvePlanSubmitHistoryId(
                  "history-server-owned-1",
                  "history-other-owner",
                ),
              ),
              malformedResponseHistoryIdErrors: [{}, ["history-array"], true, 123].map((value) =>
                captureError(() => api.__testResolvePlanSubmitHistoryId("", value)),
              ),
              parsedRoleIds: parsedOptions.roleBuckets.map((option) => option.id),
              parsedStatusIds: parsedOptions.employmentStatuses.map((option) => option.id),
              parsedRoleMatchIds: parsedOptions.roleMatchOptions.map((option) => option.id),
              defaultCohort,
              orderedRoleToggle: cohortSelection.toggleOrderedOption(
                ["engineering"],
                "research",
                true,
                parsedOptions.roleBuckets,
              ),
              legacyCohortOmitted: !("cohort_selection" in api.__testBuildPlanSubmitPayload("find people")),
              explicitCohortSubmit: api.__testBuildPlanSubmitPayload(
                "find people",
                "history-server-owned-1",
                explicitCohort,
              ).cohort_selection,
              exactCohortPlan: exactCohortPlan.cohortSelection,
              exactCohortPlanDefault: exactCohortPlan.reviewDecisionDefaults.cohortSelection,
              exactCohortReview: exactCohortReview.cohort_selection,
              clonedReviewCohort: clonedReviewDecision.cohortSelection,
              conflictingCohortMirrorError: captureError(() =>
                api.__testMapPlanPayloadToDemoPlan({
                  request: { cohort_selection: explicitCohort },
                  request_preview: {
                    cohort_selection: {
                      ...explicitCohort,
                      employment_statuses: ["current"],
                    },
                  },
                  plan: {
                    acquisition_strategy: {
                      provider_execution_manifest: {
                        registry_version: "cohort_selection.registry.v1",
                        registry_digest: "9f2c1ab4d5e6478091a2b3c4d5e6f708192a3b4c5d6e7f8091a2b3c4d5e6f708",
                      },
                    },
                  },
                }, "find people"),
              ),
              duplicateOptionError: captureError(() =>
                cohortSelection.parseCohortSelectionOptionsPayload({
                  ...optionsPayload,
                  role_buckets: [
                    { id: "research", label: "Researcher", order: 10 },
                    { id: "research", label: "Duplicate", order: 20 },
                  ],
                }),
              ),
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

    def test_plan_submit_identity_lifecycle_uses_server_id_then_explicit_revision_id(self) -> None:
        result = self._run_contract_cases()

        self.assertNotIn("history_id", result["initialPlanSubmitPayload"])
        self.assertEqual(
            result["revisionPlanSubmitPayload"]["history_id"],
            "history-server-owned-1",
        )
        self.assertEqual(result["initialResolvedHistoryId"], "history-server-owned-1")
        self.assertIn("missing the server-owned history id", result["missingInitialHistoryIdError"])
        self.assertIn("changed the existing history id", result["mismatchedRevisionHistoryIdError"])
        self.assertEqual(len(result["malformedResponseHistoryIdErrors"]), 4)
        self.assertTrue(
            all(
                "non-string server-owned history id" in message
                for message in result["malformedResponseHistoryIdErrors"]
            )
        )

        source = (REPO_ROOT / "frontend-demo/src/pages/SearchPage.tsx").read_text(encoding="utf-8")
        initial_start = source.index("const submitSearch")
        initial_end = source.index("const startExcelWorkflow", initial_start)
        initial_section = source[initial_start:initial_end]
        self.assertIn("planNaturalLanguageSearch(", initial_section)
        self.assertIn('nextQuery,\n          "",\n          cohortSelection || undefined,', initial_section)
        self.assertNotIn("planNaturalLanguageSearch(nextQuery, historyItem.id)", initial_section)
        self.assertGreaterEqual(initial_section.count("persistFlow("), 3)
        self.assertIn("persistFlow(pendingHydrationFlow, null, historyItem.id)", initial_section)
        self.assertIn("persistFlow(readyFlow, null, historyItem.id)", initial_section)

        revision_start = source.index("const applyRevision")
        revision_section = source[revision_start:]
        revision_call_start = revision_section.index("await sourcingBackendClient.planNaturalLanguageSearch(")
        revision_call_end = revision_section.index(");", revision_call_start)
        revision_call = revision_section[revision_call_start:revision_call_end]
        self.assertIn("currentFlow.id", revision_call)

    def test_cohort_selection_uses_public_options_and_round_trips_exactly(self) -> None:
        result = self._run_contract_cases()
        explicit = {
            "schema_version": "cohort_selection.v1",
            "role_bucket_ids": ["research", "engineering"],
            "employment_statuses": ["current", "former"],
            "role_match": "any",
            "source": "user_explicit",
        }

        self.assertEqual(
            result["parsedRoleIds"],
            ["research", "engineering", "product_management"],
        )
        self.assertEqual(result["parsedStatusIds"], ["current", "former"])
        self.assertEqual(result["parsedRoleMatchIds"], ["any", "all"])
        self.assertEqual(
            result["defaultCohort"],
            {
                "schema_version": "cohort_selection.v1",
                "role_bucket_ids": [],
                "employment_statuses": ["current", "former"],
                "role_match": "any",
                "source": "user_explicit",
            },
        )
        self.assertEqual(result["orderedRoleToggle"], ["research", "engineering"])
        self.assertTrue(result["legacyCohortOmitted"])
        self.assertEqual(result["explicitCohortSubmit"], explicit)
        self.assertEqual(result["exactCohortPlan"], explicit)
        self.assertEqual(result["exactCohortPlanDefault"], explicit)
        self.assertEqual(result["exactCohortReview"], explicit)
        self.assertEqual(result["clonedReviewCohort"], explicit)
        self.assertIn("conflicting cohort_selection mirrors", result["conflictingCohortMirrorError"])
        self.assertIn("duplicate role_buckets id", result["duplicateOptionError"])

        picker_source = (REPO_ROOT / "frontend-demo/src/components/CohortSelectionPicker.tsx").read_text(
            encoding="utf-8"
        )
        self.assertIn("options.roleBuckets.map", picker_source)
        self.assertIn("options.employmentStatuses.map", picker_source)
        self.assertIn("options.roleMatchOptions.map", picker_source)
        self.assertNotIn('value="research"', picker_source)
        self.assertNotIn('value="engineering"', picker_source)
        self.assertNotIn('value="product_management"', picker_source)

        page_source = (REPO_ROOT / "frontend-demo/src/pages/SearchPage.tsx").read_text(encoding="utf-8")
        self.assertIn("getCohortSelectionOptions()", page_source)
        self.assertIn("cohortSelection || undefined", page_source)
        self.assertIn(
            "currentFlow.reviewDecision.cohortSelection || currentFlow.plan.cohortSelection",
            page_source,
        )

    def test_local_plan_snapshot_rekey_removes_provisional_id(self) -> None:
        if shutil.which("node") is None:
            self.skipTest("node is required for frontend TypeScript helper checks")
        script = textwrap.dedent(
            """
            const fs = require("fs");
            const path = require("path");
            const vm = require("vm");
            const ts = require("./frontend-demo/node_modules/typescript");
            const source = fs.readFileSync(
              path.join(process.cwd(), "frontend-demo/src/lib/searchHistory.ts"),
              "utf8",
            );
            const compiled = ts.transpileModule(source, {
              compilerOptions: { module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2020 },
            }).outputText;
            let stored = JSON.stringify([
              { id: "history-local-provisional", createdAt: "2026-07-14T02:00:00Z" },
              { id: "history-unrelated", createdAt: "2026-07-14T01:00:00Z" },
            ]);
            let updateEvents = 0;
            const window = {
              localStorage: {
                getItem: () => stored,
                setItem: (_key, value) => { stored = value; },
              },
              dispatchEvent: () => { updateEvents += 1; },
            };
            const module = { exports: {} };
            const localRequire = (specifier) => {
              if (specifier === "./api") {
                return { deleteFrontendHistory: async () => ({}), listFrontendHistory: async () => [] };
              }
              if (specifier === "./historyRecovery") {
                return { historyItemFromRecoveryEnvelope: (value) => value };
              }
              return require(specifier);
            };
            vm.runInNewContext(
              compiled,
              { module, exports: module.exports, require: localRequire, window, Event: class Event {} },
              { filename: "searchHistory.js" },
            );
            module.exports.replaceSearchHistoryItem(
              "history-local-provisional",
              { id: "history-server-owned", createdAt: "2026-07-14T03:00:00Z" },
            );
            console.log(JSON.stringify({ items: JSON.parse(stored), updateEvents }));
            """
        )
        completed = subprocess.run(
            ["node", "-e", script],
            cwd=REPO_ROOT,
            check=True,
            text=True,
            capture_output=True,
        )
        result = json.loads(completed.stdout)
        self.assertEqual(
            [item["id"] for item in result["items"]],
            ["history-server-owned", "history-unrelated"],
        )
        self.assertEqual(result["updateEvents"], 1)

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
