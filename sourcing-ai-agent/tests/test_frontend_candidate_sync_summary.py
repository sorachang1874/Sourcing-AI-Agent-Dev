from __future__ import annotations

import json
import shutil
import subprocess
import textwrap
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]


class FrontendCandidateSyncSummaryTest(unittest.TestCase):
    def _run_summary_cases(self) -> list[dict]:
        if shutil.which("node") is None:
            self.skipTest("node is required for frontend TypeScript helper checks")
        script = textwrap.dedent(
            """
            const fs = require("fs");
            const path = require("path");
            const vm = require("vm");
            const ts = require("./frontend-demo/node_modules/typescript");
            const source = fs.readFileSync(
              path.join(process.cwd(), "frontend-demo/src/lib/candidateSyncSummary.ts"),
              "utf8",
            );
            const lifecycleSource = fs.readFileSync(
              path.join(process.cwd(), "frontend-demo/src/lib/resultViewLifecycle.ts"),
              "utf8",
            );
            const lifecycleCompiled = ts.transpileModule(lifecycleSource, {
              compilerOptions: { module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2020 },
            }).outputText;
            const lifecycleModule = { exports: {} };
            vm.runInNewContext(
              lifecycleCompiled,
              { module: lifecycleModule, exports: lifecycleModule.exports, require, console },
              { filename: "resultViewLifecycle.js" },
            );
            const compiled = ts.transpileModule(source, {
              compilerOptions: { module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2020 },
            }).outputText;
            const module = { exports: {} };
            const localRequire = (specifier) => {
              if (specifier === "./resultViewLifecycle") {
                return lifecycleModule.exports;
              }
              return require(specifier);
            };
            vm.runInNewContext(compiled, { module, exports: module.exports, require: localRequire, console }, {
              filename: "candidateSyncSummary.js",
            });
            const { buildCandidateSyncSummary } = module.exports;
            const baseLifecycle = {
              state: "current_snapshot_serving",
              baselineSnapshotId: "",
              currentSnapshotId: "20260428T183413",
              servedSnapshotId: "20260428T183413",
              baselineCandidateCount: 0,
              servedCandidateCount: 890,
              expectedCandidateCount: 890,
              deltaProfileRequiredCount: 4,
              deltaProfileFetchedCount: 4,
              deltaProfileMaterializedCount: 0,
              deltaProfilePendingCount: 0,
              deltaProfileQueuedCount: 0,
              deltaProfileRetryableCount: 0,
              deltaProfileProgressApplicable: false,
              deltaProfileProgressReason: "not_applicable_full_local_asset_reuse",
              backgroundSnapshotMaterializationStatus: "",
              outreachLayeringStatus: "completed",
            };
            const baseLinkedinProgress = {
              currentSearchReturnedCount: 2,
              formerSearchReturnedCount: 2,
              allSearchReturnedCount: 0,
              dedupedCandidateCount: 4,
              dedupedProfileUrlCount: 4,
              profileFetchRequiredCount: 4,
              profileFetchedCount: 4,
              profileQueuedCount: 0,
              profileFailedRetryableCount: 0,
              profileUnrecoverableCount: 0,
              profilePendingCount: 0,
              statusCounts: {},
            };
            const deltaLifecycle = {
              ...baseLifecycle,
              state: "current_snapshot_materializing",
              baselineSnapshotId: "20260428T000000",
              baselineCandidateCount: 825,
              servedCandidateCount: 850,
              expectedCandidateCount: 890,
              deltaProfileRequiredCount: 65,
              deltaProfileFetchedCount: 40,
              deltaProfileMaterializedCount: 25,
              deltaProfileRetryableCount: 1,
              deltaProfileProgressApplicable: true,
              deltaProfileProgressReason: "linkedin_stage_1_progress",
            };
            const deltaLinkedinProgress = {
              ...baseLinkedinProgress,
              profileFetchRequiredCount: 65,
              profileFetchedCount: 40,
              profileFailedRetryableCount: 1,
            };
            const results = [
              buildCandidateSyncSummary({
                loadedCandidateCount: 250,
                expectedCandidateCount: 890,
                resultViewLifecycle: baseLifecycle,
                linkedinStage1Progress: baseLinkedinProgress,
                recallOptions: [
                  { id: "all", label: "全量", count: 890 },
                  { id: "keyword:agent", label: "Agent", count: 46 },
                ],
                effectiveExecutionSemantics: {
                  effectiveAcquisitionMode: "full_local_asset_reuse",
                  defaultResultsMode: "asset_population",
                  executionStrategyLabel: "全量本地资产复用",
                  fullLocalAssetReuse: true,
                  requiresDeltaAcquisition: false,
                  assetPopulationSupported: true,
                },
              }),
              buildCandidateSyncSummary({
                loadedCandidateCount: 825,
                expectedCandidateCount: 890,
                resultViewLifecycle: deltaLifecycle,
                linkedinStage1Progress: deltaLinkedinProgress,
                recallOptions: [
                  { id: "all", label: "全量", count: 890 },
                  { id: "keyword:agent", label: "Agent", count: 70 },
                ],
              }),
              buildCandidateSyncSummary({
                loadedCandidateCount: 72,
                expectedCandidateCount: 145,
                resultViewLifecycle: {
                  ...baseLifecycle,
                  servedCandidateCount: 36,
                  expectedCandidateCount: 145,
                  deltaProfileRequiredCount: 145,
                  deltaProfileFetchedCount: 72,
                  deltaProfileMaterializedCount: 36,
                  deltaProfileProgressApplicable: true,
                  deltaProfileProgressReason: "linkedin_stage_1_progress",
                },
                linkedinStage1Progress: {
                  ...baseLinkedinProgress,
                  profileFetchRequiredCount: 145,
                  profileFetchedCount: 72,
                },
                effectiveExecutionSemantics: {
                  effectiveAcquisitionMode: "full_company_roster",
                  defaultResultsMode: "asset_population",
                  executionStrategyLabel: "Live roster",
                  fullLocalAssetReuse: false,
                  requiresDeltaAcquisition: false,
                  assetPopulationSupported: true,
                },
                recallOptions: [
                  { id: "all", label: "全量", count: 145 },
                ],
              }),
            buildCandidateSyncSummary({
              loadedCandidateCount: 597,
              expectedCandidateCount: 597,
                resultViewLifecycle: {
                  ...baseLifecycle,
                  state: "current_snapshot_materializing",
                  baselineSnapshotId: "20260414T120300",
                  baselineCandidateCount: 300,
                  servedCandidateCount: 597,
                  expectedCandidateCount: 597,
                  deltaProfileRequiredCount: 200,
                  deltaProfileFetchedCount: 100,
                  deltaProfileMaterializedCount: 0,
                  deltaProfileProgressApplicable: true,
                  deltaProfileProgressReason: "linkedin_stage_1_progress",
                },
                linkedinStage1Progress: {
                  ...baseLinkedinProgress,
                  profileFetchRequiredCount: 200,
                  profileFetchedCount: 100,
                },
                recallOptions: [
                  { id: "all", label: "全量", count: 597 },
                  { id: "keyword:agent", label: "Agent", count: 256 },
                ],
              }),
              buildCandidateSyncSummary({
                loadedCandidateCount: 300,
                expectedCandidateCount: 300,
                resultViewLifecycle: {
                  ...baseLifecycle,
                  state: "delta_applying",
                  baselineSnapshotId: "20260414T120300",
                  baselineCandidateCount: 300,
                  servedCandidateCount: 300,
                  expectedCandidateCount: 388,
                  deltaProfileRequiredCount: 88,
                  deltaProfileFetchedCount: 0,
                  deltaProfileMaterializedCount: 0,
                  deltaProfileProgressApplicable: true,
                  deltaProfileProgressReason: "linkedin_stage_1_progress",
                },
                linkedinStage1Progress: {
                  ...baseLinkedinProgress,
                  profileFetchRequiredCount: 88,
                  profileFetchedCount: 0,
                },
                recallOptions: [
                  { id: "all", label: "全量", count: 300 },
                ],
              }),
              buildCandidateSyncSummary({
                loadedCandidateCount: 597,
                expectedCandidateCount: 597,
                resultViewLifecycle: {
                  ...baseLifecycle,
                  state: "current_snapshot_materializing",
                  baselineSnapshotId: "20260414T120300",
                  baselineCandidateCount: 300,
                  servedCandidateCount: 597,
                  expectedCandidateCount: 597,
                  deltaProfileRequiredCount: 297,
                  deltaProfileFetchedCount: 150,
                  deltaProfileMaterializedCount: 125,
                  deltaProfileProgressApplicable: true,
                  deltaProfileProgressReason: "linkedin_stage_1_progress",
                },
                linkedinStage1Progress: {
                  ...baseLinkedinProgress,
                  profileFetchRequiredCount: 297,
                  profileFetchedCount: 150,
                },
                recallOptions: [
                  { id: "all", label: "全量", count: 597 },
                  { id: "keyword:agent", label: "Agent", count: 297 },
                ],
            }),
            buildCandidateSyncSummary({
              loadedCandidateCount: 96,
              expectedCandidateCount: 597,
              resultViewLifecycle: {
                ...baseLifecycle,
                state: "current_snapshot_serving",
                baselineSnapshotId: "20260414T120300",
                baselineCandidateCount: 300,
                servedCandidateCount: 597,
                expectedCandidateCount: 597,
                deltaProfileRequiredCount: 297,
                deltaProfileFetchedCount: 297,
                deltaProfileMaterializedCount: 297,
                deltaProfileProgressApplicable: true,
                deltaProfileProgressReason: "linkedin_stage_1_progress",
              },
              linkedinStage1Progress: {
                ...baseLinkedinProgress,
                profileFetchRequiredCount: 297,
                profileFetchedCount: 297,
              },
              recallOptions: [
                { id: "all", label: "全量", count: 96 },
                { id: "keyword:agent", label: "Agent", count: 48 },
              ],
            }),
            buildCandidateSyncSummary({
              loadedCandidateCount: 96,
              expectedCandidateCount: 1110,
              resultViewLifecycle: {
                ...baseLifecycle,
                state: "current_snapshot_serving",
                baselineSnapshotId: "20260429T174612",
                currentSnapshotId: "20260430T090520",
                servedSnapshotId: "20260430T090520",
                baselineCandidateCount: 1061,
                servedCandidateCount: 1110,
                expectedCandidateCount: 1110,
                deltaProfileRequiredCount: 83,
                deltaProfileFetchedCount: 83,
                deltaProfileMaterializedCount: 83,
                deltaProfileBoardVisibleCount: 0,
                deltaProfileProgressApplicable: true,
                deltaProfileProgressReason: "linkedin_stage_1_progress",
              },
              linkedinStage1Progress: {
                ...baseLinkedinProgress,
                profileFetchRequiredCount: 83,
                profileFetchedCount: 83,
              },
              recallOptions: [
                { id: "all", label: "全量", count: 1110 },
                { id: "keyword:health", label: "Health", count: 83 },
              ],
            }),
            buildCandidateSyncSummary({
              loadedCandidateCount: 120,
              expectedCandidateCount: 140,
              boardRuntimeState: {
                publicationStatus: "partial",
                expectedCandidateCount: 140,
                servedCandidateCount: 120,
                publishedCandidateCount: 120,
                displayReadyCandidateCount: 48,
                previewCandidateCount: 72,
                profileDetailCandidateCount: 48,
                explicitProfileCaptureCandidateCount: 48,
                needsProfileCompletionCandidateCount: 72,
                lowProfileRichnessCandidateCount: 0,
                cardMaterializationQualityFieldsAvailable: true,
                rowHydrationTargetCount: 120,
                baselineCandidateCount: 0,
                deltaProfileRequiredCount: 140,
                deltaProfileFetchedCount: 48,
                deltaProfileMaterializedCount: 48,
                deltaProfileBoardVisibleCount: 48,
                syncStatusText: "120/140",
                profileFetchStatusText: "本次 LinkedIn Profile 已取回 48/140",
                cardMaterializationStatusText: "卡片详情已合入看板 48/140",
              },
              recallOptions: [
                { id: "all", label: "全量", count: 140 },
              ],
            }),
            buildCandidateSyncSummary({
              loadedCandidateCount: 597,
              expectedCandidateCount: 597,
              resultViewLifecycle: {
                ...baseLifecycle,
                state: "current_snapshot_materializing",
                baselineSnapshotId: "20260414T120300",
                baselineCandidateCount: 300,
                servedCandidateCount: 548,
                expectedCandidateCount: 597,
                deltaProfileRequiredCount: 297,
                deltaProfileFetchedCount: 175,
                deltaProfileMaterializedCount: 248,
                deltaProfileProgressApplicable: true,
                deltaProfileProgressReason: "linkedin_stage_1_progress",
              },
              linkedinStage1Progress: {
                ...baseLinkedinProgress,
                profileFetchRequiredCount: 297,
                profileFetchedCount: 195,
              },
              boardRuntimeState: {
                publicationStatus: "complete",
                expectedCandidateCount: 597,
                servedCandidateCount: 597,
                publishedCandidateCount: 597,
                displayReadyCandidateCount: 597,
                previewCandidateCount: 0,
                profileDetailCandidateCount: 597,
                explicitProfileCaptureCandidateCount: 597,
                needsProfileCompletionCandidateCount: 0,
                lowProfileRichnessCandidateCount: 0,
                cardMaterializationQualityFieldsAvailable: true,
                rowHydrationTargetCount: 597,
                baselineCandidateCount: 300,
                deltaProfileRequiredCount: 297,
                deltaProfileFetchedCount: 297,
                deltaProfileMaterializedCount: 297,
                deltaProfileBoardVisibleCount: 297,
                syncStatusText: "597/597",
                profileFetchStatusText: "新增 LinkedIn Profile 已取回 297/297",
                cardMaterializationStatusText: "卡片详情已合入看板 297/297",
              },
              recallOptions: [
                { id: "all", label: "全量", count: 597 },
                { id: "keyword:agent", label: "Agent", count: 297 },
              ],
            }),
          ];
            console.log(JSON.stringify(results));
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

    def test_reuse_only_summary_separates_intent_matches_from_provider_fetch_count(self) -> None:
        reuse_only, _, _, _, _, _, _, _, _, _ = self._run_summary_cases()

        self.assertEqual(reuse_only["loadedCandidateCount"], 890)
        self.assertEqual(reuse_only["syncedCandidateCount"], 890)
        self.assertEqual(reuse_only["hydratedCandidateCount"], 250)
        self.assertEqual(reuse_only["expectedCandidateCount"], 890)
        self.assertEqual(reuse_only["noteText"], "")
        self.assertNotIn("已物化到看板", reuse_only["noteText"])
        self.assertNotIn("LinkedIn Profile 已取回", reuse_only["noteText"])

    def test_delta_summary_reports_materialized_count_from_served_baseline_delta(self) -> None:
        _, delta, _, _, _, _, _, _, _, _ = self._run_summary_cases()

        self.assertEqual(delta["loadedCandidateCount"], 850)
        self.assertEqual(delta["expectedCandidateCount"], 890)
        self.assertEqual(
            delta["profileFetchStatusText"],
            "新增 LinkedIn Profile 已取回 40/65，可重试 1",
        )
        self.assertEqual(
            delta["cardMaterializationStatusText"],
            "卡片详情已合入看板 25/65",
        )
        self.assertNotIn("当前意图匹配 Agent 70 人", delta["noteText"])

    def test_live_roster_summary_still_reports_provider_profile_progress(self) -> None:
        _, _, live_roster, _, _, _, _, _, _, _ = self._run_summary_cases()

        self.assertEqual(live_roster["loadedCandidateCount"], 36)
        self.assertEqual(live_roster["expectedCandidateCount"], 145)
        self.assertEqual(
            live_roster["profileFetchStatusText"],
            "本次 LinkedIn Profile 已取回 72/145",
        )
        self.assertEqual(
            live_roster["cardMaterializationStatusText"],
            "卡片详情已合入看板 36/145",
        )

    def test_delta_summary_uses_explicit_materialized_count_not_fetched_count(self) -> None:
        _, _, _, premature_served, _, _, _, _, _, _ = self._run_summary_cases()

        self.assertEqual(premature_served["loadedCandidateCount"], 597)
        self.assertEqual(premature_served["expectedCandidateCount"], 597)
        self.assertEqual(
            premature_served["profileFetchStatusText"],
            "新增 LinkedIn Profile 已取回 100/200",
        )
        self.assertEqual(
            premature_served["cardMaterializationStatusText"],
            "卡片详情已合入看板 0/200",
        )

    def test_baseline_serving_delta_pending_uses_lifecycle_expected_denominator(self) -> None:
        _, _, _, _, baseline_pending, _, _, _, _, _ = self._run_summary_cases()

        self.assertEqual(baseline_pending["loadedCandidateCount"], 300)
        self.assertEqual(baseline_pending["expectedCandidateCount"], 388)
        self.assertEqual(
            baseline_pending["profileFetchStatusText"],
            "新增 LinkedIn Profile 已取回 0/88",
        )
        self.assertEqual(
            baseline_pending["cardMaterializationStatusText"],
            "卡片详情已合入看板 0/88",
        )

    def test_delta_summary_does_not_use_local_page_cache_as_sync_progress(self) -> None:
        _, _, _, _, _, partially_materialized, _, _, _, _ = self._run_summary_cases()

        self.assertEqual(partially_materialized["loadedCandidateCount"], 597)
        self.assertEqual(partially_materialized["expectedCandidateCount"], 597)
        self.assertEqual(
            partially_materialized["profileFetchStatusText"],
            "新增 LinkedIn Profile 已取回 150/297",
        )
        self.assertEqual(
            partially_materialized["cardMaterializationStatusText"],
            "卡片详情已合入看板 125/297",
        )

    def test_terminal_sync_summary_does_not_use_frontend_hydration_window_as_business_progress(self) -> None:
        _, _, _, _, _, _, hydrating, _, _, _ = self._run_summary_cases()

        self.assertEqual(hydrating["loadedCandidateCount"], 597)
        self.assertEqual(hydrating["syncedCandidateCount"], 597)
        self.assertEqual(hydrating["hydratedCandidateCount"], 96)
        self.assertEqual(hydrating["expectedCandidateCount"], 597)
        self.assertEqual(
            hydrating["profileFetchStatusText"],
            "新增 LinkedIn Profile 已取回 297/297",
        )
        self.assertEqual(
            hydrating["cardMaterializationStatusText"],
            "卡片详情已合入看板 297/297",
        )
        self.assertEqual(hydrating["intentMatchStatusText"], "")
        self.assertNotIn("当前意图匹配 Agent", hydrating["noteText"])

    def test_terminal_current_snapshot_summary_ignores_stale_board_visible_zero(self) -> None:
        _, _, _, _, _, _, _, historical_health, _, _ = self._run_summary_cases()

        self.assertEqual(historical_health["loadedCandidateCount"], 1110)
        self.assertEqual(historical_health["syncedCandidateCount"], 1110)
        self.assertEqual(historical_health["hydratedCandidateCount"], 96)
        self.assertEqual(
            historical_health["profileFetchStatusText"],
            "新增 LinkedIn Profile 已取回 83/83",
        )
        self.assertEqual(
            historical_health["cardMaterializationStatusText"],
            "卡片详情已合入看板 83/83",
        )

    def test_board_runtime_sync_uses_backend_row_publication_status(self) -> None:
        _, _, _, _, _, _, _, _, board_runtime, _ = self._run_summary_cases()

        self.assertEqual(board_runtime["syncedCandidateCount"], 120)
        self.assertEqual(board_runtime["loadedCandidateCount"], 120)
        self.assertEqual(board_runtime["hydratedCandidateCount"], 120)
        self.assertEqual(board_runtime["expectedCandidateCount"], 140)
        self.assertEqual(
            board_runtime["profileFetchStatusText"],
            "本次 LinkedIn Profile 已取回 48/140",
        )
        self.assertEqual(
            board_runtime["cardMaterializationStatusText"],
            "卡片详情已合入看板 48/140",
        )

    def test_board_runtime_summary_ignores_conflicting_lifecycle_and_stage1_progress(self) -> None:
        _, _, _, _, _, _, _, _, _, board_runtime_final = self._run_summary_cases()

        self.assertEqual(board_runtime_final["syncedCandidateCount"], 597)
        self.assertEqual(board_runtime_final["loadedCandidateCount"], 597)
        self.assertEqual(board_runtime_final["expectedCandidateCount"], 597)
        self.assertEqual(
            board_runtime_final["profileFetchStatusText"],
            "新增 LinkedIn Profile 已取回 297/297",
        )
        self.assertEqual(
            board_runtime_final["cardMaterializationStatusText"],
            "卡片详情已合入看板 297/297",
        )
        self.assertNotIn("195/297", board_runtime_final["noteText"])
        self.assertNotIn("175/297", board_runtime_final["noteText"])
        self.assertNotIn("248/297", board_runtime_final["noteText"])

    def test_board_runtime_sync_status_text_is_canonical_public_progress(self) -> None:
        if shutil.which("node") is None:
            self.skipTest("node is required for frontend TypeScript helper checks")
        script = textwrap.dedent(
            """
            const fs = require("fs");
            const path = require("path");
            const vm = require("vm");
            const ts = require("./frontend-demo/node_modules/typescript");
            const source = fs.readFileSync(
              path.join(process.cwd(), "frontend-demo/src/lib/candidateSyncSummary.ts"),
              "utf8",
            );
            const lifecycleSource = fs.readFileSync(
              path.join(process.cwd(), "frontend-demo/src/lib/resultViewLifecycle.ts"),
              "utf8",
            );
            const lifecycleCompiled = ts.transpileModule(lifecycleSource, {
              compilerOptions: { module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2020 },
            }).outputText;
            const lifecycleModule = { exports: {} };
            vm.runInNewContext(
              lifecycleCompiled,
              { module: lifecycleModule, exports: lifecycleModule.exports, require, console },
              { filename: "resultViewLifecycle.js" },
            );
            const compiled = ts.transpileModule(source, {
              compilerOptions: { module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2020 },
            }).outputText;
            const module = { exports: {} };
            const localRequire = (specifier) => {
              if (specifier === "./resultViewLifecycle") {
                return lifecycleModule.exports;
              }
              return require(specifier);
            };
            vm.runInNewContext(compiled, { module, exports: module.exports, require: localRequire, console }, {
              filename: "candidateSyncSummary.js",
            });
            const { buildCandidateSyncSummary } = module.exports;
            const summary = buildCandidateSyncSummary({
              loadedCandidateCount: 120,
              expectedCandidateCount: 140,
              boardRuntimeState: {
                publicationStatus: "partial",
                expectedCandidateCount: 140,
                servedCandidateCount: 120,
                publishedCandidateCount: 120,
                displayReadyCandidateCount: 0,
                previewCandidateCount: 120,
                profileDetailCandidateCount: 0,
                explicitProfileCaptureCandidateCount: 0,
                needsProfileCompletionCandidateCount: 120,
                lowProfileRichnessCandidateCount: 0,
                cardMaterializationQualityFieldsAvailable: false,
                rowHydrationTargetCount: 120,
                baselineCandidateCount: 0,
                deltaProfileRequiredCount: 140,
                deltaProfileFetchedCount: 0,
                deltaProfileMaterializedCount: 0,
                deltaProfileBoardVisibleCount: 0,
                syncStatusText: "120/140",
              },
              recallOptions: [],
            });
            console.log(JSON.stringify(summary));
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
        self.assertEqual(payload["syncedCandidateCount"], 120)
        self.assertEqual(payload["loadedCandidateCount"], 120)
        self.assertEqual(payload["hydratedCandidateCount"], 120)
        self.assertEqual(payload["expectedCandidateCount"], 140)
        self.assertEqual(payload["noteText"], "")

    def test_board_runtime_summary_uses_backend_sync_note_lines_without_intent_note(self) -> None:
        if shutil.which("node") is None:
            self.skipTest("node is required for frontend TypeScript helper checks")
        script = textwrap.dedent(
            """
            const fs = require("fs");
            const path = require("path");
            const vm = require("vm");
            const ts = require("./frontend-demo/node_modules/typescript");
            const source = fs.readFileSync(
              path.join(process.cwd(), "frontend-demo/src/lib/candidateSyncSummary.ts"),
              "utf8",
            );
            const lifecycleSource = fs.readFileSync(
              path.join(process.cwd(), "frontend-demo/src/lib/resultViewLifecycle.ts"),
              "utf8",
            );
            const lifecycleCompiled = ts.transpileModule(lifecycleSource, {
              compilerOptions: { module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2020 },
            }).outputText;
            const lifecycleModule = { exports: {} };
            vm.runInNewContext(
              lifecycleCompiled,
              { module: lifecycleModule, exports: lifecycleModule.exports, require, console },
              { filename: "resultViewLifecycle.js" },
            );
            const compiled = ts.transpileModule(source, {
              compilerOptions: { module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2020 },
            }).outputText;
            const module = { exports: {} };
            const localRequire = (specifier) => {
              if (specifier === "./resultViewLifecycle") {
                return lifecycleModule.exports;
              }
              return require(specifier);
            };
            vm.runInNewContext(compiled, { module, exports: module.exports, require: localRequire, console }, {
              filename: "candidateSyncSummary.js",
            });
            const { buildCandidateSyncSummary } = module.exports;
            const summary = buildCandidateSyncSummary({
              loadedCandidateCount: 597,
              expectedCandidateCount: 597,
              boardRuntimeState: {
                publicationStatus: "complete",
                expectedCandidateCount: 597,
                servedCandidateCount: 597,
                publishedCandidateCount: 597,
                displayReadyCandidateCount: 597,
                rowHydrationTargetCount: 597,
                baselineCandidateCount: 300,
                deltaProfileRequiredCount: 297,
                deltaProfileFetchedCount: 297,
                deltaProfileMaterializedCount: 297,
                deltaProfileBoardVisibleCount: 297,
                syncStatusText: "597/597",
                profileFetchStatusText: "新增 LinkedIn Profile 已取回 297/297",
                cardMaterializationStatusText: "卡片详情已合入看板 297/297",
                syncNoteLines: [
                  { id: "profile_fetch", text: "新增 LinkedIn Profile 已取回 297/297" },
                  { id: "card_materialization", text: "卡片详情已合入看板 297/297" },
                ],
                noteText: "legacy note should not be used",
              },
              recallOptions: [
                { id: "all", label: "全量", count: 597 },
                { id: "keyword:agent", label: "Agent", count: 297 },
              ],
            });
            console.log(JSON.stringify(summary));
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
        self.assertIn("当前意图匹配 Agent 297 人", payload["intentMatchStatusText"])
        self.assertEqual(
            payload["noteText"],
            "新增 LinkedIn Profile 已取回 297/297；卡片详情已合入看板 297/297",
        )
        self.assertNotIn("当前意图匹配", payload["noteText"])
        self.assertNotIn("legacy note", payload["noteText"])

    def test_board_runtime_summary_keeps_canonical_fraction_wording(self) -> None:
        if shutil.which("node") is None:
            self.skipTest("node is required for frontend TypeScript helper checks")
        script = textwrap.dedent(
            """
            const fs = require("fs");
            const path = require("path");
            const vm = require("vm");
            const ts = require("./frontend-demo/node_modules/typescript");
            const source = fs.readFileSync(
              path.join(process.cwd(), "frontend-demo/src/lib/candidateSyncSummary.ts"),
              "utf8",
            );
            const lifecycleSource = fs.readFileSync(
              path.join(process.cwd(), "frontend-demo/src/lib/resultViewLifecycle.ts"),
              "utf8",
            );
            const lifecycleCompiled = ts.transpileModule(lifecycleSource, {
              compilerOptions: { module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2020 },
            }).outputText;
            const lifecycleModule = { exports: {} };
            vm.runInNewContext(
              lifecycleCompiled,
              { module: lifecycleModule, exports: lifecycleModule.exports, require, console },
              { filename: "resultViewLifecycle.js" },
            );
            const compiled = ts.transpileModule(source, {
              compilerOptions: { module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2020 },
            }).outputText;
            const module = { exports: {} };
            const localRequire = (specifier) => {
              if (specifier === "./resultViewLifecycle") {
                return lifecycleModule.exports;
              }
              return require(specifier);
            };
            vm.runInNewContext(compiled, { module, exports: module.exports, require: localRequire, console }, {
              filename: "candidateSyncSummary.js",
            });
            const { buildCandidateSyncSummary } = module.exports;
            const summary = buildCandidateSyncSummary({
              loadedCandidateCount: 111,
              expectedCandidateCount: 297,
              boardRuntimeState: {
                publicationStatus: "partial",
                expectedCandidateCount: 297,
                servedCandidateCount: 111,
                publishedCandidateCount: 111,
                displayReadyCandidateCount: 111,
                rowHydrationTargetCount: 111,
                syncStatusText: "111/297",
                profileFetchStatusText: "本次 LinkedIn Profile 已取回 111 张",
                cardMaterializationStatusText: "卡片详情已合入看板 111 张",
                syncNoteLines: [
                  { id: "candidate_discovery", text: "候选人发现 297/297" },
                  { id: "profile_fetch", text: "本次 LinkedIn Profile 已取回 111/297" },
                  { id: "card_materialization", text: "卡片详情已合入看板 111/297" },
                ],
              },
            });
            console.log(JSON.stringify(summary));
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
        self.assertEqual(
            payload["noteText"],
            "候选人发现 297/297；本次 LinkedIn Profile 已取回 111/297；卡片详情已合入看板 111/297",
        )
        self.assertNotIn("111 张", payload["noteText"])


if __name__ == "__main__":
    unittest.main()
