from __future__ import annotations

import json
import shutil
import subprocess
import textwrap
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]


class FrontendC1aContractTest(unittest.TestCase):
    def _run_contract_cases(self) -> dict[str, object]:
        if shutil.which("node") is None:
            self.skipTest("node is required for frontend TypeScript contract checks")
        script = textwrap.dedent(
            r"""
            const fs = require("fs");
            const path = require("path");
            const vm = require("vm");
            const ts = require("./frontend-demo/node_modules/typescript");

            function loadModule(relativePath, overrides = {}, globals = {}) {
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
                return require(specifier);
              };
              vm.runInNewContext(
                compiled,
                {
                  module,
                  exports: module.exports,
                  require: localRequire,
                  console,
                  URL,
                  URLSearchParams,
                  Headers,
                  AbortController,
                  DOMException,
                  Blob,
                  FormData,
                  crypto: { randomUUID: () => "test-c1a-id" },
                  setTimeout,
                  clearTimeout,
                  ...globals,
                },
                { filename: path.basename(relativePath) },
              );
              return module.exports;
            }

            const workflowStatus = loadModule("frontend-demo/src/lib/workflowStatus.ts");
            const workflow = loadModule(
              "frontend-demo/src/lib/workflow.ts",
              {
                "./historySummary": { summarizeSearchQuery: () => "pretraining researchers" },
                "./time": {
                  formatWorkflowTimestamp: (value) => value,
                  parseWorkflowTimestamp: (value) => new Date(value),
                },
                "./workflowStatus": workflowStatus,
              },
            );

            const baseOverrides = {
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
                dashboardRowHydrationTargetCount: () => 0,
              },
              "./resultViewLifecycle": {
                lifecycleEffectiveDeltaMaterializedCount: () => 0,
              },
              "./workflowStatus": workflowStatus,
            };

            const api = loadModule("frontend-demo/src/lib/api.ts", baseOverrides);

            const capture = (fn) => {
              try {
                return { ok: true, value: fn() };
              } catch (error) {
                return { ok: false, message: String(error && error.message ? error.message : error) };
              }
            };

            const statusInputs = [
              "queued", "running", "blocked", "completed", "failed", "cancelled",
              "canceled", "detached", "superseded", "mystery", "",
            ];
            const statuses = Object.fromEntries(statusInputs.map((value) => [
              value || "<missing>",
              workflowStatus.resolveWorkflowStatus(value),
            ]));
            const emptyTimelineCases = Object.fromEntries(
              ["queued", "running", "blocked", "completed", "failed", "cancelled", "mystery"].map(
                (status) => [
                  status,
                  workflow.buildTimelineSteps(
                    { status, timeline: [] },
                    "Find pretraining researchers",
                    { targetCompany: "OpenAI" },
                  )[0],
                ],
              ),
            );

            const handleCases = {
              valid: capture(() => api.__testRequireExactExportArtifactHandle(
                "task-1",
                { artifact: { handle: "/api/exports/task-1/artifact" } },
              )),
              missing: capture(() => api.__testRequireExactExportArtifactHandle("task-1", {})),
              absolute: capture(() => api.__testRequireExactExportArtifactHandle(
                "task-1", { artifact: { handle: "https://example.com/api/exports/task-1/artifact" } },
              )),
              protocolRelative: capture(() => api.__testRequireExactExportArtifactHandle(
                "task-1", { artifact: { handle: "//example.com/api/exports/task-1/artifact" } },
              )),
              wrongId: capture(() => api.__testRequireExactExportArtifactHandle(
                "task-1", { artifact: { handle: "/api/exports/task-2/artifact" } },
              )),
              query: capture(() => api.__testRequireExactExportArtifactHandle(
                "task-1", { artifact: { handle: "/api/exports/task-1/artifact?download=1" } },
              )),
              fragment: capture(() => api.__testRequireExactExportArtifactHandle(
                "task-1", { artifact: { handle: "/api/exports/task-1/artifact#download" } },
              )),
              dotSegment: capture(() => api.__testRequireExactExportArtifactHandle(
                "task-1", { artifact: { handle: "/api/exports/task-1/../task-1/artifact" } },
              )),
              encodedSeparator: capture(() => api.__testRequireExactExportArtifactHandle(
                "task-1", { artifact: { handle: "/api/exports/task-1%2Fother/artifact" } },
              )),
            };
            const taskIdCases = {
              valid: capture(() => api.__testRequireExactExportTaskId("task-1")),
              empty: capture(() => api.__testRequireExactExportTaskId("")),
              dot: capture(() => api.__testRequireExactExportTaskId(".")),
              dotdot: capture(() => api.__testRequireExactExportTaskId("..")),
              slash: capture(() => api.__testRequireExactExportTaskId("task/1")),
              encodedSlash: capture(() => api.__testRequireExactExportTaskId("task%2F1")),
              mixedEncodedDot: capture(() => api.__testRequireExactExportTaskId(".%2e")),
              doubleEncodedDot: capture(() => api.__testRequireExactExportTaskId("%252e%252e")),
              encodedAlphanumericAlias: capture(() => api.__testRequireExactExportTaskId("%74ask-2")),
              query: capture(() => api.__testRequireExactExportTaskId("task?1")),
              fragment: capture(() => api.__testRequireExactExportTaskId("task#1")),
            };

            async function exerciseExport(initialPayload, pollPayload = null) {
              const calls = [];
              const responses = [initialPayload];
              if (pollPayload) {
                responses.push(pollPayload);
              }
              responses.push("binary");
              const headers = new Headers({
                "Content-Type": "application/zip",
                "Content-Disposition": 'attachment; filename="projection.zip"',
                "X-Sourcing-Membership-Revision": "rev-1",
                "X-Sourcing-Projection-Id": "projection-1",
                "X-Sourcing-Source-Candidate-Count": "1",
                "X-Sourcing-Export-Record-Count": "1",
                "X-Sourcing-Exported-Record-Count": "1",
                "X-Sourcing-Skipped-Assertion-Count": "0",
              });
              const fakeFetch = async (url, options) => {
                calls.push({ url: String(url), method: String((options && options.method) || "GET") });
                const next = responses.shift();
                if (next === "binary") {
                  return {
                    ok: true,
                    status: 200,
                    headers,
                    text: async () => "",
                    blob: async () => new Blob(["zip"], { type: "application/zip" }),
                  };
                }
                return {
                  ok: true,
                  status: 200,
                  headers: new Headers({ "Content-Type": "application/json" }),
                  text: async () => JSON.stringify(next),
                  blob: async () => new Blob([]),
                };
              };
              const localStorage = {
                values: new Map(),
                getItem(key) { return this.values.get(key) || null; },
                setItem(key, value) { this.values.set(key, String(value)); },
                removeItem(key) { this.values.delete(key); },
              };
              const browserWindow = {
                location: { hostname: "localhost", protocol: "http:", search: "" },
                localStorage,
              };
              const immediatePollTimer = (callback, delay) => {
                if (delay === 1500) {
                  callback();
                  return 0;
                }
                return setTimeout(callback, delay);
              };
              const transportApi = loadModule(
                "frontend-demo/src/lib/api.ts",
                baseOverrides,
                { window: browserWindow, fetch: fakeFetch, setTimeout: immediatePollTimer },
              );
              let error = "";
              try {
                await transportApi.exportProjectionCandidatesArchive({
                  projectionId: "projection-1",
                  expectedMembershipRevision: "rev-1",
                });
              } catch (caught) {
                error = String(caught && caught.message ? caught.message : caught);
              }
              return { calls, error };
            }

            (async () => {
              const validImmediate = await exerciseExport({
                task_id: "task-1",
                status: "succeeded",
                artifact: { handle: "/api/exports/task-1/artifact" },
              });
              const validQueued = await exerciseExport(
                { task_id: "task-1", status: "queued" },
                {
                  task_id: "task-1",
                  status: "succeeded",
                  artifact: { handle: "/api/exports/task-1/artifact" },
                },
              );
              const invalidHandle = await exerciseExport({
                task_id: "task-1",
                status: "succeeded",
                artifact: { handle: "https://example.com/api/exports/task-1/artifact" },
              });
              const invalidTaskId = await exerciseExport({
                task_id: "%74ask-2",
                status: "succeeded",
                artifact: { handle: "/api/exports/%2574ask-2/artifact" },
              });
              console.log(JSON.stringify({
                statuses,
                emptyTimelineCases,
                handleCases,
                taskIdCases,
                planPending: workflowStatus.isPlanSubmitPendingStatus("pending"),
                planQueued: workflowStatus.isPlanSubmitPendingStatus("queued"),
                planRunning: workflowStatus.isPlanSubmitPendingStatus("running"),
                launchReused: workflowStatus.normalizeWorkflowLaunchStatus("reused_completed_job", "running"),
                launchJoinedCancelled: workflowStatus.normalizeWorkflowLaunchStatus(
                  "joined_existing_job", "superseded",
                ),
                cancelledProgress: api.__testMapProgressPayloadToRunStatus({
                  job_id: "job-cancelled",
                  status: "superseded",
                  progress: {
                    events: [{ id: "event-1", stage: "acquiring", status: "running" }],
                  },
                }),
                unknownProgress: api.__testMapProgressPayloadToRunStatus({
                  job_id: "job-unknown",
                  status: "unexpected_status",
                  progress: {
                    events: [{ id: "event-1", stage: "acquiring", status: "running" }],
                  },
                }),
                postCompletionProgress: api.__testMapProgressPayloadToRunStatus({
                  job_id: "job-post-completion",
                  status: "completed",
                  progress: {
                    worker_summary: { by_status: { running: 1 } },
                    events: [{ id: "event-1", stage: "retrieving", status: "running" }],
                  },
                }),
                summarizedCancelledProgress: api.__testMapProgressPayloadToRunStatus({
                  job_id: "job-summarized-cancelled",
                  status: "superseded",
                  stage: "acquiring",
                  linkedin_stage_1_progress: {
                    current_search_returned_count: 2,
                    profile_fetch_required_count: 2,
                    profile_fetched_count: 0,
                  },
                }),
                validImmediate,
                validQueued,
                invalidHandle,
                invalidTaskId,
              }));
            })().catch((error) => {
              console.error(error);
              process.exitCode = 1;
            });
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

    def test_workflow_status_registry_is_terminal_total(self) -> None:
        result = self._run_contract_cases()
        statuses = result["statuses"]

        self.assertEqual(statuses["queued"]["status"], "queued")
        self.assertFalse(statuses["queued"]["terminal"])
        self.assertEqual(statuses["running"]["status"], "running")
        self.assertFalse(statuses["blocked"]["terminal"])
        self.assertEqual(statuses["completed"]["outcome"], "succeeded")
        self.assertTrue(statuses["failed"]["terminal"])
        for alias in ("cancelled", "canceled", "detached", "superseded"):
            self.assertEqual(statuses[alias]["status"], "cancelled")
            self.assertTrue(statuses[alias]["terminal"])
        self.assertEqual(statuses["mystery"]["reason"], "unknown_domain_status")
        self.assertTrue(statuses["mystery"]["terminal"])
        self.assertEqual(statuses["<missing>"]["reason"], "missing_domain_status")
        self.assertTrue(statuses["<missing>"]["terminal"])
        self.assertEqual(result["launchReused"], "completed")
        self.assertEqual(result["launchJoinedCancelled"], "cancelled")
        self.assertEqual(result["cancelledProgress"]["status"], "cancelled")
        self.assertEqual(result["cancelledProgress"]["timeline"][0]["status"], "cancelled")
        self.assertEqual(result["unknownProgress"]["status"], "failed")
        self.assertEqual(result["unknownProgress"]["timeline"][0]["status"], "failed")
        self.assertEqual(result["postCompletionProgress"]["status"], "running")
        self.assertEqual(result["postCompletionProgress"]["timeline"][0]["status"], "running")
        self.assertEqual(result["summarizedCancelledProgress"]["status"], "cancelled")
        self.assertNotIn(
            "running",
            [item["status"] for item in result["summarizedCancelledProgress"]["timeline"]],
        )

    def test_empty_terminal_timeline_uses_canonical_overall_status(self) -> None:
        cases = self._run_contract_cases()["emptyTimelineCases"]

        self.assertEqual(cases["queued"]["status"], "running")
        self.assertEqual(cases["running"]["status"], "running")
        self.assertEqual(cases["blocked"]["status"], "pending")
        self.assertEqual(cases["completed"]["status"], "completed")
        self.assertEqual(cases["failed"]["status"], "failed")
        self.assertEqual(cases["cancelled"]["status"], "cancelled")
        self.assertEqual(cases["mystery"]["status"], "failed")
        for terminal_status in ("completed", "failed", "cancelled", "mystery"):
            self.assertNotIn("正在准备执行", cases[terminal_status]["title"])

    def test_plan_submit_accepts_only_pending_or_queued_bridge_states(self) -> None:
        result = self._run_contract_cases()

        self.assertTrue(result["planPending"])
        self.assertTrue(result["planQueued"])
        self.assertFalse(result["planRunning"])

    def test_export_handle_and_task_id_validation_fail_closed(self) -> None:
        result = self._run_contract_cases()

        self.assertTrue(result["handleCases"]["valid"]["ok"])
        for case in (
            "missing",
            "absolute",
            "protocolRelative",
            "wrongId",
            "query",
            "fragment",
            "dotSegment",
            "encodedSeparator",
        ):
            self.assertFalse(result["handleCases"][case]["ok"], case)
        self.assertTrue(result["taskIdCases"]["valid"]["ok"])
        for case in (
            "empty",
            "dot",
            "dotdot",
            "slash",
            "encodedSlash",
            "mixedEncodedDot",
            "doubleEncodedDot",
            "encodedAlphanumericAlias",
            "query",
            "fragment",
        ):
            self.assertFalse(result["taskIdCases"][case]["ok"], case)

    def test_export_transport_uses_exact_owner_handle_for_immediate_and_polled_success(self) -> None:
        result = self._run_contract_cases()

        self.assertEqual(
            [call["url"] for call in result["validImmediate"]["calls"]],
            [
                "http://127.0.0.1:8765/api/projections/export",
                "http://127.0.0.1:8765/api/exports/task-1/artifact",
            ],
        )
        self.assertEqual(result["validImmediate"]["error"], "")
        self.assertEqual(
            [call["url"] for call in result["validQueued"]["calls"]],
            [
                "http://127.0.0.1:8765/api/projections/export",
                "http://127.0.0.1:8765/api/exports/task-1",
                "http://127.0.0.1:8765/api/exports/task-1/artifact",
            ],
        )
        self.assertEqual(result["validQueued"]["error"], "")
        self.assertEqual(len(result["invalidHandle"]["calls"]), 1)
        self.assertIn("exact canonical artifact handle", result["invalidHandle"]["error"])
        self.assertEqual(len(result["invalidTaskId"]["calls"]), 1)
        self.assertIn("valid single path segment", result["invalidTaskId"]["error"])

    def test_public_export_callers_keep_task_id_between_submit_wait_and_download(self) -> None:
        source = (REPO_ROOT / "frontend-demo/src/lib/api.ts").read_text(encoding="utf-8")

        self.assertNotIn("submitAndDownloadExport", source)
        self.assertEqual(source.count("const submission = await submitExportTask("), 2)
        self.assertEqual(
            source.count("await waitForExportArtifact(submission.taskId, submission.payload)"),
            2,
        )
        self.assertEqual(
            source.count("await downloadExportArtifact(submission.taskId, completedPayload)"),
            2,
        )
        self.assertIn("const terminalJobStatus = resolveWorkflowStatus(jobStatus).terminal", source)
        self.assertNotIn('["completed", "failed"].includes(jobStatus)', source)

    def test_excel_terminal_launch_is_persisted_without_restarting_polling(self) -> None:
        source = (REPO_ROOT / "frontend-demo/src/pages/SearchPage.tsx").read_text(encoding="utf-8")
        start = source.index("const startExcelWorkflow")
        end = source.index("const applyRevision", start)
        section = source[start:end]

        self.assertIn("normalizeWorkflowStatus(group.runStatus?.status || group.status)", section)
        self.assertIn('phase: isWorkflowStatusTerminal(status) ? "results" : "running"', section)
        self.assertIn("errorMessage: terminalMessage", section)
        self.assertIn('status === "cancelled"', section)

    def test_polled_terminal_status_clears_loading_before_preview_warmup(self) -> None:
        source = (REPO_ROOT / "frontend-demo/src/pages/SearchPage.tsx").read_text(encoding="utf-8")
        poll_start = source.index("const startProgressPolling")
        poll_end = source.index("const startExcelWorkflow", poll_start)
        poll_section = source[poll_start:poll_end]
        terminal_guard = 'isWorkflowStatusTerminal(nextRunStatus.status) && nextRunStatus.status !== "completed"'
        terminal_index = poll_section.index(terminal_guard)
        warmup_index = poll_section.index("warmDashboardForPreviewReadyJob(jobId, nextRunStatus, requestEpoch)")

        self.assertLess(terminal_index, warmup_index)
        terminal_block = poll_section[terminal_index:warmup_index]
        self.assertIn("setIsLoadingResults(false)", terminal_block)

        warm_start = source.index("const warmDashboardForPreviewReadyJob")
        warm_end = source.index("const startProgressPolling", warm_start)
        warm_section = source[warm_start:warm_end]
        self.assertGreaterEqual(warm_section.count('currentFlow.phase !== "running"'), 3)

    def test_history_hydration_orders_status_before_dashboard_and_preserves_terminal_error(self) -> None:
        source = (REPO_ROOT / "frontend-demo/src/pages/SearchPage.tsx").read_text(encoding="utf-8")
        hydrate_start = source.index("const hydrateFromHistory")
        hydrate_end = source.index("const recoverHistoryFromBackend", hydrate_start)
        section = source[hydrate_start:hydrate_end]

        status_index = section.index(
            "const restoredRunStatus = await sourcingBackendClient.getWorkflowProgress(recoveredItem.jobId)"
        )
        dashboard_index = section.index("const restoredDashboard = await getDashboard(recoveredItem.jobId)")
        self.assertLess(status_index, dashboard_index)
        self.assertNotIn("void sourcingBackendClient.getWorkflowProgress", section)
        self.assertGreaterEqual(section.count("setErrorMessage(restoredError)"), 2)
        self.assertNotIn('setErrorMessage("")', section)
        self.assertIn("errorMessage: restoredError", section)


if __name__ == "__main__":
    unittest.main()
