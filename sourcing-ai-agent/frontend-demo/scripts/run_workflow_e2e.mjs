#!/usr/bin/env node

import fs from "node:fs/promises";
import path from "node:path";
import process from "node:process";
import { fileURLToPath } from "node:url";
import { chromium } from "playwright";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const frontendRoot = path.resolve(__dirname, "..");
const repoRoot = path.resolve(frontendRoot, "..");
const outputRoot = path.join(repoRoot, "output", "playwright");
const CANDIDATE_SYNC_STALE_COMPLETE_BUDGET_MS = 2500;
const MATERIALIZATION_STUCK_AFTER_FETCH_BUDGET_MS = 30000;
const RESULTS_PAGE_SIZE = 24;

process.env.PLAYWRIGHT_BROWSERS_PATH ||= path.join(repoRoot, ".cache", "ms-playwright");
const playwrightLdLibraryPath = path.join(repoRoot, ".cache", "ubuntu-libs", "root", "usr", "lib", "x86_64-linux-gnu");
process.env.LD_LIBRARY_PATH = process.env.LD_LIBRARY_PATH
  ? `${playwrightLdLibraryPath}:${process.env.LD_LIBRARY_PATH}`
  : playwrightLdLibraryPath;

function parseArgs(argv) {
  const options = {
    frontendUrl: "http://127.0.0.1:4173",
    startUrl: "",
    query: "我想了解Humans&里偏Coding agents方向的研究成员",
    timeoutMs: 60000,
    screenshotPath: path.join(outputRoot, "workflow-e2e.png"),
    checkPaginationStability: false,
    paginationTargetPage: 2,
    paginationHydrationTimeoutMs: 20000,
    restoreExistingResults: false,
    observeDeltaStreaming: false,
    apiBaseUrl: "",
    expectedBaselineSnapshotId: "",
    expectedBaselineMinCount: 0,
    deltaObservationPollMs: 500,
    signalSharedRecovery: false,
    driveProviderWebhookEvents: false,
    providerWebhookToken: process.env.SOURCING_SCRIPTED_PROVIDER_WEBHOOK_TOKEN || "",
    workerRecoveryPollMs: 1000,
  };
  for (let index = 0; index < argv.length; index += 1) {
    const current = argv[index];
    const next = argv[index + 1];
    if (current === "--frontend-url" && next) {
      options.frontendUrl = next;
      index += 1;
      continue;
    }
    if (current === "--query" && next) {
      options.query = next;
      index += 1;
      continue;
    }
    if (current === "--start-url" && next) {
      options.startUrl = next;
      index += 1;
      continue;
    }
    if (current === "--timeout-ms" && next) {
      options.timeoutMs = Number(next) || options.timeoutMs;
      index += 1;
      continue;
    }
    if (current === "--screenshot" && next) {
      options.screenshotPath = next;
      index += 1;
      continue;
    }
    if (current === "--check-pagination-stability") {
      options.checkPaginationStability = true;
      continue;
    }
    if (current === "--restore-existing-results") {
      options.restoreExistingResults = true;
      continue;
    }
    if (current === "--observe-delta-streaming") {
      options.observeDeltaStreaming = true;
      continue;
    }
    if (current === "--api-base-url" && next) {
      options.apiBaseUrl = next;
      index += 1;
      continue;
    }
    if (current === "--expected-baseline-snapshot-id" && next) {
      options.expectedBaselineSnapshotId = next;
      index += 1;
      continue;
    }
    if (current === "--expected-baseline-min-count" && next) {
      options.expectedBaselineMinCount = Math.max(0, Number(next) || 0);
      index += 1;
      continue;
    }
    if (current === "--delta-observation-poll-ms" && next) {
      options.deltaObservationPollMs = Math.max(100, Number(next) || options.deltaObservationPollMs);
      index += 1;
      continue;
    }
    if (current === "--drive-worker-recovery" || current === "--signal-shared-recovery") {
      options.signalSharedRecovery = true;
      continue;
    }
    if (current === "--drive-provider-webhook-events") {
      options.driveProviderWebhookEvents = true;
      continue;
    }
    if (current === "--provider-webhook-token" && next) {
      options.providerWebhookToken = next;
      index += 1;
      continue;
    }
    if (current === "--worker-recovery-poll-ms" && next) {
      options.workerRecoveryPollMs = Math.max(250, Number(next) || options.workerRecoveryPollMs);
      index += 1;
      continue;
    }
    if (current === "--pagination-target-page" && next) {
      options.paginationTargetPage = Math.max(1, Number(next) || options.paginationTargetPage);
      index += 1;
      continue;
    }
    if (current === "--pagination-hydration-timeout-ms" && next) {
      options.paginationHydrationTimeoutMs = Math.max(1000, Number(next) || options.paginationHydrationTimeoutMs);
      index += 1;
      continue;
    }
    if (current === "--help" || current === "-h") {
      process.stdout.write(
        [
          "Usage:",
          "  node ./scripts/run_workflow_e2e.mjs --frontend-url http://127.0.0.1:4173 \\",
          '    --query "我想了解Humans&里偏Coding agents方向的研究成员"',
          "  node ./scripts/run_workflow_e2e.mjs --start-url 'http://127.0.0.1:4173/?history=...&job=...' --restore-existing-results",
          "    [--check-pagination-stability --pagination-target-page 2 --pagination-hydration-timeout-ms 20000]",
          "    [--observe-delta-streaming --api-base-url http://127.0.0.1:8765 --expected-baseline-snapshot-id 20260414T120300]",
          "    [--signal-shared-recovery --worker-recovery-poll-ms 1000]",
          "    [--drive-worker-recovery  # compatibility alias; signal-only, never executes a tick]",
          "    [--drive-provider-webhook-events --provider-webhook-token test-token --worker-recovery-poll-ms 1000]",
        ].join("\n"),
      );
      process.stdout.write("\n");
      process.exit(0);
    }
  }
  return options;
}

async function extractPlanFields(planCard) {
  const dtNodes = await planCard.locator("dt").allTextContents();
  const ddNodes = await planCard.locator("dd").allTextContents();
  const fields = {};
  dtNodes.forEach((label, index) => {
    const normalizedLabel = String(label || "").trim();
    const normalizedValue = String(ddNodes[index] || "").trim();
    if (normalizedLabel) {
      fields[normalizedLabel] = normalizedValue;
    }
  });
  return fields;
}

async function extractPlanMetadata(planCard) {
  return planCard.evaluate((node) => ({
    dispatchStrategy: node.getAttribute("data-plan-dispatch-strategy") || "",
    plannerMode: node.getAttribute("data-plan-planner-mode") || "",
    requiresDeltaAcquisition: (node.getAttribute("data-plan-requires-delta-acquisition") || "") === "true",
    currentLaneBehavior: node.getAttribute("data-plan-current-lane-behavior") || "",
    formerLaneBehavior: node.getAttribute("data-plan-former-lane-behavior") || "",
    defaultAcquisitionMode: node.getAttribute("data-plan-default-acquisition-mode") || "",
    organizationScaleBand: node.getAttribute("data-plan-organization-scale-band") || "",
    baselineSnapshotId: node.getAttribute("data-plan-baseline-snapshot-id") || "",
  }));
}

function apiUrl(baseUrl, pathname) {
  const normalizedBase = String(baseUrl || "").replace(/\/+$/, "");
  const normalizedPath = String(pathname || "").startsWith("/") ? pathname : `/${pathname}`;
  return `${normalizedBase}${normalizedPath}`;
}

function extractJobIdFromUrl(value) {
  try {
    const parsed = new URL(String(value || ""));
    return parsed.searchParams.get("job") || "";
  } catch {
    return "";
  }
}

function extractHistoryIdFromUrl(value) {
  try {
    const parsed = new URL(String(value || ""));
    return parsed.searchParams.get("history") || "";
  } catch {
    return "";
  }
}

async function fetchApiJson(baseUrl, pathname) {
  if (!baseUrl) {
    throw new Error("api base url missing");
  }
  const response = await fetch(apiUrl(baseUrl, pathname), {
    headers: {
      accept: "application/json",
    },
  });
  if (!response.ok) {
    throw new Error(`Request failed: ${response.status}`);
  }
  return response.json();
}

async function fetchBrowserJson(page, pathname) {
  return page.evaluate(async (targetPathname) => {
    const response = await fetch(targetPathname, {
      headers: {
        accept: "application/json",
      },
    });
    if (!response.ok) {
      throw new Error(`Request failed: ${response.status}`);
    }
    return response.json();
  }, pathname);
}

async function postApiJson(baseUrl, pathname, payload, headers = {}) {
  if (!baseUrl) {
    throw new Error("api base url missing");
  }
  const response = await fetch(apiUrl(baseUrl, pathname), {
    method: "POST",
    headers: {
      accept: "application/json",
      "content-type": "application/json",
      ...headers,
    },
    body: JSON.stringify(payload || {}),
  });
  if (!response.ok) {
    throw new Error(`Request failed: ${response.status}`);
  }
  return response.json();
}

function firstString(...values) {
  for (const value of values) {
    const text = String(value || "").trim();
    if (text) {
      return text;
    }
  }
  return "";
}

function numberValue(...values) {
  for (const value of values) {
    const parsed = Number(value);
    if (Number.isFinite(parsed) && parsed > 0) {
      return parsed;
    }
  }
  return 0;
}

function collectNumericRegressions(samples, selectors) {
  const regressions = {};
  for (const [key, selector] of Object.entries(selectors)) {
    let peak = 0;
    let regressionCount = 0;
    let largestDrop = 0;
    let peakBeforeDrop = 0;
    let latestValue = 0;
    for (const sample of samples) {
      const value = Number(selector(sample) || 0);
      if (!Number.isFinite(value)) {
        continue;
      }
      latestValue = value;
      if (value < peak) {
        regressionCount += 1;
        const drop = peak - value;
        largestDrop = Math.max(largestDrop, drop);
        peakBeforeDrop = Math.max(peakBeforeDrop, peak);
      }
      peak = Math.max(peak, value);
    }
    if (regressionCount > 0) {
      regressions[key] = {
        regressionCount,
        largestDrop,
        peakBeforeDrop,
        latestValue,
      };
    }
  }
  return regressions;
}

function summarizeBoardRuntimeState(boardRuntimeState) {
  return {
    schemaVersion: numberValue(boardRuntimeState?.schema_version),
    jobId: firstString(boardRuntimeState?.job_id),
    resultMode: firstString(boardRuntimeState?.result_mode) === "asset_population" ? "asset_population" : "ranked_results",
    phase: firstString(boardRuntimeState?.phase),
    publicationStatus: firstString(boardRuntimeState?.publication_status),
    expectedCandidateCount: numberValue(boardRuntimeState?.expected_candidate_count),
    servedCandidateCount: numberValue(boardRuntimeState?.served_candidate_count),
    publishedCandidateCount: numberValue(boardRuntimeState?.published_candidate_count),
    displayReadyCandidateCount: numberValue(boardRuntimeState?.display_ready_candidate_count),
    previewCandidateCount: numberValue(boardRuntimeState?.preview_candidate_count),
    profileDetailCandidateCount: numberValue(boardRuntimeState?.profile_detail_candidate_count),
    explicitProfileCaptureCandidateCount: numberValue(boardRuntimeState?.explicit_profile_capture_candidate_count),
    needsProfileCompletionCandidateCount: numberValue(boardRuntimeState?.needs_profile_completion_candidate_count),
    lowProfileRichnessCandidateCount: numberValue(boardRuntimeState?.low_profile_richness_candidate_count),
    cardMaterializationQualityFieldsAvailable: Boolean(boardRuntimeState?.card_materialization_quality_fields_available),
    rowHydrationTargetCount: numberValue(boardRuntimeState?.row_hydration_target_count),
    baselineCandidateCount: numberValue(boardRuntimeState?.baseline_candidate_count),
    deltaProfileRequiredCount: numberValue(boardRuntimeState?.delta_profile_required_count),
    deltaProfileFetchedCount: numberValue(boardRuntimeState?.delta_profile_fetched_count),
    deltaProfileMaterializedCount: numberValue(boardRuntimeState?.delta_profile_materialized_count),
    deltaProfileBoardVisibleCount: numberValue(boardRuntimeState?.delta_profile_board_visible_count),
    deltaProfileDenominatorPromoted: Boolean(boardRuntimeState?.delta_profile_denominator_promoted),
    rowPublicationSequence: numberValue(boardRuntimeState?.row_publication_sequence),
    rowPublicationWatermark: firstString(boardRuntimeState?.row_publication_watermark),
    rowPublicationUpdatedAt: firstString(boardRuntimeState?.row_publication_updated_at),
    facetSummaryStatus: firstString(boardRuntimeState?.facet_summary_status),
    facetSummaryScope: firstString(boardRuntimeState?.facet_summary_scope),
    facetSummaryCandidateCount: numberValue(boardRuntimeState?.facet_summary_candidate_count),
    layeringStatus: firstString(boardRuntimeState?.layering_status),
    filterContract: {
      source: firstString(boardRuntimeState?.filter_contract?.source),
      facetCountScope: firstString(boardRuntimeState?.filter_contract?.facet_count_scope),
      rowFilterScope: firstString(boardRuntimeState?.filter_contract?.row_filter_scope),
      backendFilteredPagingSupported: Boolean(boardRuntimeState?.filter_contract?.backend_filtered_paging_supported),
    },
    syncStatusText: firstString(boardRuntimeState?.sync_status_text),
    profileFetchStatusText: firstString(boardRuntimeState?.profile_fetch_status_text),
    cardMaterializationStatusText: firstString(boardRuntimeState?.card_materialization_status_text),
    noteText: firstString(boardRuntimeState?.note_text),
  };
}

function summarizeComparableBoardRuntimeState(boardRuntimeState) {
  const summarized = summarizeBoardRuntimeState(boardRuntimeState);
  return {
    expectedCandidateCount: summarized.expectedCandidateCount,
    servedCandidateCount: summarized.servedCandidateCount,
    publishedCandidateCount: summarized.publishedCandidateCount,
    displayReadyCandidateCount: summarized.displayReadyCandidateCount,
    phase: summarized.phase,
    layeringStatus: summarized.layeringStatus,
    deltaProfileDenominatorPromoted: summarized.deltaProfileDenominatorPromoted,
    cardMaterializationStatusText: summarized.cardMaterializationStatusText,
    profileFetchStatusText: summarized.profileFetchStatusText,
    rowPublicationSequence: summarized.rowPublicationSequence,
    rowPublicationWatermark: summarized.rowPublicationWatermark,
  };
}

function parseCandidateSyncCounts(text) {
  const normalized = String(text || "").replace(/\s+/g, " ").trim();
  const match = normalized.match(/候选人同步\s*(\d+)\s*\/\s*(\d+)/);
  if (!match) {
    return { loaded: 0, expected: 0 };
  }
  return { loaded: Number(match[1]) || 0, expected: Number(match[2]) || 0 };
}

function candidateSyncLooksPrematurelyComplete(sample) {
  const loaded = Number(sample.ui?.candidateSyncLoadedCount || 0);
  const expected = Number(sample.ui?.candidateSyncExpectedCount || 0);
  if (expected <= 0 || loaded < expected) {
    return false;
  }
  const dashboardProgress = sample.dashboard?.linkedinStage1Progress || {};
  const progressPayload = sample.progress?.linkedinStage1Progress || {};
  const required = Math.max(
    Number(dashboardProgress.profileFetchRequiredCount || 0),
    Number(progressPayload.profileFetchRequiredCount || 0),
  );
  const fetched = Math.max(
    Number(dashboardProgress.profileFetchedCount || 0),
    Number(progressPayload.profileFetchedCount || 0),
  );
  return required > 0 && fetched < required;
}

function collectBudgetExceededPrematureCandidateSyncSamples(samples, budgetMs) {
  const failures = [];
  for (let index = 0; index < samples.length; index += 1) {
    const sample = samples[index];
    if (!candidateSyncLooksPrematurelyComplete(sample)) {
      continue;
    }
    const startedAt = Number(sample.offsetMs || 0);
    const resolvedSample = samples.slice(index + 1).find((candidate) => {
      if (candidate.jobId !== sample.jobId) {
        return false;
      }
      return !candidateSyncLooksPrematurelyComplete(candidate);
    });
    if (!resolvedSample) {
      const lastSameJobSample = [...samples].reverse().find((candidate) => candidate.jobId === sample.jobId) || sample;
      const unresolvedMs = Number(lastSameJobSample.offsetMs || startedAt) - startedAt;
      if (unresolvedMs > budgetMs) {
        failures.push({ ...sample, candidateSyncPrematureCompleteLagMs: unresolvedMs });
      }
      continue;
    }
    const resolvedMs = Number(resolvedSample.offsetMs || startedAt) - startedAt;
    if (resolvedMs > budgetMs) {
      failures.push({ ...sample, candidateSyncPrematureCompleteLagMs: resolvedMs });
    }
  }
  return failures;
}

function sampleLooksMaterializationStuckAfterFetch(sample) {
  const lifecycle = sample.dashboard?.resultViewLifecycle || {};
  const required = Number(lifecycle.deltaProfileRequiredCount || 0);
  const fetched = Number(lifecycle.deltaProfileFetchedCount || 0);
  const baseline = Number(lifecycle.baselineCandidateCount || 0);
  const served = Number(lifecycle.servedCandidateCount || 0);
  return required > 0 && fetched >= required && baseline > 0 && served <= baseline;
}

function collectBudgetExceededMaterializationStuckSamples(samples, budgetMs) {
  const failures = [];
  for (let index = 0; index < samples.length; index += 1) {
    const sample = samples[index];
    if (!sampleLooksMaterializationStuckAfterFetch(sample)) {
      continue;
    }
    const startedAt = Number(sample.offsetMs || 0);
    const resolvedSample = samples.slice(index + 1).find((candidate) => {
      if (candidate.jobId !== sample.jobId) {
        return false;
      }
      return !sampleLooksMaterializationStuckAfterFetch(candidate);
    });
    if (!resolvedSample) {
      const lastSameJobSample = [...samples].reverse().find((candidate) => candidate.jobId === sample.jobId) || sample;
      const unresolvedMs = Number(lastSameJobSample.offsetMs || startedAt) - startedAt;
      if (unresolvedMs > budgetMs) {
        failures.push({ ...sample, materializationStuckAfterFetchLagMs: unresolvedMs });
      }
      continue;
    }
    const resolvedMs = Number(resolvedSample.offsetMs || startedAt) - startedAt;
    if (resolvedMs > budgetMs) {
      failures.push({ ...sample, materializationStuckAfterFetchLagMs: resolvedMs });
    }
  }
  return failures;
}

function summarizeDashboardPayload(payload) {
  const dashboard = payload && typeof payload === "object" ? payload : {};
  const job = dashboard.job && typeof dashboard.job === "object" ? dashboard.job : {};
  const summary = job.summary && typeof job.summary === "object" ? job.summary : {};
  const assetPopulation =
    dashboard.asset_population && typeof dashboard.asset_population === "object"
      ? dashboard.asset_population
      : {};
  const candidateSource =
    assetPopulation.candidate_source && typeof assetPopulation.candidate_source === "object"
      ? assetPopulation.candidate_source
      : summary.candidate_source && typeof summary.candidate_source === "object"
        ? summary.candidate_source
        : {};
  const resultView =
    candidateSource.result_view && typeof candidateSource.result_view === "object"
      ? candidateSource.result_view
      : {};
  const backgroundMaterialization =
    summary.background_snapshot_materialization && typeof summary.background_snapshot_materialization === "object"
      ? summary.background_snapshot_materialization
      : {};
  const lifecycle =
    dashboard.result_view_lifecycle && typeof dashboard.result_view_lifecycle === "object"
      ? dashboard.result_view_lifecycle
      : {};
  const executionPhase =
    dashboard.execution_phase_contract && typeof dashboard.execution_phase_contract === "object"
      ? dashboard.execution_phase_contract
      : {};
  const linkedinProgress =
    dashboard.linkedin_stage_1_progress && typeof dashboard.linkedin_stage_1_progress === "object"
      ? dashboard.linkedin_stage_1_progress
      : {};
  const boardRuntimeState =
    dashboard.board_runtime_state && typeof dashboard.board_runtime_state === "object"
      ? dashboard.board_runtime_state
      : {};
  const candidates = Array.isArray(assetPopulation.candidates)
    ? assetPopulation.candidates
    : Array.isArray(dashboard.results)
      ? dashboard.results
      : [];
  return {
    jobStatus: String(job.status || "").toLowerCase(),
    jobStage: String(job.stage || ""),
    snapshotId: firstString(
      assetPopulation.snapshot_id,
      candidateSource.snapshot_id,
      summary.snapshot_id,
      resultView.snapshot_id,
    ),
    assetPopulationSnapshotId: firstString(assetPopulation.snapshot_id),
    candidateSourceSnapshotId: firstString(candidateSource.snapshot_id),
    resultViewSnapshotId: firstString(resultView.snapshot_id),
    sourceKind: firstString(candidateSource.source_kind, resultView.source_kind),
    viewKind: firstString(candidateSource.result_view_kind, resultView.view_kind),
    materializationGenerationKey: firstString(
      candidateSource.materialization_generation_key,
      resultView.materialization_generation_key,
    ),
    authoritativeSnapshotId: firstString(candidateSource.authoritative_snapshot_id, resultView.authoritative_snapshot_id),
    candidateCount: numberValue(
      assetPopulation.candidate_count,
      candidateSource.candidate_count,
      dashboard.asset_population_count,
      candidates.length,
    ),
    returnedCount: candidates.length,
    backgroundMaterializationStatus: String(backgroundMaterialization.status || ""),
    resultViewLifecycle: {
      state: String(lifecycle.state || ""),
      baselineSnapshotId: String(lifecycle.baseline_snapshot_id || ""),
      currentSnapshotId: String(lifecycle.current_snapshot_id || ""),
      servedSnapshotId: String(lifecycle.served_snapshot_id || ""),
      baselineCandidateCount: numberValue(lifecycle.baseline_candidate_count),
      expectedCandidateCount: numberValue(lifecycle.expected_candidate_count),
      servedCandidateCount: numberValue(lifecycle.served_candidate_count),
      deltaProfileRequiredCount: numberValue(lifecycle.delta_profile_required_count),
      deltaProfileFetchedCount: numberValue(lifecycle.delta_profile_fetched_count),
      deltaProfileMaterializedCount: numberValue(lifecycle.delta_profile_materialized_count),
      deltaProfilePendingCount: numberValue(lifecycle.delta_profile_pending_count),
      deltaProfileQueuedCount: numberValue(lifecycle.delta_profile_queued_count),
    },
    executionPhaseContract: {
      activePhaseId: String(executionPhase.active_phase_id || ""),
      activeStageId: String(executionPhase.active_stage_id || ""),
      activePhaseLabel: String(executionPhase.active_phase_label || ""),
      publicWebStageApplicable: Boolean(executionPhase.public_web_stage_applicable),
      profileWorkPending: Boolean(executionPhase.profile_work_pending),
    },
    linkedinStage1Progress: {
      currentSearchReturnedCount: numberValue(linkedinProgress.current_search_returned_count),
      formerSearchReturnedCount: numberValue(linkedinProgress.former_search_returned_count),
      dedupedCandidateCount: numberValue(linkedinProgress.deduped_candidate_count),
      profileFetchRequiredCount: numberValue(linkedinProgress.profile_fetch_required_count),
      profileFetchedCount: numberValue(linkedinProgress.profile_fetched_count),
    },
    boardRuntimeState: summarizeBoardRuntimeState(boardRuntimeState),
  };
}

function summarizeProgressPayload(payload) {
  const progress = payload && typeof payload === "object" ? payload : {};
  const lifecycle =
    progress.result_view_lifecycle && typeof progress.result_view_lifecycle === "object"
      ? progress.result_view_lifecycle
      : {};
  const executionPhase =
    progress.execution_phase_contract && typeof progress.execution_phase_contract === "object"
      ? progress.execution_phase_contract
      : {};
  const linkedinProgress =
    progress.linkedin_stage_1_progress && typeof progress.linkedin_stage_1_progress === "object"
      ? progress.linkedin_stage_1_progress
      : {};
  const boardRuntimeState =
    progress.board_runtime_state && typeof progress.board_runtime_state === "object"
      ? progress.board_runtime_state
      : {};
  return {
    jobStatus: String(progress.status || "").toLowerCase(),
    jobStage: String(progress.stage || progress.current_stage || ""),
    currentMessage: String(progress.message || progress.current_message || ""),
    resultViewLifecycle: {
      state: String(lifecycle.state || ""),
      baselineSnapshotId: String(lifecycle.baseline_snapshot_id || ""),
      currentSnapshotId: String(lifecycle.current_snapshot_id || ""),
      servedSnapshotId: String(lifecycle.served_snapshot_id || ""),
      baselineCandidateCount: numberValue(lifecycle.baseline_candidate_count),
      expectedCandidateCount: numberValue(lifecycle.expected_candidate_count),
      servedCandidateCount: numberValue(lifecycle.served_candidate_count),
      deltaProfileRequiredCount: numberValue(lifecycle.delta_profile_required_count),
      deltaProfileFetchedCount: numberValue(lifecycle.delta_profile_fetched_count),
      deltaProfileMaterializedCount: numberValue(lifecycle.delta_profile_materialized_count),
    },
    executionPhaseContract: {
      activePhaseId: String(executionPhase.active_phase_id || ""),
      activeStageId: String(executionPhase.active_stage_id || ""),
      activePhaseLabel: String(executionPhase.active_phase_label || ""),
      publicWebStageApplicable: Boolean(executionPhase.public_web_stage_applicable),
      profileWorkPending: Boolean(executionPhase.profile_work_pending),
    },
    linkedinStage1Progress: {
      currentSearchReturnedCount: numberValue(linkedinProgress.current_search_returned_count),
      formerSearchReturnedCount: numberValue(linkedinProgress.former_search_returned_count),
      dedupedCandidateCount: numberValue(linkedinProgress.deduped_candidate_count),
      profileFetchRequiredCount: numberValue(linkedinProgress.profile_fetch_required_count),
      profileFetchedCount: numberValue(linkedinProgress.profile_fetched_count),
    },
    boardRuntimeState: summarizeBoardRuntimeState(boardRuntimeState),
  };
}

function summarizeWorkersPayload(payload) {
  const workers = Array.isArray(payload?.agent_workers) ? payload.agent_workers : [];
  const byStatus = {};
  const harvestProfileWorkers = [];
  for (const worker of workers) {
    const status = String(worker?.status || "").trim().toLowerCase() || "unknown";
    byStatus[status] = Number(byStatus[status] || 0) + 1;
    const metadata = worker?.metadata && typeof worker.metadata === "object" ? worker.metadata : {};
    const recoveryKind = String(metadata.recovery_kind || metadata.recoveryKind || "").trim();
    if (recoveryKind !== "harvest_profile_batch") {
      continue;
    }
    const checkpoint = worker?.checkpoint && typeof worker.checkpoint === "object" ? worker.checkpoint : {};
    const output = worker?.output && typeof worker.output === "object" ? worker.output : {};
    const summary = output.summary && typeof output.summary === "object" ? output.summary : {};
    const inlineIngest =
      output.inline_incremental_ingest && typeof output.inline_incremental_ingest === "object"
        ? output.inline_incremental_ingest
        : {};
    const remote = remoteCheckpointForWorker(worker);
    harvestProfileWorkers.push({
      workerId: Number(worker?.worker_id || worker?.workerId || 0) || 0,
      status,
      stage: String(checkpoint.stage || ""),
      runId: remote.runId,
      datasetId: remote.datasetId,
      scriptedRemoteReadyEpochMs: Number(checkpoint.scripted_remote_ready_epoch_ms || 0) || 0,
      canReceiveProviderWebhook: workerCanReceiveProviderWebhook(worker),
      needsRecovery: workerNeedsScriptedRecovery(worker),
      inlineIngested: Boolean(String(inlineIngest.applied_at || "").trim()),
      requestedUrlCount: Number(summary.requested_url_count || 0) || 0,
      dispatchedUrlCount: Number(summary.dispatched_url_count || 0) || 0,
      persistedProfileCount: Number(output.persisted_profile_count || summary.persisted_profile_count || 0) || 0,
      unresolvedUrlCount: Number(summary.unresolved_url_count || 0) || 0,
      queuedUrlCount: Array.isArray(summary.queued_urls) ? summary.queued_urls.length : 0,
      message: String(summary.message || "").slice(0, 160),
    });
  }
  const profileByStatus = {};
  for (const worker of harvestProfileWorkers) {
    profileByStatus[worker.status] = Number(profileByStatus[worker.status] || 0) + 1;
  }
  return {
    totalCount: workers.length,
    byStatus,
    harvestProfileBatch: {
      totalCount: harvestProfileWorkers.length,
      byStatus: profileByStatus,
      recoverableCount: harvestProfileWorkers.filter((worker) => worker.needsRecovery).length,
      webhookEligibleCount: harvestProfileWorkers.filter((worker) => worker.canReceiveProviderWebhook).length,
      inlineIngestedCount: harvestProfileWorkers.filter((worker) => worker.inlineIngested).length,
      requestedUrlCount: harvestProfileWorkers.reduce((total, worker) => total + worker.requestedUrlCount, 0),
      persistedProfileCount: harvestProfileWorkers.reduce((total, worker) => total + worker.persistedProfileCount, 0),
      unresolvedUrlCount: harvestProfileWorkers.reduce((total, worker) => total + worker.unresolvedUrlCount, 0),
      workers: harvestProfileWorkers.slice(-12),
    },
  };
}

function isTerminalStatus(value) {
  return ["completed", "failed", "cancelled", "superseded"].includes(String(value || "").toLowerCase());
}

function sampleProfileProgressPending(sample) {
  const payload = sample && typeof sample === "object" ? sample : {};
  const progressSources = [
    payload.dashboard?.linkedinStage1Progress,
    payload.progress?.linkedinStage1Progress,
    payload.dashboard?.resultViewLifecycle,
    payload.progress?.resultViewLifecycle,
  ].filter((item) => item && typeof item === "object");
  return progressSources.some((source) => {
    const required = Number(source.profileFetchRequiredCount || source.deltaProfileRequiredCount || 0) || 0;
    const fetched = Number(source.profileFetchedCount || source.deltaProfileFetchedCount || 0) || 0;
    return required > 0 && fetched < required;
  });
}

function sampleMaterializationPending(sample) {
  const payload = sample && typeof sample === "object" ? sample : {};
  const lifecycleSources = [
    payload.dashboard?.resultViewLifecycle,
    payload.progress?.resultViewLifecycle,
  ].filter((item) => item && typeof item === "object");
  const lifecyclePending = lifecycleSources.some((source) => {
    const required = Number(source.deltaProfileRequiredCount || 0) || 0;
    const fetched = Number(source.deltaProfileFetchedCount || 0) || 0;
    const materialized = Number(source.deltaProfileMaterializedCount || 0) || 0;
    return required > 0 && fetched >= required && materialized < required;
  });
  const workerSummary = payload.workers?.harvestProfileBatch || {};
  const workerPending = Number(workerSummary.recoverableCount || 0) > 0;
  return lifecyclePending || workerPending;
}

function terminalSampleIsSettled(sample) {
  const status = String(sample?.jobStatus || "").toLowerCase();
  if (!isTerminalStatus(status)) {
    return false;
  }
  if (["failed", "cancelled", "superseded"].includes(status)) {
    return true;
  }
  return !sampleProfileProgressPending(sample) && !sampleMaterializationPending(sample);
}

function workerNeedsScriptedRecovery(worker) {
  const payload = worker && typeof worker === "object" ? worker : {};
  const metadata = payload.metadata && typeof payload.metadata === "object" ? payload.metadata : {};
  const recoveryKind = String(metadata.recovery_kind || metadata.recoveryKind || "").trim();
  if (!["harvest_company_employees", "harvest_profile_batch"].includes(recoveryKind)) {
    return false;
  }
  const status = String(payload.status || "").trim().toLowerCase();
  const output = payload.output && typeof payload.output === "object" ? payload.output : {};
  const inlineIngest =
    output.inline_incremental_ingest && typeof output.inline_incremental_ingest === "object"
      ? output.inline_incremental_ingest
      : {};
  if (!isTerminalStatus(status)) {
    return true;
  }
  if (status !== "completed") {
    return false;
  }
  return recoveryKind !== "harvest_profile_batch" && !String(inlineIngest.applied_at || "").trim();
}

function remoteCheckpointForWorker(worker) {
  const payload = worker && typeof worker === "object" ? worker : {};
  const checkpoint = payload.checkpoint && typeof payload.checkpoint === "object" ? payload.checkpoint : {};
  const output = payload.output && typeof payload.output === "object" ? payload.output : {};
  const summary = output.summary && typeof output.summary === "object" ? output.summary : {};
  const runId = String(
    checkpoint.run_id || checkpoint.actor_run_id || summary.run_id || summary.actor_run_id || "",
  ).trim();
  const datasetId = String(
    checkpoint.dataset_id ||
      checkpoint.default_dataset_id ||
      summary.dataset_id ||
      summary.default_dataset_id ||
      "",
  ).trim();
  return { runId, datasetId };
}

function workerCanReceiveProviderWebhook(worker) {
  if (!workerNeedsScriptedRecovery(worker)) {
    return false;
  }
  const payload = worker && typeof worker === "object" ? worker : {};
  if (isTerminalStatus(String(payload.status || "").trim().toLowerCase())) {
    return false;
  }
  const checkpoint = remoteCheckpointForWorker(worker);
  if (!checkpoint.runId && !checkpoint.datasetId) {
    return false;
  }
  const rawCheckpoint = payload.checkpoint && typeof payload.checkpoint === "object" ? payload.checkpoint : {};
  const readyEpochMs = Number(rawCheckpoint.scripted_remote_ready_epoch_ms || 0) || 0;
  if (readyEpochMs > 0 && Date.now() < readyEpochMs) {
    return false;
  }
  return true;
}

function workerHasActiveRecoveryLease(worker) {
  const payload = worker && typeof worker === "object" ? worker : {};
  const leaseOwner = String(payload.lease_owner || payload.leaseOwner || "").trim();
  const leaseExpiresAt = String(payload.lease_expires_at || payload.leaseExpiresAt || "").trim();
  if (!leaseOwner || !leaseExpiresAt) {
    return false;
  }
  const normalizedExpiry = leaseExpiresAt.replace(" ", "T");
  const expiresAtMs = Date.parse(
    /(?:Z|[+-]\d\d:?\d\d)$/i.test(normalizedExpiry) ? normalizedExpiry : `${normalizedExpiry}Z`,
  );
  if (!Number.isFinite(expiresAtMs) || expiresAtMs <= Date.now()) {
    return false;
  }
  const checkpoint = payload.checkpoint && typeof payload.checkpoint === "object" ? payload.checkpoint : {};
  const stage = String(checkpoint.stage || "").trim();
  return stage === "waiting_remote_search" || stage === "waiting_remote_harvest";
}

function buildProviderWebhookPayload(
  worker,
  { source = "scripted_browser_provider_webhook", eventSequence = 1 } = {},
) {
  const payload = worker && typeof worker === "object" ? worker : {};
  const { runId, datasetId } = remoteCheckpointForWorker(payload);
  const workerId = Number(payload.worker_id || payload.workerId || 0) || 0;
  const normalizedSequence = Math.max(1, Number(eventSequence || 1) || 1);
  return {
    eventType: "ACTOR.RUN.SUCCEEDED",
    eventData: {
      actorRunId: runId,
      defaultDatasetId: datasetId,
      status: "SUCCEEDED",
      finishedAt: new Date().toISOString(),
    },
    owner_id: `scripted-browser-webhook-${runId || datasetId || workerId || "worker"}-${normalizedSequence}`,
    worker_scan_limit: 500,
    total_limit: 4,
    explicit_job_followup_rounds: 1,
    source,
    worker_id: workerId,
  };
}

function summarizeProviderWebhookEvents(events) {
  const normalizedEvents = Array.isArray(events)
    ? events.filter((event) => event && typeof event === "object")
    : [];
  const statusCounts = {};
  const reasonCounts = {};
  let acceptedCount = 0;
  let failedEventCount = 0;
  let recoveryCount = 0;
  let lateDuplicateCount = 0;
  let inFlightDuplicateCount = 0;
  let maxWebhookToResponseMs = 0;
  for (const event of normalizedEvents) {
    const status = String(event.status || "").trim() || "unknown";
    const reason = String(event.reason || "").trim();
    statusCounts[status] = Number(statusCounts[status] || 0) + 1;
    if (reason) {
      reasonCounts[reason] = Number(reasonCounts[reason] || 0) + 1;
    }
    if (status === "accepted") {
      acceptedCount += 1;
    }
    if (status.endsWith("_failed") || status === "failed" || status === "invalid") {
      failedEventCount += 1;
    }
    if (reason === "matching_remote_provider_workers_not_recoverable") {
      lateDuplicateCount += 1;
    }
    if (reason === "remote_provider_event_recovery_already_in_flight") {
      inFlightDuplicateCount += 1;
    }
    recoveryCount += Math.max(0, Number(event.recoveryCount || 0) || 0);
    maxWebhookToResponseMs = Math.max(
      maxWebhookToResponseMs,
      Math.max(0, Number(event.webhookToResponseMs || 0) || 0),
    );
  }
  return {
    eventCount: normalizedEvents.length,
    acceptedCount,
    failedEventCount,
    recoveryCount,
    lateDuplicateCount,
    inFlightDuplicateCount,
    maxWebhookToResponseMs,
    statusCounts,
    reasonCounts,
  };
}

function createDeltaStreamingObserver(page, options, startedAtMs, getObservedJobId) {
  const samples = [];
  const sharedRecoverySignalEvents = [];
  const providerWebhookEvents = [];
  const maxSamples = 600;
  let lastPollAtMs = 0;
  let lastWorkerRecoveryAtMs = 0;
  let lastJobId = "";
  let baselineBoardObservedInLoop = false;
  let recordInFlight = null;
  let workerRecoveryInFlight = null;
  let providerWebhookInFlight = null;
  const providerWebhookAttemptByWorker = new Map();
  const providerWebhookAcceptedByWorker = new Map();
  const providerWebhookLastAttemptByWorker = new Map();
  let backgroundSamplingStopped = true;
  let backgroundSamplingPromise = null;

  const resolveJobId = async () => {
    const explicitJobId = getObservedJobId() || extractJobIdFromUrl(page.url()) || lastJobId;
    if (explicitJobId) {
      lastJobId = explicitJobId;
      return explicitJobId;
    }
    const historyId = extractHistoryIdFromUrl(page.url());
    if (!historyId || !options.apiBaseUrl) {
      return "";
    }
    try {
      const payload = await fetchApiJson(options.apiBaseUrl, `/api/frontend-history/${encodeURIComponent(historyId)}`);
      const recovery = payload?.recovery && typeof payload.recovery === "object" ? payload.recovery : {};
      const historyJobId = firstString(
        recovery.job_id,
        recovery.jobId,
        payload.job_id,
        payload.jobId,
      );
      if (historyJobId) {
        lastJobId = historyJobId;
      }
      return historyJobId;
    } catch {
      return "";
    }
  };

  const recordOnce = async (label, force = false) => {
    if (!options.observeDeltaStreaming || !options.apiBaseUrl) {
      return null;
    }
    const now = Date.now();
    if (!force && lastPollAtMs > 0 && now - lastPollAtMs < options.deltaObservationPollMs) {
      return samples[samples.length - 1] || null;
    }
    lastPollAtMs = now;
    const jobId = await resolveJobId();
    if (!jobId) {
      return null;
    }
    lastJobId = jobId;
    const timelineTab = page.locator('[data-testid="workflow-step-tab-timeline"]');
    if (await timelineTab.isVisible().catch(() => false)) {
      await timelineTab.click({ noWaitAfter: true }).catch(() => {});
    }
    const executionProcessText = await page.locator(".run-status-card").textContent().catch(() => "");
    const resultsTab = page.locator('[data-testid="workflow-step-tab-results"]');
    if (await resultsTab.isVisible().catch(() => false)) {
      await resultsTab.click({ noWaitAfter: true }).catch(() => {});
    }
    const boardVisible = await page.locator('[data-testid="results-board-panel"]').isVisible().catch(() => false);
    const boardCount = await readVisibleCountState(page).catch(() => ({ raw: "", loadedCount: 0, totalCount: 0 }));
    const candidateSyncText = await page
      .locator(".metric-card")
      .filter({ hasText: "候选人同步" })
      .first()
      .textContent()
      .catch(() => "");
    const candidateSyncCounts = parseCandidateSyncCounts(candidateSyncText);
    let dashboard = null;
    let dashboardError = "";
    let progress = null;
    let progressError = "";
    let workers = null;
    let workersError = "";
    try {
      dashboard = summarizeDashboardPayload(await fetchApiJson(options.apiBaseUrl, `/api/jobs/${jobId}/dashboard`));
    } catch (error) {
      dashboardError = error instanceof Error ? error.message : String(error);
    }
    try {
      progress = summarizeProgressPayload(await fetchApiJson(options.apiBaseUrl, `/api/jobs/${jobId}/progress`));
    } catch (error) {
      progressError = error instanceof Error ? error.message : String(error);
    }
    try {
      workers = summarizeWorkersPayload(await fetchApiJson(options.apiBaseUrl, `/api/jobs/${jobId}/workers`));
    } catch (error) {
      workersError = error instanceof Error ? error.message : String(error);
    }
    const sample = {
      label,
      offsetMs: Math.max(0, now - startedAtMs),
      jobId,
      jobStatus: firstString(dashboard?.jobStatus, progress?.jobStatus),
      jobStage: firstString(dashboard?.jobStage, progress?.jobStage),
      board: {
        visible: boardVisible,
        ...boardCount,
      },
      ui: {
        executionProcessText: String(executionProcessText || "").replace(/\s+/g, " ").trim(),
        candidateSyncText: String(candidateSyncText || "").replace(/\s+/g, " ").trim(),
        candidateSyncLoadedCount: candidateSyncCounts.loaded,
        candidateSyncExpectedCount: candidateSyncCounts.expected,
      },
      dashboard,
      dashboardError,
      progress,
      progressError,
      workers,
      workersError,
    };
    samples.push(sample);
    if (samples.length > maxSamples) {
      samples.shift();
    }
    const expectedBaselineSnapshotId = String(options.expectedBaselineSnapshotId || "").trim();
    const baselineSnapshotMatches = expectedBaselineSnapshotId
      ? sample.dashboard?.snapshotId === expectedBaselineSnapshotId ||
        sample.dashboard?.resultViewLifecycle?.servedSnapshotId === expectedBaselineSnapshotId
      : Boolean(sample.dashboard?.snapshotId || sample.dashboard?.resultViewLifecycle?.servedSnapshotId);
    if (
      sample.board?.visible &&
      Number(sample.board?.loadedCount || 0) > 0 &&
      baselineSnapshotMatches &&
      !isTerminalStatus(sample.jobStatus)
    ) {
      baselineBoardObservedInLoop = true;
    }
    return sample;
  };

  const record = async (label, force = false) => {
    if (recordInFlight) {
      return recordInFlight.catch(() => null);
    }
    recordInFlight = recordOnce(label, force);
    try {
      return await recordInFlight;
    } finally {
      recordInFlight = null;
    }
  };

  const maybeSignalSharedRecovery = async (jobId) => {
    if (
      !options.signalSharedRecovery ||
      options.driveProviderWebhookEvents ||
      !options.apiBaseUrl ||
      !jobId
    ) {
      return;
    }
    if (options.expectedBaselineSnapshotId && !baselineBoardObservedInLoop) {
      return;
    }
    if (workerRecoveryInFlight) {
      return;
    }
    const now = Date.now();
    if (lastWorkerRecoveryAtMs > 0 && now - lastWorkerRecoveryAtMs < options.workerRecoveryPollMs) {
      return;
    }
    lastWorkerRecoveryAtMs = now;
    workerRecoveryInFlight = (async () => {
      let recoverableWorkers = [];
      try {
        const workersPayload = await fetchApiJson(options.apiBaseUrl, `/api/jobs/${jobId}/workers`);
        recoverableWorkers = (Array.isArray(workersPayload?.agent_workers) ? workersPayload.agent_workers : [])
          .filter((worker) => workerNeedsScriptedRecovery(worker));
      } catch (error) {
        sharedRecoverySignalEvents.push({
          offsetMs: Math.max(0, Date.now() - startedAtMs),
          status: "workers_fetch_failed",
          error: error instanceof Error ? error.message : String(error),
        });
        return;
      }
      if (recoverableWorkers.length <= 0) {
        return;
      }
      const workerIds = recoverableWorkers
        .map((worker) => Number(worker.worker_id || worker.workerId || 0))
        .filter((workerId) => Number.isFinite(workerId) && workerId > 0);
      try {
        const signalResponse = await postApiJson(
          options.apiBaseUrl,
          "/api/workers/daemon/run-once",
          {},
        );
        const signal =
          signalResponse?.shared_recovery_signal && typeof signalResponse.shared_recovery_signal === "object"
            ? signalResponse.shared_recovery_signal
            : {};
        let serviceStatus = {};
        try {
          serviceStatus = await fetchApiJson(
            options.apiBaseUrl,
            `/api/workers/daemon/status?job_id=${encodeURIComponent(jobId)}&include_details=1`,
          );
        } catch {
          serviceStatus = {};
        }
        const sharedService =
          serviceStatus?.recovery_services?.shared && typeof serviceStatus.recovery_services.shared === "object"
            ? serviceStatus.recovery_services.shared
            : {};
        sharedRecoverySignalEvents.push({
          offsetMs: Math.max(0, Date.now() - startedAtMs),
          status: String(signalResponse?.status || ""),
          mode: String(signalResponse?.mode || ""),
          signalStatus: String(signal.status || ""),
          signalReason: String(signal.reason || signalResponse?.reason || ""),
          signalServiceName: String(signal.service_name || ""),
          observedSharedServiceStatus: String(sharedService.status || ""),
          observedSharedServiceTick: Number(sharedService.tick || 0) || 0,
          progressObservationSource: "subsequent_progress_workers_and_service_status_polls",
          recoverableWorkerCount: recoverableWorkers.length,
          recoverableWorkerIds: workerIds,
        });
        if (sharedRecoverySignalEvents.length > 80) {
          sharedRecoverySignalEvents.shift();
        }
      } catch (error) {
        sharedRecoverySignalEvents.push({
          offsetMs: Math.max(0, Date.now() - startedAtMs),
          status: "shared_recovery_signal_failed",
          recoverableWorkerCount: recoverableWorkers.length,
          recoverableWorkerIds: workerIds,
          error: error instanceof Error ? error.message : String(error),
        });
      }
    })();
    try {
      await workerRecoveryInFlight;
    } finally {
      workerRecoveryInFlight = null;
    }
  };

  const maybeDriveProviderWebhookEvents = async (jobId) => {
    if (!options.driveProviderWebhookEvents || !options.apiBaseUrl || !jobId) {
      return;
    }
    if (providerWebhookInFlight) {
      return;
    }
    const now = Date.now();
    if (lastWorkerRecoveryAtMs > 0 && now - lastWorkerRecoveryAtMs < options.workerRecoveryPollMs) {
      return;
    }
    lastWorkerRecoveryAtMs = now;
    providerWebhookInFlight = (async () => {
      let webhookWorkers = [];
      try {
        const workersPayload = await fetchApiJson(options.apiBaseUrl, `/api/jobs/${jobId}/workers`);
        webhookWorkers = (Array.isArray(workersPayload?.agent_workers) ? workersPayload.agent_workers : [])
          .filter((worker) => workerCanReceiveProviderWebhook(worker));
      } catch (error) {
        providerWebhookEvents.push({
          offsetMs: Math.max(0, Date.now() - startedAtMs),
          status: "workers_fetch_failed",
          error: error instanceof Error ? error.message : String(error),
        });
        return;
      }
      if (webhookWorkers.length <= 0) {
        return;
      }
      const headers = {};
      const providerWebhookToken = String(options.providerWebhookToken || "").trim();
      if (providerWebhookToken) {
        headers["X-Sourcing-Provider-Webhook-Token"] = providerWebhookToken;
      }
      for (const worker of webhookWorkers.slice(0, 4)) {
        const workerId = Number(worker.worker_id || worker.workerId || 0) || 0;
        const remoteCheckpoint = remoteCheckpointForWorker(worker);
        const webhookKey = [workerId, remoteCheckpoint.runId, remoteCheckpoint.datasetId].join(":");
        if (workerHasActiveRecoveryLease(worker)) {
          providerWebhookEvents.push({
            offsetMs: Math.max(0, Date.now() - startedAtMs),
            status: "provider_webhook_recovery_in_flight",
            workerId,
            runId: remoteCheckpoint.runId,
            datasetId: remoteCheckpoint.datasetId,
          });
          if (providerWebhookEvents.length > 120) {
            providerWebhookEvents.shift();
          }
          continue;
        }
        const lastAttemptAtMs = Number(providerWebhookLastAttemptByWorker.get(webhookKey) || 0) || 0;
        const acceptedAtMs = Number(providerWebhookAcceptedByWorker.get(webhookKey) || 0) || 0;
        const backoffMs = Math.max(5000, options.workerRecoveryPollMs * 5);
        const acceptedBackoffMs = Math.max(30000, options.workerRecoveryPollMs * 15);
        if ((lastAttemptAtMs > 0 && now - lastAttemptAtMs < backoffMs) || (acceptedAtMs > 0 && now - acceptedAtMs < acceptedBackoffMs)) {
          continue;
        }
        providerWebhookLastAttemptByWorker.set(webhookKey, now);
        const eventSequence = Number(providerWebhookAttemptByWorker.get(webhookKey) || 0) + 1;
        providerWebhookAttemptByWorker.set(webhookKey, eventSequence);
        const requestStartedAtMs = Date.now();
        try {
          const webhookResult = await postApiJson(
            options.apiBaseUrl,
            "/api/providers/apify/webhook",
            buildProviderWebhookPayload(worker, { eventSequence }),
            headers,
          );
          providerWebhookEvents.push({
            offsetMs: Math.max(0, Date.now() - startedAtMs),
            status: String(webhookResult?.status || ""),
            reason: String(webhookResult?.reason || ""),
            workerId,
            runId: remoteCheckpoint.runId,
            datasetId: remoteCheckpoint.datasetId,
            eventSequence,
            recoveryCount: Number(webhookResult?.recovery_count || 0) || 0,
            webhookToResponseMs: Math.max(0, Date.now() - requestStartedAtMs),
          });
          if (String(webhookResult?.status || "") === "accepted") {
            providerWebhookAcceptedByWorker.set(webhookKey, Date.now());
          }
        } catch (error) {
          providerWebhookEvents.push({
            offsetMs: Math.max(0, Date.now() - startedAtMs),
            status: "provider_webhook_failed",
            workerId,
            runId: remoteCheckpoint.runId,
            datasetId: remoteCheckpoint.datasetId,
            eventSequence,
            error: error instanceof Error ? error.message : String(error),
          });
        }
        if (providerWebhookEvents.length > 120) {
          providerWebhookEvents.shift();
        }
      }
    })();
    try {
      await providerWebhookInFlight;
    } finally {
      providerWebhookInFlight = null;
    }
  };

  const startBackgroundSampling = () => {
    if (backgroundSamplingPromise) {
      return;
    }
    backgroundSamplingStopped = false;
    backgroundSamplingPromise = (async () => {
      while (!backgroundSamplingStopped) {
        const sample = await record("background_poll").catch(() => null);
        void maybeSignalSharedRecovery(sample?.jobId || lastJobId).catch(() => null);
        void maybeDriveProviderWebhookEvents(sample?.jobId || lastJobId).catch(() => null);
        await page.waitForTimeout(Math.max(100, options.deltaObservationPollMs)).catch(() => null);
      }
    })();
  };

  const stopBackgroundSampling = async () => {
    backgroundSamplingStopped = true;
    const pending = backgroundSamplingPromise;
    backgroundSamplingPromise = null;
    if (pending) {
      await pending.catch(() => null);
    }
    if (workerRecoveryInFlight) {
      await workerRecoveryInFlight.catch(() => null);
    }
    if (providerWebhookInFlight) {
      await providerWebhookInFlight.catch(() => null);
    }
  };

  const waitForTerminal = async (timeoutMs) => {
    const deadline = Date.now() + Math.max(1000, timeoutMs);
    let latest = await record("terminal_wait_start", true);
    while (Date.now() < deadline) {
      latest = await record("terminal_wait", true);
      if (terminalSampleIsSettled(latest)) {
        return latest;
      }
      await page.waitForTimeout(Math.max(100, options.deltaObservationPollMs));
    }
    return latest;
  };

  const summarize = () => {
    const expectedBaselineSnapshotId = String(options.expectedBaselineSnapshotId || "").trim();
    const expectedBaselineMinCount = Math.max(0, Number(options.expectedBaselineMinCount || 0));
    const boardSamples = samples.filter((sample) => sample.board?.visible && Number(sample.board?.loadedCount || 0) > 0);
    const firstBoardSample = boardSamples[0] || null;
    const baselineBoardSamples = boardSamples.filter((sample) => {
      const dashboard = sample.dashboard || {};
      const snapshotMatches = expectedBaselineSnapshotId
        ? dashboard.snapshotId === expectedBaselineSnapshotId ||
          dashboard.resultViewLifecycle?.servedSnapshotId === expectedBaselineSnapshotId
        : Boolean(dashboard.snapshotId || dashboard.resultViewLifecycle?.servedSnapshotId);
      const countMatches = expectedBaselineMinCount
        ? Number(
            dashboard.resultViewLifecycle?.servedCandidateCount ||
              dashboard.candidateCount ||
              sample.board?.totalCount ||
              0,
          ) >= expectedBaselineMinCount
        : true;
      return snapshotMatches && countMatches && !isTerminalStatus(sample.jobStatus);
    });
    const currentBoardSamples = boardSamples.filter((sample) => {
      const snapshotId = String(sample.dashboard?.snapshotId || "");
      return snapshotId && (!expectedBaselineSnapshotId || snapshotId !== expectedBaselineSnapshotId);
    });
    const terminalSamples = samples.filter((sample) => isTerminalStatus(sample.jobStatus));
    const firstDashboardCandidateSample = samples.find(
      (sample) => Number(sample.dashboard?.candidateCount || 0) > 0,
    ) || null;
    const currentSnapshotIds = Array.from(
      new Set(
        currentBoardSamples
          .map((sample) => String(sample.dashboard?.snapshotId || "").trim())
          .filter(Boolean),
      ),
    );
    const baselineFirstBoardObserved = baselineBoardObservedInLoop || baselineBoardSamples.length > 0;
    const finalCurrentSnapshotObserved = currentBoardSamples.some((sample) => isTerminalStatus(sample.jobStatus));
    const executionMetricSamples = samples.filter((sample) => {
      const text = String(sample.ui?.executionProcessText || "");
      return (
        text.includes("新发现在职候选人") ||
        text.includes("需取回 LinkedIn Profile") ||
        text.includes("需补取 LinkedIn Profile")
      );
    });
    const candidateSyncSamples = samples.filter((sample) => String(sample.ui?.candidateSyncText || "").includes("候选人同步"));
    const profileProgressSamples = samples.filter((sample) => {
      const linkedinProgress = sample.dashboard?.linkedinStage1Progress || sample.progress?.linkedinStage1Progress || {};
      return Number(linkedinProgress.profileFetchedCount || 0) > 0;
    });
    const profileProgressCompletedSample = samples.find((sample) => {
      const linkedinProgress = sample.dashboard?.linkedinStage1Progress || sample.progress?.linkedinStage1Progress || {};
      const required = Number(linkedinProgress.profileFetchRequiredCount || 0);
      const fetched = Number(linkedinProgress.profileFetchedCount || 0);
      return required > 0 && fetched >= required;
    }) || null;
    const candidateSyncProfileProgressSamples = samples.filter((sample) => {
      const text = String(sample.ui?.candidateSyncText || "");
      const match = text.match(/LinkedIn(?:\s*Profile)?(?:\s*已取回)?\s*(\d+)\s*\/\s*(\d+)/i);
      return Boolean(match && Number(match[1]) > 0);
    });
    const executionProfileProgressSamples = samples.filter((sample) => {
      const text = String(sample.ui?.executionProcessText || "");
      const match = text.match(/已取回\s*LinkedIn\s*Profile\s*(\d+)/i);
      return Boolean(match && Number(match[1]) > 0);
    });
    const prematurePublicWebStageSamples = samples.filter((sample) => {
      const text = String(sample.ui?.executionProcessText || "");
      if (!text.includes("Public Web Stage 2")) {
        return false;
      }
      const dashboardPhase = sample.dashboard?.executionPhaseContract || {};
      const progressPhase = sample.progress?.executionPhaseContract || {};
      const linkedinProgress = sample.dashboard?.linkedinStage1Progress || sample.progress?.linkedinStage1Progress || {};
      const required = Number(linkedinProgress.profileFetchRequiredCount || 0);
      const fetched = Number(linkedinProgress.profileFetchedCount || 0);
      const profilePending = required > 0 && fetched < required;
      const contractSaysNotPublicWeb =
        dashboardPhase.publicWebStageApplicable === false ||
        progressPhase.publicWebStageApplicable === false ||
        !["", "public_web_stage_2"].includes(String(dashboardPhase.activePhaseId || progressPhase.activePhaseId || ""));
      return profilePending || contractSaysNotPublicWeb;
    });
    const prematureCandidateSyncCompleteSamples = collectBudgetExceededPrematureCandidateSyncSamples(
      samples,
      CANDIDATE_SYNC_STALE_COMPLETE_BUDGET_MS,
    );
    const materializedAheadOfFetchedSamples = samples.filter((sample) => {
      const text = String(sample.ui?.candidateSyncText || "");
      const match = text.match(/已取回\s*(\d+)\s*\/\s*(\d+).*已物化到看板\s*(\d+)\s*\/\s*(\d+)/);
      if (!match) {
        return false;
      }
      return Number(match[3] || 0) > Number(match[1] || 0);
    });
    const impossibleStage1ProgressSamples = samples.filter((sample) => {
      const linkedinProgress = sample.dashboard?.linkedinStage1Progress || sample.progress?.linkedinStage1Progress || {};
      const dedupedCandidateCount = Number(linkedinProgress.dedupedCandidateCount || 0);
      const dedupedProfileUrlCount = Number(linkedinProgress.dedupedProfileUrlCount || 0);
      const required = Number(linkedinProgress.profileFetchRequiredCount || 0);
      const fetched = Number(linkedinProgress.profileFetchedCount || 0);
      const knownDedupe = Math.max(dedupedCandidateCount, dedupedProfileUrlCount);
      return (
        (required > 0 && knownDedupe > 0 && required > knownDedupe) ||
        (fetched > 0 && required > 0 && fetched > required)
      );
    });
    const localMaterializationLabelSamples = samples.filter((sample) => {
      const text = String(sample.ui?.executionProcessText || "");
      return text.includes("本地资产物化") || text.includes("合并到候选人看板");
    });
    const progressCounterRegressions = collectNumericRegressions(samples, {
      "dashboard.linkedin.profile_fetch_required_count": (sample) =>
        sample.dashboard?.linkedinStage1Progress?.profileFetchRequiredCount,
      "dashboard.linkedin.profile_fetched_count": (sample) =>
        sample.dashboard?.linkedinStage1Progress?.profileFetchedCount,
      "dashboard.lifecycle.served_candidate_count": (sample) =>
        sample.dashboard?.resultViewLifecycle?.servedCandidateCount,
      "dashboard.lifecycle.expected_candidate_count": (sample) =>
        sample.dashboard?.resultViewLifecycle?.expectedCandidateCount,
      "dashboard.lifecycle.delta_profile_materialized_count": (sample) =>
        sample.dashboard?.resultViewLifecycle?.deltaProfileMaterializedCount,
      "ui.candidate_sync.loaded_count": (sample) => sample.ui?.candidateSyncLoadedCount,
      "ui.candidate_sync.expected_count": (sample) => sample.ui?.candidateSyncExpectedCount,
    });
    const snapshotDivergenceSamples = samples.filter((sample) => {
      const dashboard = sample.dashboard || {};
      const lifecycle = dashboard.resultViewLifecycle || {};
      const candidateSourceSnapshotId = String(dashboard.candidateSourceSnapshotId || "").trim();
      const resultViewSnapshotId = String(dashboard.resultViewSnapshotId || "").trim();
      const servedSnapshotId = String(lifecycle.servedSnapshotId || "").trim();
      const snapshotIds = [candidateSourceSnapshotId, resultViewSnapshotId, servedSnapshotId].filter(Boolean);
      return new Set(snapshotIds).size > 1;
    });
    const materializationStuckSamples = collectBudgetExceededMaterializationStuckSamples(
      samples,
      MATERIALIZATION_STUCK_AFTER_FETCH_BUDGET_MS,
    );
    const boardRuntimeSamples = samples.filter((sample) => {
      const dashboardBoard = sample.dashboard?.boardRuntimeState || {};
      const progressBoard = sample.progress?.boardRuntimeState || {};
      return (
        Number(dashboardBoard.expectedCandidateCount || 0) > 0 ||
        Number(progressBoard.expectedCandidateCount || 0) > 0
      );
    });
    const boardRuntimePreviewShellSyncLeakSamples = samples.filter((sample) => {
      const board = sample.dashboard?.boardRuntimeState || sample.progress?.boardRuntimeState || {};
      const expected = Number(board.expectedCandidateCount || 0);
      const preview = Number(board.previewCandidateCount || 0);
      const displayReady = Number(board.displayReadyCandidateCount || 0);
      const synced = Number(sample.ui?.candidateSyncLoadedCount || 0);
      const qualityFieldsAvailable =
        Boolean(board.cardMaterializationQualityFieldsAvailable) ||
        Number(board.profileDetailCandidateCount || 0) > 0 ||
        Number(board.explicitProfileCaptureCandidateCount || 0) > 0 ||
        preview > 0;
      return expected > 0 && qualityFieldsAvailable && preview > 0 && synced > displayReady;
    });
    const boardRuntimeFilterContractSamples = boardRuntimeSamples.filter((sample) => {
      const contract = sample.dashboard?.boardRuntimeState?.filterContract || sample.progress?.boardRuntimeState?.filterContract || {};
      return Boolean(String(contract.source || contract.rowFilterScope || contract.facetCountScope || "").trim());
    });
    return {
      attempted: true,
      apiBaseUrl: options.apiBaseUrl,
      workerRecoveryDriven: false,
      sharedRecoverySignalDriven: Boolean(options.signalSharedRecovery),
      providerWebhookDriven: Boolean(options.driveProviderWebhookEvents),
      jobId: lastJobId,
      expectedBaselineSnapshotId,
      expectedBaselineMinCount,
      sampleCount: samples.length,
      baselineFirstBoardObserved,
      finalCurrentSnapshotObserved,
      contractReady: baselineFirstBoardObserved && finalCurrentSnapshotObserved,
      contractGapDetected: !baselineFirstBoardObserved || !finalCurrentSnapshotObserved,
      firstBoardSample,
      firstDashboardCandidateSample,
      firstTerminalSample: terminalSamples[0] || null,
      observedCurrentSnapshotIds: currentSnapshotIds,
      executionMetricsObserved: executionMetricSamples.length > 0,
      candidateSyncObserved: candidateSyncSamples.length > 0,
      profileProgressObserved: profileProgressSamples.length > 0,
      profileProgressCompleted: Boolean(profileProgressCompletedSample),
      candidateSyncProfileProgressObserved: candidateSyncProfileProgressSamples.length > 0,
      executionProfileProgressObserved: executionProfileProgressSamples.length > 0,
      prematurePublicWebStageDetected: prematurePublicWebStageSamples.length > 0,
      prematurePublicWebStageSampleCount: prematurePublicWebStageSamples.length,
      firstPrematurePublicWebStageSample: prematurePublicWebStageSamples[0] || null,
      prematureCandidateSyncCompleteDetected: prematureCandidateSyncCompleteSamples.length > 0,
      prematureCandidateSyncCompleteSampleCount: prematureCandidateSyncCompleteSamples.length,
      firstPrematureCandidateSyncCompleteSample: prematureCandidateSyncCompleteSamples[0] || null,
      materializedAheadOfFetchedDetected: materializedAheadOfFetchedSamples.length > 0,
      materializedAheadOfFetchedSampleCount: materializedAheadOfFetchedSamples.length,
      firstMaterializedAheadOfFetchedSample: materializedAheadOfFetchedSamples[0] || null,
      impossibleStage1ProgressDetected: impossibleStage1ProgressSamples.length > 0,
      impossibleStage1ProgressSampleCount: impossibleStage1ProgressSamples.length,
      firstImpossibleStage1ProgressSample: impossibleStage1ProgressSamples[0] || null,
      localMaterializationLabelObserved: localMaterializationLabelSamples.length > 0,
      firstLocalMaterializationLabelSample: localMaterializationLabelSamples[0] || null,
      progressRegressionDetected: Object.keys(progressCounterRegressions).length > 0,
      progressCounterRegressions,
      snapshotDivergenceDetected: snapshotDivergenceSamples.length > 0,
      snapshotDivergenceSampleCount: snapshotDivergenceSamples.length,
      firstSnapshotDivergenceSample: snapshotDivergenceSamples[0] || null,
      materializationStuckAfterFetchDetected: materializationStuckSamples.length > 0,
      materializationStuckAfterFetchSampleCount: materializationStuckSamples.length,
      materializationStuckAfterFetchBudgetMs: MATERIALIZATION_STUCK_AFTER_FETCH_BUDGET_MS,
      firstMaterializationStuckAfterFetchSample: materializationStuckSamples[0] || null,
      boardRuntimeObserved: boardRuntimeSamples.length > 0,
      boardRuntimeSampleCount: boardRuntimeSamples.length,
      firstBoardRuntimeSample: boardRuntimeSamples[0] || null,
      boardRuntimeFilterContractObserved: boardRuntimeFilterContractSamples.length > 0,
      boardRuntimePreviewShellSyncLeakDetected: boardRuntimePreviewShellSyncLeakSamples.length > 0,
      boardRuntimePreviewShellSyncLeakSampleCount: boardRuntimePreviewShellSyncLeakSamples.length,
      firstBoardRuntimePreviewShellSyncLeakSample: boardRuntimePreviewShellSyncLeakSamples[0] || null,
      firstExecutionMetricSample: executionMetricSamples[0] || null,
      firstCandidateSyncSample: candidateSyncSamples[0] || null,
      firstProfileProgressSample: profileProgressSamples[0] || null,
      profileProgressCompletedSample,
      firstCandidateSyncProfileProgressSample: candidateSyncProfileProgressSamples[0] || null,
      firstExecutionProfileProgressSample: executionProfileProgressSamples[0] || null,
      sharedRecoverySignalEvents,
      providerWebhookEvents,
      providerWebhookSummary: summarizeProviderWebhookEvents(providerWebhookEvents),
      samples,
    };
  };

  return {
    record,
    startBackgroundSampling,
    stopBackgroundSampling,
    waitForTerminal,
    summarize,
  };
}

async function captureTimelineAndResultsState(page, state, startedAtMs) {
  const timelineTab = page.locator('[data-testid="workflow-step-tab-timeline"]');
  if (await timelineTab.isVisible().catch(() => false)) {
    await timelineTab.click({ noWaitAfter: true }).catch(() => {});
    const finalResultsCompleted = page.locator(".timeline-step.timeline-completed").filter({ hasText: "Final Results" });
    if (!state.finalResultsCompletedAtMs && (await finalResultsCompleted.count().catch(() => 0)) > 0) {
      state.finalResultsCompletedAtMs = Date.now();
    }
  }

  const resultsTab = page.locator('[data-testid="workflow-step-tab-results"]');
  if (await resultsTab.isVisible().catch(() => false)) {
    await resultsTab.click({ noWaitAfter: true }).catch(() => {});
  }

  const resultsLoadingCard = page.locator('[data-testid="results-loading-card"]');
  if (await resultsLoadingCard.isVisible().catch(() => false)) {
    state.sawResultsLoadingCard = true;
    if (!state.resultsSurfaceVisibleAtMs) {
      state.resultsSurfaceVisibleAtMs = Date.now();
    }
  }

  const resultsBoardPanel = page.locator('[data-testid="results-board-panel"]');
  if (await resultsBoardPanel.isVisible().catch(() => false)) {
    if (!state.resultsSurfaceVisibleAtMs) {
      state.resultsSurfaceVisibleAtMs = Date.now();
    }
    const countText = await page.locator('[data-testid="results-visible-count"]').textContent().catch(() => "");
    const visibleCardCount = await page.locator('[data-testid="results-candidate-card"]').count().catch(() => 0);
    if (visibleCardCount > 0) {
      if (!state.candidateResultsReadyAtMs) {
        state.candidateResultsReadyAtMs = Date.now();
      }
      return {
        ready: true,
        countText: String(countText || "").trim(),
        visibleCardCount,
        lastSnapshot: `${String(countText || "").trim()} cards=${visibleCardCount}`,
      };
    }
    const progressiveEmptyStateText = await resultsBoardPanel.locator(".empty-state").textContent().catch(() => "");
    if (
      String(progressiveEmptyStateText || "").includes("不是最终空结果") ||
      String(progressiveEmptyStateText || "").includes("继续装载")
    ) {
      state.sawResultsLoadingCard = true;
    }
    return {
      ready: false,
      countText: String(countText || "").trim(),
      visibleCardCount,
      lastSnapshot: `${String(countText || "").trim()} cards=${visibleCardCount}`,
    };
  }

  return {
    ready: false,
    countText: "",
    visibleCardCount: 0,
    lastSnapshot: "",
  };
}

async function ensureResultsBoardTabSelected(page) {
  const resultsTab = page.locator('[data-testid="workflow-step-tab-results"]');
  if (!(await resultsTab.isVisible().catch(() => false))) {
    return;
  }
  await resultsTab.click({ noWaitAfter: true }).catch(() => {});
  await page.waitForTimeout(200).catch(() => {});
}

async function ensureWorkflowTimelineTabSelected(page) {
  const timelineTab = page.locator('[data-testid="workflow-step-tab-timeline"]');
  if (!(await timelineTab.isVisible().catch(() => false))) {
    return;
  }
  await timelineTab.click({ noWaitAfter: true }).catch(() => {});
  await page.waitForTimeout(200).catch(() => {});
}

async function collectWorkflowTabSnapshot(page) {
  const activeTab = await page.locator('[role="tab"][aria-selected="true"] strong').textContent().catch(() => "");
  const timelineTabVisible = await page.locator('[data-testid="workflow-step-tab-timeline"]').isVisible().catch(() => false);
  const resultsTabVisible = await page.locator('[data-testid="workflow-step-tab-results"]').isVisible().catch(() => false);
  const reviewTabVisible = await page.locator('[data-testid="workflow-step-tab-review"]').isVisible().catch(() => false);
  const continueStage2Visible = await page
    .getByRole("button", { name: /继续执行 Stage 2/ })
    .isVisible()
    .catch(() => false);
  const resultsBoardVisible = await page
    .locator('[data-testid="results-board-panel"]')
    .isVisible()
    .catch(() => false);
  return {
    activeTab: String(activeTab || "").trim(),
    timelineTabVisible,
    resultsTabVisible,
    reviewTabVisible,
    continueStage2Visible,
    resultsBoardVisible,
  };
}

async function waitForCandidateResults(page, timeoutMs, deltaStreamingObserver = null) {
  const deadline = Date.now() + timeoutMs;
  const startedAtMs = Date.now();
  const state = {
    resultsSurfaceVisibleAtMs: 0,
    finalResultsCompletedAtMs: 0,
    candidateResultsReadyAtMs: 0,
    sawResultsLoadingCard: false,
  };
  let lastSnapshot = "";
  const debugBrowserE2E = process.env.SOURCING_DEBUG_FRONTEND_BROWSER_E2E === "1";
  let iteration = 0;
  await ensureWorkflowTimelineTabSelected(page);
  const initialContinueStage2Button = page.getByRole("button", { name: /继续执行 Stage 2/ });
  if (await initialContinueStage2Button.isVisible().catch(() => false)) {
    await initialContinueStage2Button.click().catch(() => {});
    await page.waitForTimeout(500).catch(() => {});
  }
  await ensureResultsBoardTabSelected(page);
  while (Date.now() < deadline) {
    const continueStage2Button = page.getByRole("button", { name: /继续执行 Stage 2/ });
    if (await continueStage2Button.isVisible().catch(() => false)) {
      await continueStage2Button.click();
      await page.waitForTimeout(500);
    }

    if (deltaStreamingObserver) {
      await deltaStreamingObserver.record("candidate_result_wait").catch(() => null);
    }
    const observation = await captureTimelineAndResultsState(page, state, startedAtMs);
    if (observation.lastSnapshot) {
      lastSnapshot = observation.lastSnapshot;
    }
    if (debugBrowserE2E && (iteration % 10 === 0 || observation.ready)) {
      process.stderr.write(
        `[browser-e2e] iter=${iteration} ready=${observation.ready} active=${JSON.stringify(await collectWorkflowTabSnapshot(page).catch(() => ({})))} count=${JSON.stringify({
          text: observation.countText || "",
          cards: observation.visibleCardCount || 0,
        })}\n`,
      );
    }
    iteration += 1;
    if (observation.ready) {
      if (deltaStreamingObserver) {
        await deltaStreamingObserver.record("candidate_result_ready", true).catch(() => null);
      }
      return {
        sawResultsLoadingCard: state.sawResultsLoadingCard,
        timingsMs: {
          resultsSurfaceVisibleOffsetMs:
            state.resultsSurfaceVisibleAtMs > 0 ? Math.max(0, state.resultsSurfaceVisibleAtMs - startedAtMs) : null,
          finalResultsCompletedOffsetMs:
            state.finalResultsCompletedAtMs > 0 ? Math.max(0, state.finalResultsCompletedAtMs - startedAtMs) : null,
          candidateResultsReadyOffsetMs:
            state.candidateResultsReadyAtMs > 0 ? Math.max(0, state.candidateResultsReadyAtMs - startedAtMs) : null,
          finalResultsToCandidateBoardMs:
            state.finalResultsCompletedAtMs > 0 && state.candidateResultsReadyAtMs > 0
              ? Math.max(0, state.candidateResultsReadyAtMs - state.finalResultsCompletedAtMs)
              : null,
        },
      };
    }

    const errorCardText = await page.locator(".error-card").textContent().catch(() => "");
    if (errorCardText?.trim()) {
      lastSnapshot = errorCardText.trim();
    }

    await page.waitForTimeout(200);
  }
  const snapshot = await collectWorkflowTabSnapshot(page).catch(() => ({}));
  throw new Error(
    `Timed out waiting for candidate results. last_state=${lastSnapshot} snapshot=${JSON.stringify(snapshot)}`,
  );
}

async function collectCandidatePreview(page) {
  const cards = page.locator('[data-testid="results-candidate-card"]');
  const count = await cards.count();
  const previewNames = [];
  for (let index = 0; index < Math.min(count, 3); index += 1) {
    const name = await cards.nth(index).locator("h4").textContent();
    if (name?.trim()) {
      previewNames.push(name.trim());
    }
  }
  return {
    count,
    previewNames,
  };
}

async function readVisibleCountState(page) {
  const text = await page.locator('[data-testid="results-visible-count"]').textContent().catch(() => "");
  const match = String(text || "").match(/(\d+)\s*\/\s*(\d+)/);
  return {
    raw: String(text || "").trim(),
    loadedCount: match ? Number(match[1]) : 0,
    totalCount: match ? Number(match[2]) : 0,
  };
}

async function readPagerState(page) {
  const text = await page.locator(".results-pager .muted").textContent().catch(() => "");
  const match = String(text || "").match(/第\s*(\d+)\s*\/\s*(\d+)\s*页/);
  return {
    raw: String(text || "").trim(),
    currentPage: match ? Number(match[1]) : 1,
    totalPages: match ? Number(match[2]) : 1,
  };
}

async function waitForLoadedCountIncrease(page, baselineLoadedCount, timeoutMs) {
  const deadline = Date.now() + timeoutMs;
  let lastState = await readVisibleCountState(page);
  while (Date.now() < deadline) {
    lastState = await readVisibleCountState(page);
    if (lastState.loadedCount > baselineLoadedCount) {
      return {
        observed: true,
        state: lastState,
      };
    }
    await page.waitForTimeout(500);
  }
  return {
    observed: false,
    state: lastState,
  };
}

async function verifyPaginationStability(page, options, jobId) {
  const resultsTab = page.locator('[data-testid="workflow-step-tab-results"]');
  if (await resultsTab.isVisible().catch(() => false)) {
    await ensureResultsBoardTabSelected(page);
  }
  await page.locator('[data-testid="results-board-panel"]').waitFor({ state: "visible", timeout: options.timeoutMs });
  const candidateCountState = await readVisibleCountState(page);
  if (candidateCountState.totalCount <= 24) {
    return {
      attempted: false,
      skipReason: "single_page_results",
      initialVisibleCount: candidateCountState,
    };
  }
  const pagerState = await readPagerState(page);
  if (pagerState.totalPages < options.paginationTargetPage) {
    return {
      attempted: false,
      skipReason: "insufficient_pages",
      initialVisibleCount: candidateCountState,
      initialPagerState: pagerState,
    };
  }

  const nextPageButton = page.getByRole("button", { name: "下一页" });
  const targetOffset = Math.max(0, (options.paginationTargetPage - 1) * RESULTS_PAGE_SIZE);
  const paginationRequestDelayMs = Math.max(500, Math.min(options.paginationHydrationTimeoutMs, 1500));
  const routeMatcher = "**/api/jobs/*/candidates*";
  let paginationRequestIntercepted = false;
  let resolvePaginationRequestSeen = () => {};
  const paginationRequestSeen = new Promise((resolve) => {
    resolvePaginationRequestSeen = resolve;
  });
  const paginationRouteHandler = async (route) => {
    const requestUrl = route.request().url();
    let parsedUrl;
    try {
      parsedUrl = new URL(requestUrl);
    } catch {
      await route.continue().catch(() => {});
      return;
    }
    const pathMatches = jobId
      ? parsedUrl.pathname === `/api/jobs/${jobId}/candidates`
      : /\/api\/jobs\/[^/]+\/candidates$/.test(parsedUrl.pathname);
    const offsetMatches = Number(parsedUrl.searchParams.get("offset") || 0) === targetOffset;
    if (paginationRequestIntercepted || !pathMatches || !offsetMatches) {
      await route.continue().catch(() => {});
      return;
    }
    paginationRequestIntercepted = true;
    resolvePaginationRequestSeen();
    await page.waitForTimeout(paginationRequestDelayMs).catch(() => {});
    await route.continue().catch(() => {});
  };
  await page.route(routeMatcher, paginationRouteHandler);
  while (true) {
    const currentPagerState = await readPagerState(page);
    if (currentPagerState.currentPage >= options.paginationTargetPage) {
      break;
    }
    await nextPageButton.click();
    await page.waitForTimeout(300);
  }

  const requestIntercepted = await Promise.race([
    paginationRequestSeen.then(() => true),
    page.waitForTimeout(options.paginationHydrationTimeoutMs).then(() => false),
  ]);
  if (!requestIntercepted) {
    await page.unroute(routeMatcher, paginationRouteHandler).catch(() => {});
    throw new Error("pagination stability test never observed the backend page request");
  }
  const beforePreview = await collectCandidatePreview(page);
  const beforePagerState = await readPagerState(page);
  const beforeVisibleCount = await readVisibleCountState(page);
  const hydrationObservation = await waitForLoadedCountIncrease(
    page,
    beforeVisibleCount.loadedCount,
    options.paginationHydrationTimeoutMs,
  );
  await page.waitForTimeout(500);
  const afterPreview = await collectCandidatePreview(page);
  const afterPagerState = await readPagerState(page);
  const afterVisibleCount = await readVisibleCountState(page);
  await page.unroute(routeMatcher, paginationRouteHandler).catch(() => {});
  return {
    attempted: true,
    targetPage: options.paginationTargetPage,
    loadedCountIncreased: hydrationObservation.observed,
    stableCurrentPage:
      beforePagerState.currentPage === options.paginationTargetPage
      && afterPagerState.currentPage === options.paginationTargetPage,
    stablePreviewNames:
      JSON.stringify(beforePreview.previewNames) === JSON.stringify(afterPreview.previewNames),
    before: {
      pager: beforePagerState,
      visibleCount: beforeVisibleCount,
      previewNames: beforePreview.previewNames,
    },
    after: {
      pager: afterPagerState,
      visibleCount: afterVisibleCount,
      previewNames: afterPreview.previewNames,
    },
  };
}

async function collectEndpointParity(page, jobId) {
  const normalizedJobId = String(jobId || "").trim();
  if (!normalizedJobId) {
    return {
      attempted: false,
      consistent: false,
      reason: "missing_job_id",
    };
  }
  const [progressPayload, dashboardPayload, candidatesPayload, boardPatchesPayload] = await Promise.all([
    fetchBrowserJson(page, `/api/jobs/${normalizedJobId}/progress`),
    fetchBrowserJson(page, `/api/jobs/${normalizedJobId}/dashboard`),
    fetchBrowserJson(page, `/api/jobs/${normalizedJobId}/candidates`),
    fetchBrowserJson(page, `/api/jobs/${normalizedJobId}/board-patches`),
  ]);
  const rawBoardRuntimeStates = {
    progress: progressPayload?.board_runtime_state || progressPayload?.boardRuntimeState || {},
    dashboard: dashboardPayload?.board_runtime_state || dashboardPayload?.boardRuntimeState || {},
    candidates: candidatesPayload?.board_runtime_state || candidatesPayload?.boardRuntimeState || {},
    boardPatches: boardPatchesPayload?.board_runtime_state || boardPatchesPayload?.boardRuntimeState || {},
  };
  const summaries = {
    progress: summarizeComparableBoardRuntimeState(rawBoardRuntimeStates.progress),
    dashboard: summarizeComparableBoardRuntimeState(rawBoardRuntimeStates.dashboard),
    candidates: summarizeComparableBoardRuntimeState(rawBoardRuntimeStates.candidates),
    boardPatches: summarizeComparableBoardRuntimeState(rawBoardRuntimeStates.boardPatches),
  };
  const reference = summaries.dashboard;
  const comparableFields = [
    "jobId",
    "expectedCandidateCount",
    "servedCandidateCount",
    "publishedCandidateCount",
    "displayReadyCandidateCount",
    "phase",
    "layeringStatus",
    "deltaProfileDenominatorPromoted",
    "cardMaterializationStatusText",
    "profileFetchStatusText",
    "rowPublicationSequence",
    "rowPublicationWatermark",
    "filterContract",
  ];
  const missingBoardRuntimeStateSources = Object.entries(rawBoardRuntimeStates)
    .filter(([, value]) => !value || Object.keys(value).length === 0)
    .map(([name]) => `missing_board_runtime_state:${name}`);
  const mismatchedFields = [
    ...missingBoardRuntimeStateSources,
    ...comparableFields.filter((field) => {
    const expected = reference[field];
    if (field === "filterContract") {
      return Object.values(summaries).some((summary) => JSON.stringify(summary[field]) !== JSON.stringify(expected));
    }
    return Object.values(summaries).some((summary) => summary[field] !== expected);
    }),
  ];
  return {
    attempted: true,
    consistent: mismatchedFields.length === 0,
    mismatchedFields,
    summaries,
  };
}

async function collectTimelineDurations(page) {
  const timelineTab = page.locator('[data-testid="workflow-step-tab-timeline"]');
  if (await timelineTab.isVisible().catch(() => false)) {
    await timelineTab.click({ noWaitAfter: true }).catch(() => {});
  }
  const steps = page.locator(".timeline-step");
  if ((await steps.count().catch(() => 0)) <= 0) {
    return {};
  }
  const timelineEntries = await steps.evaluateAll((nodes) =>
    nodes.map((node) => {
      const title = node.querySelector(".timeline-copy strong")?.textContent || "";
      const duration = node.querySelector("em")?.textContent || "";
      return {
        title: String(title || "").trim(),
        duration: String(duration || "").trim(),
      };
    }),
  );
  return timelineEntries.reduce((accumulator, entry) => {
    if (entry.title) {
      accumulator[entry.title] = entry.duration;
    }
    return accumulator;
  }, {});
}

async function main() {
  const options = parseArgs(process.argv.slice(2));
  await fs.mkdir(path.dirname(options.screenshotPath), { recursive: true });

  const browser = await chromium.launch({ headless: true });
  const context = await browser.newContext({
    viewport: { width: 1440, height: 1100 },
    locale: "zh-CN",
    timezoneId: "Asia/Shanghai",
  });
  await context.addInitScript(() => {
    window.localStorage.clear();
    window.sessionStorage.clear();
  });
  const page = await context.newPage();
  let observedJobId = "";
  const workflowStartedAtMs = Date.now();
  const networkEvents = [];
  const recordNetworkEvent = (event) => {
    if (String(event.url || "").includes("/candidates")) {
      return;
    }
    networkEvents.push({
      offsetMs: Math.max(0, Date.now() - workflowStartedAtMs),
      ...event,
    });
    if (networkEvents.length > 240) {
      networkEvents.shift();
    }
  };
  page.on("request", (request) => {
    const url = request.url();
    if (!url.includes("/api/plan/review") && !url.includes("/api/workflows") && !url.includes("/api/jobs/")) {
      return;
    }
    recordNetworkEvent({
      type: "request",
      method: request.method(),
      url,
    });
  });
  page.on("response", (response) => {
    const url = response.url();
    if (url.includes("/api/plan/review") || url.includes("/api/workflows") || url.includes("/api/jobs/")) {
      recordNetworkEvent({
        type: "response",
        method: response.request().method(),
        status: response.status(),
        url,
      });
    }
    if (!url.includes("/api/workflows") || response.request().method().toUpperCase() !== "POST") {
      return;
    }
    void response
      .json()
      .then((payload) => {
        observedJobId = firstString(payload?.job_id, payload?.jobId, payload?.job?.job_id, observedJobId);
      })
      .catch(() => {});
  });

  let deltaStreamingObserver = null;
  try {
    deltaStreamingObserver = options.observeDeltaStreaming
      ? createDeltaStreamingObserver(page, options, workflowStartedAtMs, () => observedJobId)
      : null;
    if (deltaStreamingObserver) {
      deltaStreamingObserver.startBackgroundSampling();
    }
    await page.goto(options.startUrl || options.frontendUrl, { waitUntil: "domcontentloaded", timeout: options.timeoutMs });
    let planFields = {};
    let planMetadata = {};
    if (!options.restoreExistingResults) {
      await page.waitForSelector('[data-testid="search-composer-input"]', { timeout: options.timeoutMs });
      await page.locator('[data-testid="search-composer-input"]').fill(options.query);
      await page.locator('[data-testid="search-composer-submit"]').click();

      const planCard = page.locator('[data-testid="plan-card"]');
      await planCard.waitFor({ state: "visible", timeout: options.timeoutMs });
      planFields = await extractPlanFields(planCard);
      planMetadata = await extractPlanMetadata(planCard);

      await page.locator('[data-testid="plan-confirm-button"]').click({ noWaitAfter: true });
    }
    observedJobId = firstString(observedJobId, extractJobIdFromUrl(page.url()));
    const resultWait = await waitForCandidateResults(page, options.timeoutMs, deltaStreamingObserver);
    if (deltaStreamingObserver) {
      await deltaStreamingObserver.waitForTerminal(options.timeoutMs).catch(() => null);
    }
    const historyUrl = page.url();
    observedJobId = firstString(observedJobId, extractJobIdFromUrl(historyUrl));
    const initialResults = await collectCandidatePreview(page);
    const pagination =
      options.checkPaginationStability
        ? await verifyPaginationStability(page, options, observedJobId || extractJobIdFromUrl(page.url()))
        : {
            attempted: false,
            skipReason: "disabled",
          };
    const openLinkedinActionCount = await page
      .evaluate(() => {
        const panel = document.querySelector('[data-testid="results-board-panel"]');
        if (!panel) {
          return 0;
        }
        return Array.from(panel.querySelectorAll("a")).filter((node) =>
          String(node.textContent || "").includes("打开 LinkedIn"),
        ).length;
      })
      .catch(() => 0);
    const initialTimelineDurations = await collectTimelineDurations(page);

    await page.goto(historyUrl, { waitUntil: "domcontentloaded", timeout: options.timeoutMs });
    const reloadedWait = await waitForCandidateResults(page, options.timeoutMs, deltaStreamingObserver);
    const reloadedResults = await collectCandidatePreview(page);
    const reloadedTimelineDurations = await collectTimelineDurations(page);
    if (deltaStreamingObserver) {
      await deltaStreamingObserver.stopBackgroundSampling();
    }
    const endpointParity = await collectEndpointParity(page, firstString(observedJobId, extractJobIdFromUrl(historyUrl))).catch((error) => ({
      attempted: true,
      consistent: false,
      error: error instanceof Error ? error.message : String(error),
    }));

    await page.locator('[data-testid="workflow-step-tab-review"]').click();
    await page.waitForSelector('[data-testid="manual-review-panel"]', { timeout: options.timeoutMs });
    const manualReviewError = await page.locator(".manual-review-panel .error-card").textContent().catch(() => "");
    if (manualReviewError?.includes("Local backend is unreachable")) {
      throw new Error(manualReviewError.trim());
    }
    const hasReviewCards = (await page.locator('[data-testid="manual-review-card-grid"] article').count()) > 0;
    const hasEmptyState = await page.locator('[data-testid="manual-review-empty-state"]').isVisible().catch(() => false);

    try {
      await page.screenshot({ path: options.screenshotPath, fullPage: true, timeout: 0 });
    } catch {
      await page.screenshot({ path: options.screenshotPath, fullPage: false, timeout: 0 });
    }

    const summary = {
      status: "ok",
      frontendUrl: options.frontendUrl,
      historyUrl,
      query: options.query,
      plan: {
        targetCompany: planFields["目标公司"] || "",
        targetPopulation: planFields["目标人群"] || "",
        projectScope: planFields["项目范围"] || "",
        keywords: planFields["检索关键词"] || "",
        strategy: planFields["检索策略"] || "",
        metadata: planMetadata,
      },
      results: {
        initialCount: initialResults.count,
        reloadedCount: reloadedResults.count,
        previewNames: initialResults.previewNames,
        reloadedPreviewNames: reloadedResults.previewNames,
        openLinkedinActionCount,
        sawInitialResultsLoadingCard: resultWait.sawResultsLoadingCard,
        sawReloadedResultsLoadingCard: reloadedWait.sawResultsLoadingCard,
        sawResultsLoadingCard: resultWait.sawResultsLoadingCard || reloadedWait.sawResultsLoadingCard,
        timingsMs: resultWait.timingsMs,
        reloadedTimingsMs: reloadedWait.timingsMs,
        timelineDurations: initialTimelineDurations,
        reloadedTimelineDurations: reloadedTimelineDurations,
      },
      manualReview: {
        hasReviewCards,
        hasEmptyState,
      },
      networkEvents,
      pagination,
      endpointParity,
      deltaStreaming: deltaStreamingObserver
        ? deltaStreamingObserver.summarize()
        : {
            attempted: false,
          },
      screenshotPath: options.screenshotPath,
    };
    process.stdout.write(`${JSON.stringify(summary, null, 2)}\n`);
  } finally {
    // The observer runs independently so it can catch baseline-serving states
    // before the main UI assertions start waiting for final results.
    if (deltaStreamingObserver) {
      await deltaStreamingObserver.stopBackgroundSampling();
    }
    await context.close();
    await browser.close();
  }
}

main().catch((error) => {
  process.stderr.write(`${error instanceof Error ? error.stack || error.message : String(error)}\n`);
  process.exit(1);
});
