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

async function captureTimelineAndResultsState(page, state, startedAtMs) {
  const timelineTab = page.locator('[data-testid="workflow-step-tab-timeline"]');
  if (await timelineTab.isVisible().catch(() => false)) {
    await timelineTab.click().catch(() => {});
    const finalResultsCompleted = page.locator(".timeline-step.timeline-completed").filter({ hasText: "Final Results" });
    if (!state.finalResultsCompletedAtMs && (await finalResultsCompleted.count().catch(() => 0)) > 0) {
      state.finalResultsCompletedAtMs = Date.now();
    }
  }

  const resultsTab = page.locator('[data-testid="workflow-step-tab-results"]');
  if (await resultsTab.isVisible().catch(() => false)) {
    await resultsTab.click().catch(() => {});
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
    if (Number.parseInt(countText || "", 10) > 0) {
      if (!state.candidateResultsReadyAtMs) {
        state.candidateResultsReadyAtMs = Date.now();
      }
      return {
        ready: true,
        lastSnapshot: String(countText || "").trim(),
      };
    }
    return {
      ready: false,
      lastSnapshot: String(countText || "").trim(),
    };
  }

  return {
    ready: false,
    lastSnapshot: "",
  };
}

async function waitForCandidateResults(page, timeoutMs) {
  const deadline = Date.now() + timeoutMs;
  const startedAtMs = Date.now();
  const state = {
    resultsSurfaceVisibleAtMs: 0,
    finalResultsCompletedAtMs: 0,
    candidateResultsReadyAtMs: 0,
    sawResultsLoadingCard: false,
  };
  let lastSnapshot = "";
  while (Date.now() < deadline) {
    const continueStage2Button = page.getByRole("button", { name: /继续执行 Stage 2/ });
    if (await continueStage2Button.isVisible().catch(() => false)) {
      await continueStage2Button.click();
      await page.waitForTimeout(500);
    }

    const observation = await captureTimelineAndResultsState(page, state, startedAtMs);
    if (observation.lastSnapshot) {
      lastSnapshot = observation.lastSnapshot;
    }
    if (observation.ready) {
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
  throw new Error(`Timed out waiting for candidate results. last_state=${lastSnapshot}`);
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

async function verifyPaginationStability(page, options) {
  const resultsTab = page.locator('[data-testid="workflow-step-tab-results"]');
  if (await resultsTab.isVisible().catch(() => false)) {
    await resultsTab.click().catch(() => {});
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
  while (true) {
    const currentPagerState = await readPagerState(page);
    if (currentPagerState.currentPage >= options.paginationTargetPage) {
      break;
    }
    await nextPageButton.click();
    await page.waitForTimeout(300);
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

async function collectTimelineDurations(page) {
  const timelineTab = page.locator('[data-testid="workflow-step-tab-timeline"]');
  if (await timelineTab.isVisible().catch(() => false)) {
    await timelineTab.click().catch(() => {});
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

  try {
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

      await page.locator('[data-testid="plan-confirm-button"]').click();
    }
    const resultWait = await waitForCandidateResults(page, options.timeoutMs);
    const historyUrl = page.url();
    const initialResults = await collectCandidatePreview(page);
    const pagination =
      options.checkPaginationStability
        ? await verifyPaginationStability(page, options)
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
    const reloadedWait = await waitForCandidateResults(page, options.timeoutMs);
    const reloadedResults = await collectCandidatePreview(page);
    const reloadedTimelineDurations = await collectTimelineDurations(page);

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
      pagination,
      screenshotPath: options.screenshotPath,
    };
    process.stdout.write(`${JSON.stringify(summary, null, 2)}\n`);
  } finally {
    await context.close();
    await browser.close();
  }
}

main().catch((error) => {
  process.stderr.write(`${error instanceof Error ? error.stack || error.message : String(error)}\n`);
  process.exit(1);
});
