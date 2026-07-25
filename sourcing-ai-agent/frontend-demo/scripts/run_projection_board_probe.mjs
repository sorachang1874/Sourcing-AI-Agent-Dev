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
    projectionId: "",
    expectedCount: 0,
    expectedFirstName: "",
    expectedPage2Name: "",
    searchKeyword: "",
    expectedSearchName: "",
    timeoutMs: 45000,
    screenshotPath: path.join(outputRoot, "projection-board-probe.png"),
  };
  for (let index = 0; index < argv.length; index += 1) {
    const current = argv[index];
    const next = argv[index + 1];
    if (current === "--frontend-url" && next) {
      options.frontendUrl = next;
      index += 1;
    } else if (current === "--projection-id" && next) {
      options.projectionId = next;
      index += 1;
    } else if (current === "--expected-count" && next) {
      options.expectedCount = Math.max(0, Number(next) || 0);
      index += 1;
    } else if (current === "--expected-first-name" && next) {
      options.expectedFirstName = next;
      index += 1;
    } else if (current === "--expected-page2-name" && next) {
      options.expectedPage2Name = next;
      index += 1;
    } else if (current === "--search-keyword" && next) {
      options.searchKeyword = next;
      index += 1;
    } else if (current === "--expected-search-name" && next) {
      options.expectedSearchName = next;
      index += 1;
    } else if (current === "--timeout-ms" && next) {
      options.timeoutMs = Math.max(1000, Number(next) || options.timeoutMs);
      index += 1;
    } else if (current === "--screenshot" && next) {
      options.screenshotPath = next;
      index += 1;
    } else if (current === "--help" || current === "-h") {
      process.stdout.write(
        [
          "Usage:",
          "  node ./scripts/run_projection_board_probe.mjs --frontend-url http://127.0.0.1:4173 \\",
          "    --projection-id proj_... --expected-count 30 --expected-first-name 'Ada'",
        ].join("\n"),
      );
      process.stdout.write("\n");
      process.exit(0);
    }
  }
  if (!options.projectionId) {
    throw new Error("--projection-id is required");
  }
  return options;
}

function frontendProjectionUrl(frontendUrl, projectionId) {
  const normalizedBase = String(frontendUrl || "").replace(/\/+$/, "");
  return `${normalizedBase}/projections/${encodeURIComponent(projectionId)}`;
}

async function expectNoFatalUiText(page) {
  const bodyText = await page.locator("body").innerText({ timeout: 5000 });
  const forbidden = [
    "结果加载失败",
    "Request timed out",
    "当前筛选条件下没有候选人",
    "工作流完成后会在这里呈现",
  ];
  const observed = forbidden.filter((item) => bodyText.includes(item));
  if (observed.length > 0) {
    throw new Error(`fatal UI text observed: ${observed.join(", ")}`);
  }
}

async function waitForVisibleCount(page, expectedCount, timeoutMs) {
  const expectedText = `${expectedCount}/${expectedCount}`;
  await page.locator('[data-testid="results-visible-count"]').waitFor({ state: "visible", timeout: timeoutMs });
  await page.waitForFunction(
    ({ selector, expected }) => {
      const node = document.querySelector(selector);
      return Boolean(node && node.textContent && node.textContent.trim() === expected);
    },
    { selector: '[data-testid="results-visible-count"]', expected: expectedText },
    { timeout: timeoutMs },
  );
  return expectedText;
}

async function collectCandidateNames(page) {
  return (await page.locator('[data-testid="results-candidate-card"] h4').allTextContents())
    .map((item) => String(item || "").trim())
    .filter(Boolean);
}

async function waitForCandidateName(page, expectedName, timeoutMs) {
  if (!expectedName) {
    return;
  }
  await page.getByText(expectedName, { exact: false }).waitFor({ state: "visible", timeout: timeoutMs });
}

async function fetchProjectionCandidatePage(page, projectionId, searchKeyword) {
  return page.evaluate(
    async ({ id, search }) => {
      const params = new URLSearchParams({ offset: "0", limit: "24" });
      if (search) {
        params.set("search", search);
      }
      const response = await fetch(`/api/projections/${encodeURIComponent(id)}/candidates?${params.toString()}`, {
        headers: { accept: "application/json" },
      });
      if (!response.ok) {
        throw new Error(`candidate page fetch failed: ${response.status}`);
      }
      return response.json();
    },
    { id: projectionId, search: searchKeyword },
  );
}

async function main() {
  const options = parseArgs(process.argv.slice(2));
  await fs.mkdir(path.dirname(options.screenshotPath), { recursive: true });
  const browser = await chromium.launch({ headless: true });
  const page = await browser.newPage({ viewport: { width: 1440, height: 1100 } });
  try {
    page.setDefaultTimeout(options.timeoutMs);
    await page.goto(frontendProjectionUrl(options.frontendUrl, options.projectionId), {
      waitUntil: "domcontentloaded",
      timeout: options.timeoutMs,
    });
    await page.locator('[data-testid="results-board-panel"]').waitFor({ state: "visible", timeout: options.timeoutMs });
    const visibleCountText = await waitForVisibleCount(page, options.expectedCount, options.timeoutMs);
    await waitForCandidateName(page, options.expectedFirstName, options.timeoutMs);
    await expectNoFatalUiText(page);
    const firstPageNames = await collectCandidateNames(page);
    if (firstPageNames.length === 0) {
      throw new Error("candidate cards did not render on first page");
    }

    let page2Names = [];
    if (options.expectedPage2Name) {
      await page.getByRole("button", { name: "下一页" }).click();
      await waitForCandidateName(page, options.expectedPage2Name, options.timeoutMs);
      await expectNoFatalUiText(page);
      page2Names = await collectCandidateNames(page);
    }

    let searchPayload = null;
    let searchNames = [];
    if (options.searchKeyword) {
      await page.locator("#results-keyword").fill(options.searchKeyword);
      await waitForCandidateName(page, options.expectedSearchName || options.searchKeyword, options.timeoutMs);
      await expectNoFatalUiText(page);
      searchNames = await collectCandidateNames(page);
      searchPayload = await fetchProjectionCandidatePage(page, options.projectionId, options.searchKeyword);
      if (searchPayload?.filter_contract?.fallback_used) {
        throw new Error(`canonical search used fallback: ${searchPayload.filter_contract.fallback_reason || "unknown"}`);
      }
    }

    await page.screenshot({ path: options.screenshotPath, fullPage: true });
    process.stdout.write(
      `${JSON.stringify(
        {
          status: "ok",
          projectionId: options.projectionId,
          visibleCountText,
          firstPageCardCount: firstPageNames.length,
          firstPageNames,
          page2Names,
          searchNames,
          searchFilteredCandidateCount: searchPayload ? Number(searchPayload.filtered_candidate_count || 0) : null,
          searchFilterContract: searchPayload?.filter_contract || null,
          screenshotPath: options.screenshotPath,
        },
        null,
        2,
      )}\n`,
    );
  } finally {
    await browser.close();
  }
}

main().catch((error) => {
  process.stderr.write(`${error instanceof Error ? error.stack || error.message : String(error)}\n`);
  process.exit(1);
});
