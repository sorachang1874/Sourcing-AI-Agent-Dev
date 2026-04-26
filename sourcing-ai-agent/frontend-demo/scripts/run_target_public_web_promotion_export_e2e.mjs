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
    timeoutMs: 60000,
    screenshotPath: path.join(outputRoot, "target-public-web-promotion-export-e2e.png"),
    downloadPath: path.join(outputRoot, "target-public-web-promoted-export.zip"),
    expectedEmail: "public.web.browser@example.edu",
  };
  for (let index = 0; index < argv.length; index += 1) {
    const current = argv[index];
    const next = argv[index + 1];
    if (current === "--frontend-url" && next) {
      options.frontendUrl = next;
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
    if (current === "--download" && next) {
      options.downloadPath = next;
      index += 1;
      continue;
    }
    if (current === "--expected-email" && next) {
      options.expectedEmail = next;
      index += 1;
      continue;
    }
    if (current === "--help" || current === "-h") {
      process.stdout.write(
        [
          "Usage:",
          "  node ./scripts/run_target_public_web_promotion_export_e2e.mjs \\",
          "    --frontend-url http://127.0.0.1:4173 \\",
          "    --expected-email public.web.browser@example.edu",
        ].join("\n"),
      );
      process.stdout.write("\n");
      process.exit(0);
    }
  }
  return options;
}

async function main() {
  const options = parseArgs(process.argv.slice(2));
  await fs.mkdir(path.dirname(options.screenshotPath), { recursive: true });
  await fs.mkdir(path.dirname(options.downloadPath), { recursive: true });

  const browser = await chromium.launch({ headless: true });
  const context = await browser.newContext({
    acceptDownloads: true,
    viewport: { width: 1440, height: 1100 },
  });
  const page = await context.newPage();
  try {
    await page.goto(`${options.frontendUrl.replace(/\/$/, "")}/targets`, {
      waitUntil: "domcontentloaded",
      timeout: options.timeoutMs,
    });
    const checkboxes = page.getByTestId("target-candidate-select-checkbox");
    await checkboxes.first().waitFor({ state: "visible", timeout: options.timeoutMs });
    const initialCandidateCount = await checkboxes.count();

    await page.getByRole("button", { name: /查看公开信息详情/ }).first().click();
    await page.getByText(options.expectedEmail).first().waitFor({
      state: "visible",
      timeout: options.timeoutMs,
    });
    await page.getByRole("button", { name: /确认并设为 primary email/ }).first().click();
    await page.getByRole("button", { name: /已确认邮箱/ }).first().waitFor({
      state: "visible",
      timeout: options.timeoutMs,
    });
    await page.getByRole("button", { name: /确认导出链接/ }).first().click();
    await page.getByRole("button", { name: /已确认链接/ }).first().waitFor({
      state: "visible",
      timeout: options.timeoutMs,
    });
    await page.getByText(/Public Web signal 已人工确认/).first().waitFor({
      state: "visible",
      timeout: options.timeoutMs,
    });

    const downloadPromise = page.waitForEvent("download", { timeout: options.timeoutMs });
    await page.getByTestId("target-candidates-public-web-export").click();
    const download = await downloadPromise;
    await download.saveAs(options.downloadPath);
    const downloadedStat = await fs.stat(options.downloadPath);
    await page.screenshot({ path: options.screenshotPath, fullPage: true });

    process.stdout.write(
      JSON.stringify(
        {
          status: "ok",
          initialCandidateCount,
          promotedEmailVisible: await page.getByText(options.expectedEmail).first().isVisible(),
          promotedLinkVisible: await page.getByRole("button", { name: /已确认链接/ }).first().isVisible(),
          suggestedFilename: download.suggestedFilename(),
          downloadedSize: downloadedStat.size,
          screenshotPath: options.screenshotPath,
          downloadPath: options.downloadPath,
        },
        null,
        2,
      ),
    );
    process.stdout.write("\n");
  } finally {
    await context.close();
    await browser.close();
  }
}

main().catch((error) => {
  process.stderr.write(`${error instanceof Error ? error.stack || error.message : String(error)}\n`);
  process.exit(1);
});
