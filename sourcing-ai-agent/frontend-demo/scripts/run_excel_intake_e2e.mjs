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
    workbookPath: "",
    timeoutMs: 60000,
    screenshotPath: path.join(outputRoot, "excel-intake-e2e.png"),
    exerciseTargetActions: false,
  };
  for (let index = 0; index < argv.length; index += 1) {
    const current = argv[index];
    const next = argv[index + 1];
    if (current === "--frontend-url" && next) {
      options.frontendUrl = next;
      index += 1;
      continue;
    }
    if (current === "--workbook" && next) {
      options.workbookPath = next;
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
    if (current === "--exercise-target-actions") {
      options.exerciseTargetActions = true;
      continue;
    }
    if (current === "--help" || current === "-h") {
      process.stdout.write(
        [
          "Usage:",
          "  node ./scripts/run_excel_intake_e2e.mjs \\",
          "    --frontend-url http://127.0.0.1:4173 \\",
          "    --workbook /path/to/contacts.xlsx \\",
          "    [--exercise-target-actions]",
        ].join("\n"),
      );
      process.stdout.write("\n");
      process.exit(0);
    }
  }
  if (!options.workbookPath) {
    throw new Error("--workbook is required");
  }
  return options;
}

function sleep(ms) {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}

async function waitForEnabled(locator, timeoutMs, label) {
  await locator.waitFor({ state: "visible", timeout: timeoutMs });
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await locator.isEnabled().catch(() => false)) {
      return;
    }
    await sleep(250);
  }
  throw new Error(`${label} was not enabled within ${timeoutMs}ms`);
}

async function waitForText(locator, matcher, timeoutMs, label) {
  await locator.waitFor({ state: "visible", timeout: timeoutMs });
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const text = String((await locator.textContent().catch(() => "")) || "").trim();
    if (typeof matcher === "string" ? text.includes(matcher) : matcher.test(text)) {
      return text;
    }
    await sleep(250);
  }
  throw new Error(`${label} did not match within ${timeoutMs}ms`);
}

async function main() {
  const options = parseArgs(process.argv.slice(2));
  await fs.mkdir(path.dirname(options.screenshotPath), { recursive: true });

  const browser = await chromium.launch({ headless: true });
  const context = await browser.newContext({
    viewport: { width: 1440, height: 1100 },
    acceptDownloads: true,
  });
  const page = await context.newPage();
  try {
    await page.goto(options.frontendUrl, { waitUntil: "domcontentloaded", timeout: options.timeoutMs });
    await page.getByTestId("excel-intake-panel").waitFor({ state: "visible", timeout: options.timeoutMs });
    await page.getByTestId("excel-intake-file-input").setInputFiles(options.workbookPath);
    await page.getByTestId("excel-intake-submit").click();
    await page.getByTestId("excel-intake-result").waitFor({ state: "visible", timeout: options.timeoutMs });

    let targetActions = null;
    if (options.exerciseTargetActions) {
      const firstGroup = page.getByTestId("excel-intake-group").first();
      const importButton = firstGroup.getByTestId("excel-target-import-button");
      const exportButton = firstGroup.getByTestId("excel-target-export-button");
      await importButton.waitFor({ state: "visible", timeout: options.timeoutMs });
      await exportButton.waitFor({ state: "visible", timeout: options.timeoutMs });
      await waitForEnabled(importButton, options.timeoutMs, "Excel target import button");
      await importButton.click();
      const message = firstGroup.getByTestId("excel-target-action-message");
      const importMessage = await waitForText(
        message,
        /已导入\s+[1-9]\d*\s+位目标候选人/,
        options.timeoutMs,
        "Excel target import message",
      );
      await waitForEnabled(exportButton, options.timeoutMs, "Excel target export button");
      const downloadPromise = page.waitForEvent("download", { timeout: options.timeoutMs });
      await exportButton.click();
      const download = await downloadPromise;
      const downloadPath = path.join(outputRoot, download.suggestedFilename() || "target-candidates.zip");
      await download.saveAs(downloadPath);
      const exportMessage = await waitForText(
        message,
        "目标候选人包已开始下载",
        options.timeoutMs,
        "Excel target export message",
      );
      targetActions = {
        importMessage,
        exportMessage,
        downloadFilename: download.suggestedFilename(),
        downloadPath,
      };
    }
    const groupTexts = await page.getByTestId("excel-intake-group").evaluateAll((nodes) =>
      nodes.map((node) => String(node.textContent || "").replace(/\s+/g, " ").trim()),
    );
    const rowManifestSummaries = await page.getByTestId("excel-row-manifest-summary").evaluateAll((nodes) =>
      nodes.map((node) => String(node.textContent || "").replace(/\s+/g, " ").trim()),
    );
    const errorText = await page.getByTestId("excel-intake-error").textContent().catch(() => "");
    await page.screenshot({ path: options.screenshotPath, fullPage: true });

    process.stdout.write(
      JSON.stringify(
        {
          status: "ok",
          groupCount: groupTexts.length,
          groups: groupTexts,
          rowManifestSummaries,
          targetActions,
          errorText: String(errorText || "").trim(),
          screenshotPath: options.screenshotPath,
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
