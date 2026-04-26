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
    screenshotPath: path.join(outputRoot, "target-public-web-e2e.png"),
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
    if (current === "--help" || current === "-h") {
      process.stdout.write(
        [
          "Usage:",
          "  node ./scripts/run_target_public_web_e2e.mjs \\",
          "    --frontend-url http://127.0.0.1:4173",
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

  const browser = await chromium.launch({ headless: true });
  const context = await browser.newContext({
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
    await checkboxes.first().check();

    const startButton = page.getByTestId("target-candidates-public-web-start");
    await startButton.click();
    await page.getByText(/已排队|已加入现有 Public Web Search/).first().waitFor({
      state: "visible",
      timeout: options.timeoutMs,
    });
    await page.getByTestId("target-candidates-public-web-refresh").click();
    await page.waitForTimeout(500);

    const statusTexts = await page.getByTestId("target-candidate-public-web-status").evaluateAll((nodes) =>
      nodes.map((node) => String(node.textContent || "").replace(/\s+/g, " ").trim()).filter(Boolean),
    );
    const exportButtonText = await page.getByTestId("target-candidates-public-web-export").textContent().catch(() => "");
    await page.screenshot({ path: options.screenshotPath, fullPage: true });

    process.stdout.write(
      JSON.stringify(
        {
          status: "ok",
          initialCandidateCount,
          statusTexts,
          sawQueuedOrRunningStatus: statusTexts.some((text) =>
            /已排队|已提交搜索|搜索中|入口链接已就绪|抓取中|分析中|已完成|需人工复核/.test(text),
          ),
          exportButtonText: String(exportButtonText || "").trim(),
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
