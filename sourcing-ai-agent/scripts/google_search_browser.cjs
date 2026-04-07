#!/usr/bin/env node

const { chromium } = require("playwright");

function parseArgs(argv) {
  const args = {
    query: "",
    maxResults: 10,
    timeoutMs: 30000,
    headless: true,
    locale: "en-US",
  };
  for (let index = 0; index < argv.length; index += 1) {
    const key = argv[index];
    const value = argv[index + 1];
    if (key === "--query" && value) {
      args.query = value;
      index += 1;
    } else if (key === "--max-results" && value) {
      args.maxResults = Math.max(1, Math.min(parseInt(value, 10) || 10, 20));
      index += 1;
    } else if (key === "--timeout-ms" && value) {
      args.timeoutMs = Math.max(5000, parseInt(value, 10) || 30000);
      index += 1;
    } else if (key === "--headless" && value) {
      args.headless = String(value).toLowerCase() !== "false";
      index += 1;
    } else if (key === "--locale" && value) {
      args.locale = value;
      index += 1;
    }
  }
  if (!args.query) {
    throw new Error("Missing required --query");
  }
  return args;
}

function normalizeGoogleUrl(url) {
  if (!url) {
    return "";
  }
  try {
    const parsed = new URL(url, "https://www.google.com");
    if (parsed.hostname.endsWith("google.com") && parsed.pathname === "/url") {
      return parsed.searchParams.get("q") || "";
    }
    return parsed.toString();
  } catch (_err) {
    return String(url || "").trim();
  }
}

function shouldSkipUrl(url) {
  if (!url) {
    return true;
  }
  const lowered = String(url).toLowerCase();
  return (
    lowered.startsWith("javascript:") ||
    lowered.startsWith("mailto:") ||
    lowered.includes("google.com/search") ||
    lowered.includes("google.com/preferences") ||
    lowered.includes("accounts.google.com") ||
    lowered.includes("/setprefs") ||
    lowered.includes("/policies")
  );
}

function cleanText(text) {
  return String(text || "").replace(/\s+/g, " ").trim();
}

async function maybeHandleConsent(page) {
  const candidates = [
    'button:has-text("Accept all")',
    'button:has-text("I agree")',
    'button:has-text("Accept")',
    'form[action*="consent"] button',
  ];
  for (const selector of candidates) {
    const locator = page.locator(selector).first();
    try {
      if (await locator.isVisible({ timeout: 1500 })) {
        await locator.click({ timeout: 3000 });
        await page.waitForTimeout(1000);
        return true;
      }
    } catch (_err) {
      continue;
    }
  }
  return false;
}

async function extractResults(page, maxResults) {
  return page.evaluate((limit) => {
    const rows = [];
    const seen = new Set();
    const normalizeGoogleUrlInner = (url) => {
      try {
        const parsed = new URL(url, "https://www.google.com");
        if (parsed.hostname.endsWith("google.com") && parsed.pathname === "/url") {
          return parsed.searchParams.get("q") || "";
        }
        return parsed.toString();
      } catch (_err) {
        return String(url || "").trim();
      }
    };
    const shouldSkipUrlInner = (url) => {
      const lowered = String(url || "").toLowerCase();
      return (
        !lowered ||
        lowered.startsWith("javascript:") ||
        lowered.startsWith("mailto:") ||
        lowered.includes("google.com/search") ||
        lowered.includes("google.com/preferences") ||
        lowered.includes("accounts.google.com") ||
        lowered.includes("/setprefs") ||
        lowered.includes("/policies")
      );
    };
    const cleanTextInner = (text) => String(text || "").replace(/\s+/g, " ").trim();

    const anchors = Array.from(document.querySelectorAll('a[href]'));
    for (const anchor of anchors) {
      const titleNode = anchor.querySelector("h3") || anchor.querySelector("h4");
      const title = cleanTextInner(titleNode ? titleNode.textContent : anchor.textContent);
      if (!title) {
        continue;
      }
      const normalizedUrl = normalizeGoogleUrlInner(anchor.href);
      if (shouldSkipUrlInner(normalizedUrl) || seen.has(normalizedUrl)) {
        continue;
      }
      const container =
        anchor.closest("div.g") ||
        anchor.closest("div[data-snc]") ||
        anchor.closest("div[data-hveid]") ||
        anchor.parentElement;
      let snippet = "";
      if (container) {
        const snippetNode =
          container.querySelector("div.VwiC3b") ||
          container.querySelector("div[data-sncf]") ||
          container.querySelector("span.aCOpRe") ||
          container.querySelector("div.yXK7lf");
        snippet = cleanTextInner((snippetNode && snippetNode.textContent) || container.textContent || "");
        if (snippet.startsWith(title)) {
          snippet = cleanTextInner(snippet.slice(title.length));
        }
      }
      seen.add(normalizedUrl);
      rows.push({
        title,
        url: normalizedUrl,
        snippet: snippet.slice(0, 500),
        metadata: {
          source_domain: (() => {
            try {
              return new URL(normalizedUrl).hostname;
            } catch (_err) {
              return "";
            }
          })(),
        },
      });
      if (rows.length >= limit) {
        break;
      }
    }
    return rows;
  }, maxResults);
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const browser = await chromium.launch({
    headless: args.headless,
  });
  const context = await browser.newContext({
    locale: args.locale,
    userAgent:
      "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    viewport: { width: 1440, height: 960 },
  });
  const page = await context.newPage();
  page.setDefaultTimeout(args.timeoutMs);
  const searchUrl =
    "https://www.google.com/search?" +
    new URLSearchParams({
      q: args.query,
      num: String(args.maxResults),
      hl: args.locale.split("-")[0] || "en",
      gl: "us",
      pws: "0",
      safe: "off",
    }).toString();

  let consentHandled = false;
  await page.goto(searchUrl, { waitUntil: "domcontentloaded", timeout: args.timeoutMs });
  consentHandled = await maybeHandleConsent(page);
  await page.waitForTimeout(1200);

  const finalUrl = page.url();
  const content = await page.content();
  const blocked = /unusual traffic|detected unusual traffic|our systems have detected/i.test(content);
  const results = blocked ? [] : await extractResults(page, args.maxResults);

  const payload = {
    provider_name: "google_browser",
    query_text: args.query,
    final_url: finalUrl,
    results,
    metadata: {
      consent_handled: consentHandled,
      blocked,
      engine: "chromium",
      headless: args.headless,
      locale: args.locale,
    },
    raw_html: content,
  };
  console.log(JSON.stringify(payload));
  await context.close();
  await browser.close();
}

main().catch((error) => {
  console.error(String(error && error.stack ? error.stack : error));
  process.exit(1);
});
