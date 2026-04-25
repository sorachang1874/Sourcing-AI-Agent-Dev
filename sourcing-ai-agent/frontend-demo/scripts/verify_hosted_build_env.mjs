import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const SAME_ORIGIN_VALUES = new Set(["/", "same-origin", "same_origin", "sameorigin", "relative", "origin-relative"]);

function stripWrappingQuotes(value) {
  if (value.length >= 2 && ((value.startsWith('"') && value.endsWith('"')) || (value.startsWith("'") && value.endsWith("'")))) {
    return value.slice(1, -1);
  }
  return value;
}

function readEnvValueFromFile(filePath, key) {
  if (!fs.existsSync(filePath)) {
    return "";
  }
  const raw = fs.readFileSync(filePath, "utf8");
  for (const line of raw.split(/\r?\n/)) {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith("#")) {
      continue;
    }
    const match = trimmed.match(/^([A-Za-z_][A-Za-z0-9_]*)=(.*)$/);
    if (!match || match[1] !== key) {
      continue;
    }
    return stripWrappingQuotes(match[2].trim());
  }
  return "";
}

function resolveHostedApiBaseUrl() {
  const envValue = String(process.env.VITE_API_BASE_URL || "").trim();
  if (envValue) {
    return {
      source: "process.env",
      value: envValue,
    };
  }

  const projectRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
  for (const filename of [".env.production.local", ".env.production"]) {
    const filePath = path.join(projectRoot, filename);
    const fileValue = readEnvValueFromFile(filePath, "VITE_API_BASE_URL");
    if (fileValue) {
      return {
        source: filename,
        value: fileValue,
      };
    }
  }

  return {
    source: "",
    value: "",
  };
}

const { source, value } = resolveHostedApiBaseUrl();
const apiBaseUrl = String(value || "").trim();

if (!apiBaseUrl) {
  console.error("Missing VITE_API_BASE_URL for hosted frontend build. Set it in process.env or .env.production.");
  process.exit(1);
}

if (SAME_ORIGIN_VALUES.has(apiBaseUrl.toLowerCase())) {
  console.log(`Using hosted API mode from ${source || "unknown"}: same-origin (/api/* via ingress or worker proxy)`);
  process.exit(0);
}

let parsed;
try {
  parsed = new URL(apiBaseUrl);
} catch {
  console.error(`Invalid VITE_API_BASE_URL: ${apiBaseUrl}`);
  process.exit(1);
}

if (["localhost", "127.0.0.1"].includes(parsed.hostname)) {
  console.error(`Refusing hosted build with local VITE_API_BASE_URL: ${apiBaseUrl}`);
  process.exit(1);
}

console.log(`Using hosted API base URL from ${source || "unknown"}: ${apiBaseUrl}`);
