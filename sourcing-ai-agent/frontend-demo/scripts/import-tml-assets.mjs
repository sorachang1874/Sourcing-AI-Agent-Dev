import fs from "node:fs";
import path from "node:path";

const frontendRoot = process.cwd();
const projectRoot = path.resolve(frontendRoot, "..");
const runtimeRoot = path.join(projectRoot, "runtime");
const companyRoot = path.join(runtimeRoot, "company_assets", "thinkingmachineslab");
const publicRoot = path.join(frontendRoot, "public", "tml");

function readJson(filePath) {
  return JSON.parse(fs.readFileSync(filePath, "utf8"));
}

function ensureDir(dirPath) {
  fs.mkdirSync(dirPath, { recursive: true });
}

function copyIfExists(sourcePath, destinationPath) {
  if (!fs.existsSync(sourcePath)) {
    return false;
  }
  ensureDir(path.dirname(destinationPath));
  fs.copyFileSync(sourcePath, destinationPath);
  return true;
}

function resolveLatestSnapshot() {
  const latestSnapshotPath = path.join(companyRoot, "latest_snapshot.json");
  if (fs.existsSync(latestSnapshotPath)) {
    const payload = readJson(latestSnapshotPath);
    const snapshotId = payload.snapshot_id || payload.snapshotId || payload.latest_snapshot_id;
    if (snapshotId) {
      return snapshotId;
    }
  }

  if (!fs.existsSync(companyRoot)) {
    return null;
  }

  const snapshotDirs = fs
    .readdirSync(companyRoot, { withFileTypes: true })
    .filter((entry) => entry.isDirectory())
    .map((entry) => entry.name)
    .sort();

  return snapshotDirs.at(-1) || null;
}

function main() {
  const snapshotId = resolveLatestSnapshot();
  if (!snapshotId) {
    console.error("No Thinking Machines Lab snapshot found under runtime/company_assets/thinkingmachineslab.");
    process.exit(1);
  }

  const snapshotRoot = path.join(companyRoot, snapshotId);
  const normalizedRoot = path.join(snapshotRoot, "normalized_artifacts");
  ensureDir(publicRoot);

  const copied = [];
  const filesToCopy = [
    ["normalized_candidates.json", path.join(normalizedRoot, "normalized_candidates.json")],
    ["materialized_candidate_documents.json", path.join(normalizedRoot, "materialized_candidate_documents.json")],
    ["reusable_candidate_documents.json", path.join(normalizedRoot, "reusable_candidate_documents.json")],
    ["manual_review_backlog.json", path.join(normalizedRoot, "manual_review_backlog.json")],
    ["profile_completion_backlog.json", path.join(normalizedRoot, "profile_completion_backlog.json")],
    ["asset_registry.json", path.join(snapshotRoot, "asset_registry.json")],
    ["manifest.json", path.join(snapshotRoot, "manifest.json")],
  ];

  for (const [name, sourcePath] of filesToCopy) {
    const destinationPath = path.join(publicRoot, name);
    if (copyIfExists(sourcePath, destinationPath)) {
      copied.push(name);
    }
  }

  const indexPayload = {
    company: "thinkingmachineslab",
    snapshotId,
    importedAt: new Date().toISOString(),
    files: copied,
  };
  fs.writeFileSync(path.join(publicRoot, "index.json"), `${JSON.stringify(indexPayload, null, 2)}\n`, "utf8");

  console.log(`Imported Thinking Machines Lab assets for snapshot ${snapshotId}.`);
  console.log(`Copied files: ${copied.join(", ")}`);
}

main();
