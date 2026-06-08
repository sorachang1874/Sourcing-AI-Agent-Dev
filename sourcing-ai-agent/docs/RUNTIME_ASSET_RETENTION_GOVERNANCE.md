# Runtime Asset Retention Governance

> Status: active M0.5 operator guidance. Use with `docs/DATA_ASSET_GOVERNANCE.md` and `docs/SERVICE_GRADE_ARCHITECTURE_PLAN.md`.

## Purpose

M0.5 local asset governance has two different layers:

1. Company snapshot consolidation governs `runtime/company_assets/<company>/<snapshot_id>` and canonical/hot-cache company assets. Use the W5 asset consolidation audit, plan, cold archive manifest, and reviewed apply flow.
2. Runtime/output retention governs historical test, signoff, pressure, and manual-run directories such as `runtime/test_env/*`, `output/*`, W6/nightly/pre-manual artifacts, and local browser validation output. These directories are not authoritative company snapshots, but they can be needed for signoff evidence, rebuild rehearsal, incident reconstruction, or regression comparison.

Do not use a company snapshot archive decision as permission to delete runtime/test_env or output directories. Do not use a runtime retention inventory as permission to delete company snapshots.

## Read-Only Inventory

Use the runtime retention audit before moving or compressing historical run directories:

```sh
PYTHONPATH=src .venv/bin/python scripts/audit_runtime_asset_retention.py \
  --workspace-root . \
  --root runtime/test_env \
  --root output \
  --include-name google \
  --include-name reflection \
  --include-name nightly \
  --include-name w6 \
  --include-name pre_manual \
  --include-name phase12 \
  --min-size-mb 20 \
  --sample-limit 120 \
  --output-json runtime/asset_governance/<run_id>/runtime_retention_audit.json \
  --output-md runtime/asset_governance/<run_id>/runtime_retention_audit.md
```

The audit is read-only:

- It does not delete, move, compress, or exclude anything from reuse.
- It records `deletion_allowed=false`.
- It classifies directories as review targets, not safe deletion targets.
- It includes post-review gates that must be completed before any apply/archive action.

## Review Classes

`review_signoff_or_pressure_run_artifact`

Historical W6, nightly, pre-manual, pressure, or signoff artifacts. These may be large and usually have superseded reruns, but they should only be cold-archived after signoff supersession proof and review.

`review_phase_milestone_artifact`

Phase or milestone-specific artifacts. These need milestone supersession proof before archive because they often explain a contract transition or incident.

`review_current_alias_or_latest_artifact`

Directories with `current` or `latest` in the name. These are not first-pass archive candidates. Resolve the alias target and confirm a replacement pointer before any movement.

`review_cold_archive_candidate`

Generic historical runtime/output directory matching the requested markers. It still needs cold-copy/hash proof, rebuild/projection rehearsal if relevant, and Independent Review Gate before apply.

## Apply Boundary

There is intentionally no `--apply` mode in `scripts/audit_runtime_asset_retention.py`.

Any future apply tool must be a separate reviewed operation and must:

- Read an explicit reviewed manifest, not rescan live directories and decide on the fly.
- Refuse to operate when active runtime daemons are detected.
- Verify cold-copy hash or package manifest before normal-path removal/exclusion.
- Preserve enough metadata to reconstruct the original local path, file count, byte count, latest mtime, and review artifact.
- Record whether the directory is only moved out of the hot local workspace or also excluded from a normal reuse index.
- Run an Independent Review Gate before the first real apply.

## Current M0.5 Evidence

The first local inventory evidence for Google/Reflection AI and historical W6/nightly/pre-manual artifacts lives under:

```text
runtime/asset_governance/m0_5_20260608_google_reflection_v2/
```

Key observations from that read-only run:

- `runtime/company_assets/google` is about 12GB and must be governed through company snapshot consolidation, not runtime retention.
- The first company snapshot cold archive manifest has only 6 manifest-ready Reflection AI snapshots, about 108MB. Google company snapshots remain blocked by unique identities or dependencies.
- Runtime/output retention is the larger opportunity: 341 matched historical directories total about 130.9GB, with about 128.1GB under `runtime/test_env`.
- Most large directories are W6/nightly/pre-manual/signoff artifacts. They need supersession proof, not immediate deletion.
