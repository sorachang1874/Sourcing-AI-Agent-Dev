# ECS Code And Asset Migration Playbook

> Status: Current migration playbook. Use this with `ECS_ACCESS_PLAYBOOK.md`, `ECS_PRELAUNCH_CHECKLIST.md`, `DATA_ASSET_GOVERNANCE.md`, and `CANONICAL_CLOUD_BUNDLE_CATALOG.md` before replacing hosted/ECS code or runtime assets.

## Goal

This playbook captures the repeatable path for moving both current code and curated data assets to ECS.

The main lesson from the April 2026 ECS trial is that code and assets drift independently:

- ECS may run old code from `/opt/sourcing-ai-agent` while newer runtime assets live under `/srv/sourcing-ai-agent/runtime`.
- `latest_snapshot.json` can be stale and must not be treated as hosted production authority.
- Large companies can have many historical snapshots; copying all of them makes rebuilds, backups, and migrations slower without improving hosted behavior.
- The live control plane must remain Postgres-only. Disk-backed SQLite snapshots are retired and should not be restored as live authority.

## Source Of Truth Order

Use this order when deciding what to migrate:

1. Current code checkout or release commit.
2. Postgres `organization_asset_registry.authoritative`, `asset_default_pointers`, and `job_result_views`.
3. Local canonical `runtime/company_assets/<company>/<snapshot_id>` for selected snapshots.
4. Cloud/object-storage bundle indexes when using bundle import/export.
5. `latest_snapshot.json` only as a compatibility pointer or bundle helper, never as the hosted authority.

## Code Migration

Recommended target layout:

```text
/srv/sourcing-ai-agent/
  repo/
    sourcing-ai-agent/
  runtime/
    company_assets/
    secrets/
    object_sync/
    service_logs/
```

Code migration gates:

1. Stop the old backend and worker daemon first.
2. Deploy the current code into exactly one code root, preferably `/srv/sourcing-ai-agent/repo/sourcing-ai-agent`.
3. Align systemd `WorkingDirectory`, `ExecStart`, venv path, env file, and Nginx upstream docs to that same root.
4. Move the old `/opt/sourcing-ai-agent` root out of active service paths after probes pass. Do not delete `runtime/secrets` by accident.
5. Do not require legacy sibling skill packages (`anthropic-employee-scan`, `investor-chinese-scan`, `biz-visit-onepager`) for hosted runtime startup. They may exist in a developer monorepo, but production runtime summary must work from the `sourcing-ai-agent` repo plus in-repo `local_asset_packages`.
6. Verify the running process imports the new code:

```bash
pwd
git rev-parse HEAD
./.venv/bin/python -c "import sourcing_agent; print('import-ok')"
bash ./scripts/run_hosted_trial_backend.sh --print-config
```

If `/api/providers/apify/webhook` is still `404` after restart, ECS is still serving old code or an old route.

Recommended systemd shape, with secrets kept in an env file rather than embedded in the unit:

```ini
[Service]
Type=simple
User=root
WorkingDirectory=/srv/sourcing-ai-agent/repo/sourcing-ai-agent
EnvironmentFile=/etc/sourcing-ai-agent.env
Environment=PYTHONPATH=/srv/sourcing-ai-agent/repo/sourcing-ai-agent/src
Environment=SOURCING_RUNTIME_DIR=/srv/sourcing-ai-agent/runtime
Environment=SOURCING_RUNTIME_ENVIRONMENT=production
Environment=SOURCING_EXTERNAL_PROVIDER_MODE=live
Environment=SOURCING_CONTROL_PLANE_POSTGRES_LIVE_MODE=postgres_only
Environment=SOURCING_REQUIRE_CONTROL_PLANE_POSTGRES=1
Environment=SOURCING_PG_ONLY_SQLITE_BACKEND=shared_memory
Environment=SOURCING_API_ALLOWED_ORIGINS=https://demo.111874.xyz,https://api.111874.xyz
Environment=SOURCING_API_MAX_PARALLEL_REQUESTS=8
Environment=SOURCING_API_LIGHT_REQUEST_RESERVED=2
ExecStart=/opt/sourcing-ai-agent/venv/bin/python3 -m sourcing_agent.cli serve --host 0.0.0.0 --port 8765
Restart=always
RestartSec=5
```

`/etc/sourcing-ai-agent.env` should contain secret-bearing values such as `SOURCING_CONTROL_PLANE_POSTGRES_DSN`, provider tokens, and `SOURCING_PROVIDER_WEBHOOK_TOKEN`. Do not keep adding secrets directly to the unit file.

## Registry Promotion After Asset Copy

Copying selected snapshot files is not enough to change hosted serving behavior. The hosted runtime reads PG registry/result-view state first, so every migration must explicitly reconcile those rows after rsync or bundle import.

Required checks:

1. Run organization/profile registry backfill for selected companies.
2. Compare `organization_asset_registry.authoritative` against the migration manifest's selected snapshot id.
3. Rebuild/backfill acquisition shard registry after the selected snapshot files are present on ECS. This must happen after rsync/bundle import, because `search_seed_discovery/current|former/summary.json` may not exist when an earlier registry backfill ran.
4. Run `audit-company-serving-view --company <company> --snapshot-id <selected_snapshot_id>` for high-value companies.
5. If audit resolves to an older snapshot, repair PG registry state before serving traffic.

Important edge case:

- Some companies have older ECS registry rows with higher raw candidate counts than the curated selected snapshot.
- If those old rows remain `ready`, runtime inventory selection can drift back to them during execution-profile or reuse planning.
- When migration intentionally chooses a curated snapshot that is not the highest-count historical row, mark non-selected historical registry rows `superseded` or `archived`, then promote the selected row as authoritative.
- Do not delete old snapshot files as part of this step. Superseding the PG row keeps runtime behavior stable while preserving rollback/audit material.

Example from the 2026-04-30 ECS migration:

- Google was promoted to `20260428T011339` and audited with `9359` candidates.
- Anthropic initially drifted back to old `20260409T045403` because that row had a higher count. The fix was to mark old Anthropic rows `superseded` and promote `20260416T225318`, which then audited with `3455` candidates.
- OpenAI `20260430T090520` had the correct 1110-candidate serving artifact, but the PG shard registry initially missed the `health` current/former search shards that were present on disk. The UI therefore planned `Baseline 复用 + 缺口增量` until `ensure_acquisition_shard_registry_for_snapshot` registered those two shards. After repair, the same Health plan had `requires_delta_acquisition=false`.
- Meta showed the next failure mode: copying only the serving snapshot `20260427T203601` preserved Audio coverage but dropped selected source snapshots for Multimodal and Agent. The fix was to migrate the source snapshots still selected by the local authoritative registry and restore `selected_snapshot_ids` in ECS PG.
- Google showed the broader rule: selected production scope is not just one serving baseline. Hosted hot assets need the serving snapshot plus source snapshots that carry nonzero reusable shard/profile proof. For Google this meant keeping `20260428T011339` as the serving baseline while selecting `20260410T123708`, `20260411T174325`, `20260411T215236`, `20260413T073549`, and `20260413T100525` for Multimodal/Veo/Nano Banana/Video generation/Vision-language reuse.
- Do not mark incomplete shards as complete just to avoid delta. Google Gemini rows in `20260428T011339` are useful partial assets but remain `status=incomplete` (`615/736` current, `568/991` former), so a Gemini query may still require delta acquisition until the missing pages are recovered or policy explicitly accepts partial coverage.

## Asset Migration Manifest

Generate a read-only migration manifest before copying assets:

```bash
PYTHONPATH=src ./.venv/bin/python scripts/build_ecs_asset_migration_manifest.py \
  --prefer-fullest-company thinkingmachineslab \
  --prefer-fullest-company reflectionai
```

The script writes JSON and Markdown under `runtime/deployment/`.

What it records:

- selected production serving snapshot per company
- source snapshots that must remain hot because they provide reusable shard/profile coverage for scoped search
- whether `latest_snapshot.json` disagrees with the selected snapshot
- local candidate/profile counts and byte size
- archive candidates for older snapshots
- job-result-view references that make a snapshot unsafe to delete without historical replay review
- an `ecs_asset_rsync_all_files_<timestamp>.txt` file that can be passed directly to `rsync --files-from`

Default selection is `organization_asset_registry.authoritative`, including its `selected_snapshot_ids`, not only `snapshot_id`. Use `--prefer-fullest-company` only for repeated no-increment baselines such as Thinking Machines Lab or Reflection AI, where the operational goal is to keep one fullest baseline and archive redundant repeats.

## Production Asset Scope

For the current ECS trial, production migration should prioritize:

- OpenAI `20260430T090520`
- Google `20260428T011339` plus selected source snapshots with nonzero shard coverage, currently `20260410T123708`, `20260411T174325`, `20260411T215236`, `20260413T073549`, and `20260413T100525`
- Meta `20260427T203601` plus selected source snapshots `20260427T190455`, `20260423T062947`, and `20260427T153312`
- Lovable `20260426T193540`
- Anthropic `20260416T225318`
- xAI `20260416T012752`
- Recent single-snapshot assets such as Mistral AI, Perplexity, Windsurf, Physical Intelligence, Wispr Flow, Manus AI, Periodic Labs, Delve, Safe Superintelligence, PostHog, Monica AI
- Thinking Machines Lab and Reflection AI: one fullest baseline each, not every repeated historical snapshot

This is not a permanent hard-coded list. Regenerate the manifest before each migration and use the latest registry state.

## Asset Copy Pattern

Prefer selective copy from the manifest, not whole-runtime rsync:

```bash
rsync -aHz --partial --stats \
  --files-from=runtime/deployment/ecs_asset_rsync_all_files_<timestamp>.txt \
  runtime/ \
  sourcing-ecs:/srv/sourcing-ai-agent/runtime/
```

This copies only the selected snapshot files listed by the manifest. It does not delete older ECS snapshots. After copying a selected company snapshot, also copy or repair the company-level compatibility files if required:

```bash
rsync -aH --info=progress2 \
  runtime/company_assets/openai/latest_snapshot.json \
  sourcing-ecs:/srv/sourcing-ai-agent/runtime/company_assets/openai/latest_snapshot.json
```

Only do this when the pointer has been reviewed. For hosted authority, registry pointers must still be repaired/backfilled in Postgres.

`latest_snapshot.json` remains compatibility metadata only. If it is retained, sync it from the PG authoritative registry after promotion instead of using it to decide what to promote.

If using bundle import/export instead of rsync:

```bash
PYTHONPATH=src ./.venv/bin/python -m sourcing_agent.cli export-company-snapshot-bundle \
  --company openai \
  --snapshot-id 20260430T090520

PYTHONPATH=src ./.venv/bin/python -m sourcing_agent.cli upload-asset-bundle \
  --manifest runtime/asset_exports/<bundle>/bundle_manifest.json

PYTHONPATH=src ./.venv/bin/python -m sourcing_agent.cli import-cloud-assets \
  --bundle-kind company_snapshot \
  --bundle-id <bundle_id> \
  --target-runtime-dir /srv/sourcing-ai-agent/runtime
```

## Post-Copy Repair And Verification

Run these on ECS after code and selected assets are in place:

```bash
PYTHONPATH=src ./.venv/bin/python -m sourcing_agent.cli show-control-plane-runtime
PYTHONPATH=src ./.venv/bin/python -m sourcing_agent.cli backfill-organization-asset-registry --asset-view canonical_merged
PYTHONPATH=src ./.venv/bin/python -m sourcing_agent.cli backfill-linkedin-profile-registry --profile-progress-interval 500
```

For high-value companies, audit before serving:

```bash
PYTHONPATH=src ./.venv/bin/python -m sourcing_agent.cli audit-company-serving-view --company openai --snapshot-id 20260430T090520
PYTHONPATH=src ./.venv/bin/python -m sourcing_agent.cli audit-company-serving-view --company google --snapshot-id 20260428T011339
```

Rebuild serving artifacts only when audit shows missing or stale projection:

```bash
PYTHONPATH=src ./.venv/bin/python -m sourcing_agent.cli rebuild-company-serving-view \
  --company openai \
  --snapshot-id 20260430T090520 \
  --build-profile foreground_fast
```

## Archive Policy

Do not delete old production snapshots directly from a shell prompt.

Archive workflow:

1. Generate the manifest.
2. Review `archive_candidates`.
3. Keep snapshots referenced by `job_result_views` unless historical replay behavior has been reviewed.
4. Move redundant snapshots to a cold prefix, test runtime, or dedicated historical store.
5. Only then remove them from the hot ECS runtime.

Good archive candidates:

- repeated no-increment snapshots with identical or lower candidate/profile counts
- early Google/Reflection AI/Thinking Machines Lab snapshots superseded by one selected baseline
- test/smoke snapshots such as `webhook_roundtrip_smoke`

Bad archive candidates:

- the currently selected production snapshot
- snapshots referenced by active or recently used job result views
- scoped/team snapshots that represent a distinct reusable acquisition boundary

## Final Hosted Gates

Before live user traffic:

```bash
PYTHONPATH=src ./.venv/bin/python scripts/apify_webhook_preflight.py --mode hosted
curl -fsS https://api.111874.xyz/health
curl -fsS https://api.111874.xyz/api/runtime/health
curl -fsS https://api.111874.xyz/api/providers/health
```

Expected:

- runtime is `production`
- provider mode is `live`
- control plane is `postgres_only + shared_memory`
- hosted Apify webhook route returns `202` for the preflight probe
- no disk-backed SQLite file is authoritative

## April 2026 Run Checkpoint

Current staged state from the April 30 migration pass:

- current code has been synced only to a staging repo path on ECS; the active systemd service still runs old code from `/opt/sourcing-ai-agent`
- selected snapshot rsync completed from `runtime/deployment/ecs_asset_rsync_all_files_20260430T015713Z.txt`; no remote snapshot deletion was performed
- the manifest files were copied to `/srv/sourcing-ai-agent/runtime/deployment/` on ECS for audit/replay
- read-only remote validation found `0` missing selected snapshot directories or unreadable candidate counts
- high-value selected snapshots present on ECS: OpenAI `20260430T090520`, Google `20260428T011339`, Meta `20260427T203601`, Lovable `20260426T193540`, Mistral AI `20260426T163408`, Perplexity `20260424T184223`, Windsurf `20260427T131256`
- one non-blocking strict-count mismatch remains: `humansand/20260414T162042` manifest expected `25`, `candidate_documents.json` observed `26`
- ECS still returns `404` for `/api/providers/apify/webhook` because the active service has not been replaced with current code

Do not use this checkpoint as permission to switch traffic. The next operator still needs to run the current-code replacement gate, production env audit, hosted webhook preflight, registry/profile backfills, and high-value serving-view audits.
