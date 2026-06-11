# Runtime Asset Retention Governance

> Status: active M0.6-M0.9 operator guidance. Use with `docs/DATA_ASSET_GOVERNANCE.md` and `docs/SERVICE_GRADE_ARCHITECTURE_PLAN.md`.

> 2026-06-11 revision note (owner-approved): the M0.9 standalone review-gated destructive apply described below is CANCELLED. The 10 cold bundles under `runtime/cold_bundles/m0_9_20260610_runtime_supersession/` are sha256-verified (`archive_verified: true`); their source dirs and the rest of aged `runtime/test_env` are reclaimed via the TTL prune mode of `scripts/apply_runtime_asset_retention_prune.py` (local-rebuildable policy: review artifact waived; dry-run default, PROTECTED_NAMES, infra-entry protection, and active-process check still enforced). The reviewed-manifest flow below remains REQUIRED for non-rebuildable assets (company_assets, PG state, sole-copy signoff evidence). See `SERVICE_GRADE_ARCHITECTURE_PLAN.md` "2026-06-11 Plan Revision".

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

Historical W6, nightly, pre-manual, pressure, signoff, closeout, smoke, review, rerun, contract, board-runtime, profile-contract, or scripted artifacts. These may be large and usually have superseded reruns, but they should only be cold-archived after signoff supersession proof and review.

`review_phase_milestone_artifact`

Phase or milestone-specific artifacts. These need milestone supersession proof before archive because they often explain a contract transition or incident.

`review_current_alias_or_latest_artifact`

Directories with `current` or `latest` in the name. These are not first-pass archive candidates. Resolve the alias target and confirm a replacement pointer before any movement.

`review_cold_archive_candidate`

Generic historical runtime/output directory matching the requested markers after excluding current/latest aliases, phase/milestone evidence, and signoff/pressure-like run evidence. It still needs cold-copy/hash proof, rebuild/projection rehearsal if relevant, and Independent Review Gate before apply.

## Reviewed Prune Boundary

There is intentionally no `--apply` mode in `scripts/audit_runtime_asset_retention.py`. The apply-capable path is separate:

```sh
PYTHONPATH=src .venv/bin/python scripts/apply_runtime_asset_retention_prune.py \
  --workspace-root . \
  --audit-json runtime/asset_governance/<run_id>/runtime_retention_audit.json \
  --min-age-days 10 \
  --allowed-retention-class review_cold_archive_candidate \
  --review-artifact runtime/reviews/<review>.md \
  --review-title "<matching independent review title>" \
  --review-required-file src/sourcing_agent/runtime_asset_retention_prune.py \
  --review-required-file scripts/apply_runtime_asset_retention_prune.py \
  --review-required-file docs/RUNTIME_ASSET_RETENTION_GOVERNANCE.md \
  --accepted-retention-exception "<user/founder accepted exception, if no cold copy is available>" \
  --reuse-index-effect none_runtime_output_only \
  --output-json runtime/asset_governance/<run_id>/prune_plan.json \
  --output-md runtime/asset_governance/<run_id>/prune_plan.md
```

The plan is still non-mutating. A real prune must read that explicit plan and include both `--apply` and `--reviewed`:

```sh
PYTHONPATH=src .venv/bin/python scripts/apply_runtime_asset_retention_prune.py \
  --workspace-root . \
  --plan-json runtime/asset_governance/<run_id>/prune_plan.json \
  --apply \
  --reviewed \
  --output-json runtime/asset_governance/<run_id>/prune_apply.json \
  --output-md runtime/asset_governance/<run_id>/prune_apply.md \
  --strict
```

This tool only removes local historical runtime/output copies. It is not a company-asset archive tool and does not mutate registry pointers, projections, provider cache entries, or PG state.

The reviewed prune path must:

- Read an explicit reviewed manifest, not rescan live directories and decide on the fly.
- Reject parent traversal, exact root deletion, wrong action values, protected names, source-root mismatch, raw target symlinks, raw ancestor symlinks, forged `current`/`latest` aliases based on every path component, and stale manifests whose current target existence, size, file count, directory count, or normalized latest mtime no longer exactly matches the reviewed plan.
- Perform an all-operation preflight before deleting anything. If any operation is missing, stale, symlinked, outside the allowed roots, or otherwise blocked, the whole apply returns `blocked` with zero removals.
- Refuse to operate when active runtime daemons are detected. This includes direct command matches plus runtime-owned `service_logs/*.pid` and `services/*/status.json` references, because worker daemon child command lines may not include the runtime path. Direct command-token matches are workspace-aware: a process whose cwd is inside the prune workspace blocks apply; an out-of-workspace cwd is ignored; an uninspectable cwd fails closed. Reference-file scanning is bounded but recursive where deletion risk exists: root-level `runtime/service_logs` and `runtime/services` are checked non-recursively, while each planned operation directory is checked recursively for nested `service_logs/*.pid` and `services/*/status.json`. A non-terminal `status.json` is not ignored just because its heartbeat is stale; if its PID is still present in `ps`, apply blocks. Only explicit terminal statuses such as stopped, completed, failed, or cancelled suppress a status reference.
- Preserve enough metadata to reconstruct the original local path, file count, byte count, latest mtime, review artifact, accepted retention exception, and reuse-index effect.
- Record whether the directory is only removed from the hot local workspace or also excluded from a normal reuse index. The default M0.6 effect is `none_runtime_output_only`.
- Run an Independent Review Gate before the first real apply, and require the stored review artifact to contain a `GO` verdict, matching review title, matching required scope files, required artifact path/hash tokens, and `docs/INDEPENDENT_REVIEW_GATE.md` evidence before `--apply --reviewed` can proceed.
- Require either a cold-copy manifest or an explicit accepted retention exception. The no-cold-copy path is only for local disk-pressure cleanup of rebuildable historical `review_cold_archive_candidate` runtime/output copies, not company assets, signoff artifacts, or phase/milestone artifacts.
- Keep no-cold-copy plans narrow with `--allowed-retention-class review_cold_archive_candidate`. `review_signoff_or_pressure_run_artifact` and `review_phase_milestone_artifact` require cold-copy or a future per-operation supersession/rebuildability proof contract before deletion.
- Treat the manifest retention class as advisory. Apply must reclassify paths from their names/markers and reject a forged cold label for signoff-like or phase-like evidence.
- Validate cold-copy manifests as real workspace-local JSON files. Generic cold-copy manifests may only support low-risk `review_cold_archive_candidate` cleanup. Privileged runtime artifacts such as `review_signoff_or_pressure_run_artifact` and `review_phase_milestone_artifact` require `runtime_asset_supersession_cold_bundle_manifest_v1`, not just an arbitrary per-entry proof/hash field.
- Keep `runtime/company_assets`, `secrets`, and `object_store` protected. Company snapshot consolidation remains governed by W5 asset consolidation tools.

## Supersession Review Layer

After `review_cold_archive_candidate` directories have been handled, the remaining large directories are usually `review_signoff_or_pressure_run_artifact` and `review_phase_milestone_artifact`. These require a stricter review layer before any archive/apply work:

```sh
PYTHONPATH=src .venv/bin/python scripts/audit_runtime_asset_supersession.py \
  --retention-json runtime/asset_governance/<run_id>/runtime_retention_audit.json \
  --output-json runtime/asset_governance/<run_id>/runtime_supersession_audit.json \
  --output-md runtime/asset_governance/<run_id>/runtime_supersession_audit.md
```

The supersession audit is read-only and reports `deletion_allowed=false`. It only accepts `runtime_asset_retention_audit_v1` input reports with strict boolean `read_only=true` and `deletion_allowed=false`. It derives the effective scan root from each path, rejects absolute paths outside `workspace_root`, rejects `.` or `..` traversal components, only permits child paths under `runtime/test_env` and `output`, and skips any row whose source `scan_root` disagrees with the path-derived root. It groups high-proof artifacts by normalized run family within the same effective scan root and marks older family members as `supersession_review_candidate` only when a newer family reference exists. `output/*` summaries must not be treated as automatic supersession proof for `runtime/test_env/*` directories. This is not deletion approval. A future apply still needs family-reference proof, cold-copy/hash proof, Independent Review Gate, and a separate reviewed archive/exclusion operation.

## Supersession Cold Manifest Layer

After a supersession audit identifies review candidates, build a bounded cold-storage manifest before copying or pruning anything:

```sh
PYTHONPATH=src .venv/bin/python scripts/build_runtime_asset_supersession_cold_manifest.py \
  --supersession-json runtime/asset_governance/<run_id>/runtime_supersession_audit.json \
  --workspace-root . \
  --max-entries 10 \
  --target-gib 12 \
  --output-json runtime/asset_governance/<run_id>/runtime_supersession_cold_manifest.json \
  --output-md runtime/asset_governance/<run_id>/runtime_supersession_cold_manifest.md
```

The manifest contract is `runtime_asset_supersession_cold_manifest_v1`. It is still non-mutating and reports `read_only=true`, `deletion_allowed=false`. It only accepts `runtime_asset_supersession_audit_v1` input with strict `read_only=true`, `deletion_allowed=false`, and `source_contract_valid=true`; rejects path traversal, exact scan-root targets, paths outside `runtime/test_env` or `output`, source-root mismatch, missing or cross-root family references, stale size/file/directory/latest-mtime metadata, symlinks, empty artifacts, and truncated file manifests.

The manifest has two readiness levels:

- `ready_for_cold_storage_review`: the artifact has a complete file list and per-file sha256 evidence. This still does not allow deletion; it only allows the next cold-copy verification review.
- `planning_ready_needs_hash_manifest`: generated with `--no-file-sha256` or otherwise lacking complete hash proof. This is useful for selecting a first bounded batch but cannot support local removal or archive apply.

No supersession cold manifest can directly remove files. The next gates are manual family-reference confirmation, cold copy to external/offline storage, hash or bundle verification, Independent Review Gate, then a separate reviewed archive/remove operation.

## Supersession Cold Bundle Layer

When no external/offline volume is mounted, a bounded supersession batch can be converted into local compressed cold bundles before any prune apply:

```sh
PYTHONPATH=src .venv/bin/python scripts/build_runtime_asset_supersession_cold_bundles.py \
  --proof-manifest runtime/asset_governance/<run_id>/runtime_supersession_cold_manifest_proof.json \
  --workspace-root . \
  --archive-root runtime/cold_bundles/<run_id> \
  --create-archives \
  --compression zstd \
  --output-json runtime/asset_governance/<run_id>/runtime_supersession_cold_bundle_manifest.json \
  --output-md runtime/asset_governance/<run_id>/runtime_supersession_cold_bundle_manifest.md
```

The bundle manifest contract is `runtime_asset_supersession_cold_bundle_manifest_v1`. It may create archive files, but it still reports `deletion_allowed=false`; source removal is not part of this command. It only accepts a proof-ready `runtime_asset_supersession_cold_manifest_v1`, revalidates source path safety and metadata, writes one tar archive per selected source artifact, records archive size and sha256, and verifies archive integrity. Existing runtime prune apply treats this bundle manifest as a cold-copy manifest only when the bundle status is `ready_for_prune_cold_copy_manifest`, each selected entry is `archive_ready_for_prune_cold_copy`, archive files exist inside the workspace but outside `runtime/test_env`, outside `output`, and outside every source directory in the prune operation set, archive raw paths have no symlink component, archive size/sha256 still match at apply time, and the tar structure is readable and contains the source path prefix.

Bundle-derived prune plans also bind review evidence to the exact destructive scope. The plan records `source_bundle_manifest_path`, `source_bundle_manifest_sha256`, `review_plan_artifact`, and `review_plan_scope_digest_sha256`. Apply recomputes the scope digest from the current plan and the referenced plan artifact, then requires the Independent Review artifact to contain the bundle manifest path+sha, prune plan path+scope digest, and apply-derived mandatory tokens for the bundle contract, bundle sha, and plan scope digest. These mandatory tokens are derived during apply from the current plan instead of trusted from plan-supplied `review_required_tokens`, so a code-only `GO` artifact or a weakened plan artifact is rejected.

This local compressed-bundle path is a disk-pressure compromise for superseded runtime/output artifacts. It is not allowed for canonical company assets, PG state, provider cache mutation, registry pointers, or current/latest aliases. A real source removal still requires a separate prune plan, a matching `GO` Independent Review Gate artifact, dry-run evidence, `--apply --reviewed`, and active runtime process checks.

After the bundle manifest reaches `ready_for_prune_cold_copy_manifest`, build the destructive prune plan from that exact bundle manifest, not from the older retention audit. The `--output-json` path becomes the review-bound prune plan artifact for bundle-derived plans:

```sh
PYTHONPATH=src .venv/bin/python scripts/apply_runtime_asset_retention_prune.py \
  --cold-bundle-manifest runtime/asset_governance/<run_id>/runtime_supersession_cold_bundle_manifest.json \
  --workspace-root . \
  --review-artifact runtime/reviews/<go_review>.md \
  --review-title <review_title> \
  --review-required-file src/sourcing_agent/runtime_asset_retention_prune.py \
  --review-required-file src/sourcing_agent/runtime_asset_supersession_cold_bundle.py \
  --output-json runtime/asset_governance/<run_id>/runtime_supersession_cold_bundle_prune_plan.json \
  --output-md runtime/asset_governance/<run_id>/runtime_supersession_cold_bundle_prune_plan.md \
  --strict
```

The bundle-derived prune plan has the same `runtime_asset_retention_prune_v1` contract, but its `operations` are sourced only from verified bundle `archives`. It revalidates that each source directory still exists, is still a plain directory, remains under an allowed prune root, and still matches the bundle size/file/directory/latest-mtime metadata. Bundle summary counters must exactly match the actual archive list length, every archive path must be unique, and destructive apply requires the plan's recorded bundle manifest path and sha256 to still match the current cold-copy manifest. A supersession bundle manifest cannot be used from a generic/audit-derived prune plan; apply requires bundle-derived plan binding whenever `destructive_review_evidence.cold_copy_manifest` has `contract_version=runtime_asset_supersession_cold_bundle_manifest_v1`. Bundle validation reads only the contract-owned `archives` list; legacy `operations`, `entries`, `items`, or `files` fallback keys are accepted only for generic cold-copy manifests. Destructive apply now fails closed unless the plan itself reports `status=ready_for_review`.

## Current Evidence

The first local inventory evidence for Google/Reflection AI and historical W6/nightly/pre-manual artifacts lives under:

```text
runtime/asset_governance/m0_5_20260608_google_reflection_v2/
```

Key observations from that read-only run:

- `runtime/company_assets/google` is about 12GB and must be governed through company snapshot consolidation, not runtime retention.
- The first company snapshot cold archive manifest has only 6 manifest-ready Reflection AI snapshots, about 108MB. Google company snapshots remain blocked by unique identities or dependencies.
- Runtime/output retention is the larger opportunity: 341 matched historical directories total about 130.9GB, with about 128.1GB under `runtime/test_env`.
- Most large directories are W6/nightly/pre-manual/signoff artifacts. They need supersession proof, not immediate deletion.

The M0.6 closeout evidence for the apply-capable runtime prune path lives under:

```text
runtime/asset_governance/m0_6_20260609_runtime_test_env_closeout/
```

The first M0.6 scan found `runtime/test_env` as the pressure source: about 130GB under historical isolated test runtimes, while `runtime/company_assets` is about 20GB and must not be first-pass pruned through this path.

The first M0.6 reviewed apply completed successfully:

```text
runtime/asset_governance/m0_6_20260609_runtime_test_env_closeout/prune_apply.json
runtime/asset_governance/m0_6_20260609_runtime_test_env_closeout/prune_apply.md
runtime/reviews/20260609T162217Z_m0-6-runtime-test-env-prune-apply-cold-only-class-manifest-guard.md
```

Result: `35` `review_cold_archive_candidate` runtime/output directories removed, `14745489340` bytes reclaimed, zero failed operations, and zero active runtime processes at apply time. The apply did not touch `runtime/company_assets`, PG state, registry pointers, projections, provider cache entries, signoff/pressure artifacts, phase/milestone artifacts, or current/latest aliases. The remaining first-pass pressure is still mostly under protected or higher-proof classes: signoff/pressure run artifacts and phase/milestone artifacts require supersession proof plus cold-copy/supersession evidence before any future prune.

The M0.7 remaining-proof audit lives under:

```text
runtime/asset_governance/m0_7_20260610_runtime_remaining_proof_plan/
```

Post-apply inventory found no remaining plain cold candidates above the scan threshold. The remaining reviewable pressure is about `93.25GiB` of signoff/pressure artifacts and `18.87GiB` of phase/milestone artifacts. These should be processed through the supersession audit layer first, then cold-copy/reviewed apply only for manually approved superseded families.

The M0.8 supersession cold-manifest evidence lives under:

```text
runtime/asset_governance/m0_8_20260610_runtime_supersession_cold_manifest/
runtime/reviews/20260609T214634Z_m0-8-runtime-supersession-cold-manifest-final-proof-guards.md
```

The planning manifest selected the first bounded batch without hashes: `10` selected artifacts, `107` deferred artifacts, `10730637032` selected source bytes, `0` blockers, and `file_sha256_enabled=false`. It is useful only for batch sizing.

The proof manifest for the same bounded batch is `ready_for_cold_storage_review`: `10/10` selected artifacts proof-ready, `109784` files, `10730637032` bytes, `0` blockers, and `file_sha256_enabled=true`. This still does not permit deletion. It only allows the next manually approved cold-copy verification step; local removal still requires copied-and-verified external/offline storage plus another Independent Review Gate and a separate reviewed apply operation.

The M0.9 local compressed bundle evidence for that proof batch is:

```text
runtime/asset_governance/m0_8_20260610_runtime_supersession_cold_manifest/runtime_supersession_cold_bundle_manifest.json
runtime/asset_governance/m0_8_20260610_runtime_supersession_cold_manifest/runtime_supersession_cold_bundle_manifest.md
runtime/cold_bundles/m0_9_20260610_runtime_supersession/
runtime/reviews/20260610T042608Z_m0-9-runtime-supersession-cold-bundle-and-prune-gate-v3.md
```

Result so far: `10/10` archives created and verified, source bytes `10730637032`, archive bytes `1372774127`, estimated reclaim bytes `9357862905`, and `blocked_archive_count=0`. No source directories were removed by bundle creation. The next gate is a regenerated bundle-derived prune plan with review-bound artifact/digest evidence, another scoped Independent Review Gate for the exact plan/apply evidence, dry-run, active process check, then reviewed apply.

2026-06-10 current local disk pressure snapshot:

- `runtime`: about `148G`
- `runtime/test_env`: about `117G`
- `runtime/cold_bundles`: about `1.3G`
- `output`: about `5.3G`
- filesystem free space: about `63GiB`

The current M0.9 exact-prune apply is not complete, and there is no current acceptable `GO` review artifact for destructive apply. Historical artifacts are retained as evidence but must not be reused as apply prerequisites:

- `v8` returned `GO` before the destructive prune verifier enforced complete reviewer metadata. Under the current gate, it is missing required reasoning/service-tier metadata.
- `v9` returned `NO-GO` because the destructive prune verifier still accepted incomplete runner metadata.
- `v10` returned `NO-GO` because this runbook still pointed operators back to the invalid `v8` artifact.

The code-side metadata verifier has been hardened to reject review artifacts missing `reviewer_model`, `reviewer_reasoning_effort`, `reviewer_service_tier`, `prompt_path`, or `command`, and to reject default/auto/inherit reasoning or service-tier values for destructive prune apply. Before any destructive apply, generate a fresh current-scope Independent Review artifact that returns `GO`, then regenerate the plan against that artifact and confirm the plan scope digest remains exactly `2fe946d01dec5e534861e5c7ca2cf2e635da15c1d8524cb0d35a901c01add512`.

## M0.9 Exact Next Commands

First obtain a fresh `GO` review artifact for the current exact scope. Use the returned `review_output` path only if the artifact verdict is `GO` and its metadata records `gpt-5.5`, `xhigh`, and `fast`:

```sh
PYTHONPATH=src .venv/bin/python scripts/run_independent_review_gate.py \
  --execute \
  --timeout-seconds 600 \
  --title m0-9-runtime-supersession-cold-bundle-exact-prune-apply \
  --files "src/sourcing_agent/runtime_asset_retention_prune.py src/sourcing_agent/runtime_asset_supersession_cold_bundle.py scripts/apply_runtime_asset_retention_prune.py tests/test_runtime_asset_retention_prune.py tests/test_runtime_asset_supersession_cold_bundle.py docs/RUNTIME_ASSET_RETENTION_GOVERNANCE.md runtime/asset_governance/m0_8_20260610_runtime_supersession_cold_manifest/runtime_supersession_cold_bundle_manifest.json runtime/asset_governance/m0_8_20260610_runtime_supersession_cold_manifest/runtime_supersession_cold_bundle_prune_plan.json" \
  --extra-context "Review the exact M0.9 destructive prune scope. Historical v8/v9/v10 artifacts are not acceptable apply prerequisites. Verify complete review metadata enforcement, bundle manifest sha binding, review_plan_scope_digest binding, plan artifact binding, dry-run/apply separation, active runtime process guard, and no --skip-process-check use for real apply."
```

Regenerate the bundle-derived prune plan with that fresh `GO` review artifact:

```sh
PYTHONPATH=src .venv/bin/python scripts/apply_runtime_asset_retention_prune.py \
  --cold-bundle-manifest runtime/asset_governance/m0_8_20260610_runtime_supersession_cold_manifest/runtime_supersession_cold_bundle_manifest.json \
  --workspace-root . \
  --review-artifact <fresh_go_review_artifact> \
  --review-title <fresh_go_review_title> \
  --review-required-file src/sourcing_agent/runtime_asset_retention_prune.py \
  --review-required-file src/sourcing_agent/runtime_asset_supersession_cold_bundle.py \
  --review-required-file scripts/apply_runtime_asset_retention_prune.py \
  --review-required-file tests/test_runtime_asset_retention_prune.py \
  --review-required-file tests/test_runtime_asset_supersession_cold_bundle.py \
  --review-required-file docs/RUNTIME_ASSET_RETENTION_GOVERNANCE.md \
  --output-json runtime/asset_governance/m0_8_20260610_runtime_supersession_cold_manifest/runtime_supersession_cold_bundle_prune_plan.json \
  --output-md runtime/asset_governance/m0_8_20260610_runtime_supersession_cold_manifest/runtime_supersession_cold_bundle_prune_plan.md \
  --strict
```

Inspect the regenerated plan and confirm both the bundle sha and scope digest:

```sh
python3 - <<'PY'
import json
from pathlib import Path

plan = json.loads(Path("runtime/asset_governance/m0_8_20260610_runtime_supersession_cold_manifest/runtime_supersession_cold_bundle_prune_plan.json").read_text())
evidence = plan.get("destructive_review_evidence") or {}
summary = plan.get("summary") or {}
print("status=", plan.get("status"))
print("summary.candidate_count=", summary.get("candidate_count"))
print("summary.planned_bytes_to_free=", summary.get("planned_bytes_to_free"))
print("source_bundle_manifest_sha256=", plan.get("source_bundle_manifest_sha256"))
print("review_plan_scope_digest_sha256=", evidence.get("review_plan_scope_digest_sha256"))
PY
```

Expected:

```text
status= ready_for_review
summary.candidate_count= 10
summary.planned_bytes_to_free= 10730637032
source_bundle_manifest_sha256= 24bc4fa4a04b2ad5b7d004c0f13226d911cbf6f8290bc890d1ab3a954a81480f
review_plan_scope_digest_sha256= 2fe946d01dec5e534861e5c7ca2cf2e635da15c1d8524cb0d35a901c01add512
```

Run a non-mutating dry-run from the regenerated plan:

```sh
PYTHONPATH=src .venv/bin/python scripts/apply_runtime_asset_retention_prune.py \
  --workspace-root . \
  --plan-json runtime/asset_governance/m0_8_20260610_runtime_supersession_cold_manifest/runtime_supersession_cold_bundle_prune_plan.json \
  --output-json runtime/asset_governance/m0_8_20260610_runtime_supersession_cold_manifest/runtime_supersession_cold_bundle_prune_dry_run.json \
  --output-md runtime/asset_governance/m0_8_20260610_runtime_supersession_cold_manifest/runtime_supersession_cold_bundle_prune_dry_run.md \
  --strict
```

If the dry-run is ready and the user accepts a short local dev interruption, stop local dev runtime before apply:

```sh
bash ./scripts/dev_stop.sh --runtime-dir runtime --api-port 8765 --frontend-port 4173
```

Then run the reviewed destructive apply:

```sh
PYTHONPATH=src .venv/bin/python scripts/apply_runtime_asset_retention_prune.py \
  --workspace-root . \
  --plan-json runtime/asset_governance/m0_8_20260610_runtime_supersession_cold_manifest/runtime_supersession_cold_bundle_prune_plan.json \
  --apply \
  --reviewed \
  --output-json runtime/asset_governance/m0_8_20260610_runtime_supersession_cold_manifest/runtime_supersession_cold_bundle_prune_apply.json \
  --output-md runtime/asset_governance/m0_8_20260610_runtime_supersession_cold_manifest/runtime_supersession_cold_bundle_prune_apply.md \
  --strict
```

Post-apply validation:

```sh
df -h .
du -sh runtime runtime/test_env runtime/cold_bundles output 2>/dev/null
python3 - <<'PY'
import json
from pathlib import Path

path = Path("runtime/asset_governance/m0_8_20260610_runtime_supersession_cold_manifest/runtime_supersession_cold_bundle_prune_apply.json")
data = json.loads(path.read_text())
summary = data.get("summary") or {}
print("status=", data.get("status"))
print("summary.removed_count=", summary.get("removed_count"))
print("summary.removed_bytes=", summary.get("removed_bytes"))
print("summary.failed_count=", summary.get("failed_count"))
print("blocked_reason_count=", len(data.get("blockers") or []))
PY
```

If the plan digest changes, the archive hash changes, any source metadata changes, an active process is detected, or the review tokens no longer match, stop and regenerate the evidence/review instead of weakening the guard.
