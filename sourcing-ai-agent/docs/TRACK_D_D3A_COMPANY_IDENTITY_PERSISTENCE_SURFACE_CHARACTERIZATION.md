# Track D D3a — Company identity persistence surface characterization

> Status: Author characterization-only batch (2026-07-14). Fresh pinned non-author review is required after the
> enclosing commit; no review verdict is claimed here. This batch changes no product code, migration, API, frontend,
> provider/model path, or live behavior, and it does not activate or complete D3.

## 1. Outcome and boundary

D3a records the current company-identity file persistence surface before any PG owner, precedence cutover, backfill,
or migration bridge is implemented. The mechanically frozen baseline is:

- `company_identity_registry.json`: **2 physical writers / 3 refresh callers / 1 upsert caller**;
- `identity.json`: **5 production/operations snapshot writers** plus **3 scripted/seed fixture writers**;
- cached-registry read chain: **1 physical file reader → 1 cached-record normalizer → 2 direct consumers → semantic
  discovery/alias entrypoints**;
- seed-catalog live-input chain: **1 runtime/bundled file reader → refresh metadata + 1 seed-record normalizer →
  refresh/upsert/discovery**;
- the shared `asset_paths.load_company_snapshot_identity` reader: **19 calls / 8 files**;
- its fallback provenance: **6 empty payloads + 11 `latest_snapshot.json.company_identity` payloads + 1 baseline
  payload + 1 no-fallback call**;
- loader-recognized fallback copies: **3** root `manifest.json.company_identity` production write sites and **13+3**
  root `candidate_documents.json.snapshot.company_identity` production/operations + fixture write sites;
- `latest_snapshot.json.company_identity`: **7 production/operations + 5 fixture/scripted direct write sites**, plus
  one generic bundle restore that can overwrite any runtime-relative path;
- generic whole-snapshot materializers: production bundle restore, production hot-cache tree mirror, and fixture
  copytree/symlink materialization;
- the separate deterministic resolver `connectors.resolve_company_identity`: **17 calls / 6 files**; its two
  `resolve_manual_company_identity` calls remain a distinct manual-override prepass and are not folded into `17/6`;
- every currently recognized direct Python lexical owner containing an `identity.json` literal, including semantic
  reader bypasses, registry scans, fixture/CLI readers, and the non-semantic existence probe.

This is a dated characterize-first inventory, not a physical PG schema selection. The controlling D3 §4a decision is
already unambiguous: the future current-state PG read model is authoritative. This record therefore classifies every
current file-backed decision path as one of **migration bridge then retire as authority**, **mirror-only**, or
**fixture-only**. It does not invent the table shape, workspace mapping, backfill, or bridge-deletion implementation.

## 2. Global cached-registry surface

The physical file writers are both in `company_registry.py`; no caller writes the registry file directly:

| Physical writer | Current input and effect | Direct callers | Future disposition |
|---|---|---|---|
| `refresh_company_identity_registry` | merges seed rows with a scan of company snapshot `identity.json` files, then rewrites the global cache | `asset_registration.sync_company_asset_registration`; `cloud_asset_import._post_import_runtime_refresh`; `organization_assets.warmup_existing_organization_assets` | migration bridge only; callers migrate to the PG owner, then scan + rewrite retire as decision authority |
| `upsert_company_identity_registry_entry` | merges seed + cached records with one caller-supplied identity payload and snapshot provenance, then rewrites the global cache | only `AcquisitionEngine._resolve_company` | migration bridge only; read/merge/rewrite retires after PG cutover, with any retained file output mirror-only |

The exact caller multiplicity is three refresh calls and one upsert call. `upsert_company_identity_registry_entry` uses
`snapshot_dir / identity.json` as provenance for the already supplied payload; that expression is not a snapshot read
or snapshot write. The registry remains global and file-backed today. This record does not promote it to a scoped,
versioned, generation-aware, or audited D3 authority.

### 2.1 Cached-registry read chain and semantic consumers

The sole physical file reader and every current direct semantic hop are:

| Hop | Direct callers | Target disposition |
|---|---|---|
| `load_company_identity_registry` | `_load_cached_company_identity_registry_records` | bridge reader, then retire as decision authority |
| `_load_cached_company_identity_registry_records` | `upsert_company_identity_registry_entry`; `_discover_local_company_identities` | bridge normalizer; the upsert branch retires with the writer, semantic discovery routes to PG |
| `_discover_local_company_identities` | `builtin_company_identity`; `_combined_company_identities` | PG-first; unmapped bridge hit is diagnostic-only `needs_human`, never authorization or paid-work suppression |
| `_combined_company_identities` | `infer_target_company_from_text`; `_company_text_aliases`; `_combined_company_aliases` | consume the PG-first discovery result; no independent fallback authority |
| `_combined_company_aliases` | `resolve_company_alias_key` | consume the PG-first discovery result; no independent fallback authority |

This chain makes the registry a live semantic input today, not merely an operations cache. Backfill and precedence must
therefore cover alias resolution and builtin/runtime merging, not only the two file write functions.

### 2.2 Seed-catalog live input

`load_company_identity_seed_catalog` reads the runtime override or bundled
`company_identity_seed_catalog.json`. It is called directly by `_load_seed_company_identity_records` and by
`refresh_company_identity_registry` for result metadata. The seed-record normalizer then feeds all three current
decision paths: registry refresh, registry upsert, and `_discover_local_company_identities`.

The catalog is therefore a live bootstrap identity input today. Under the accepted D3 §4a target it may remain only a
versioned bootstrap/backfill or migration input to the workspace-bound PG owner. After cutover, neither the runtime nor
bundled file may resolve a live identity, authorize asset reuse, clear the gate, or suppress paid work. A PG miss plus a
catalog hit is diagnostic-only `needs_human` until the row is explicitly imported; the file-authority branch is then
deleted after the compatibility window.

## 3. Snapshot writer inventory

### 3.1 Production and operations writers

| Writer | Current physical role | Registry coupling | Characterization-only disposition |
|---|---|---|---|
| `AcquisitionEngine._resolve_company` | writes the resolved identity to the canonical asset snapshot through `AssetLogger.write_json` | immediately calls the sole registry upsert | migrate the decision to the PG owner; retained snapshot file is mirror-only |
| `company_asset_supplement._resolve_or_create_company_snapshot` | creates `runtime_dir/company_assets/<key>/<snapshot>/identity.json` | no direct registry refresh in the function | consume the PG decision; retained snapshot file is mirror-only |
| `AssetBundleManager.hydrate_published_generation` | writes `identity.json` into a hydrated hot snapshot | the wider import-generation flow can refresh the registry separately; standalone hydrate does not | mirror-only |
| `authoritative_serving_repair._write_repair_snapshot` | writes identity into a serving-repair snapshot | no direct registry refresh in the function | consume the PG decision; retained repair snapshot is mirror-only |
| `candidate_artifacts._sync_snapshot_artifact_view_to_hot_cache` | link/copies canonical `identity.json` into the hot-cache snapshot | no registry write | mirror-only |

The first, second, and fourth rows currently originate or recopy identity without a PG authority check; their direct
decision role must retire. All five file outputs may remain only as artifact mirrors after the PG owner supplies the
workspace-bound decision.

### 3.2 Scripted and seed fixture writers

These three writers create test/scripted evidence and are not counted as production identity owners:

| Writer | Current role |
|---|---|
| `smoke_runtime_seed._write_candidate_documents` | scripted/smoke snapshot fixture |
| `smoke_runtime_seed._seed_google_large_baseline_real_asset` | scripted large-baseline seed fixture |
| `scripts/dev_scripted_openai_agent_delta.sh` | local scripted Lovable fixture |

They remain fixture-only and cannot seed a production decision row or satisfy a live gate. Fixture classification must
not be used as evidence that a production writer is safe to retain.

### 3.3 Generic whole-snapshot materializers

Direct filename searches do not see three generic paths that can create or expose loader-recognized identity files:

| Generic materializer | Current entrypoints/effect | Target disposition |
|---|---|---|
| `AssetBundleManager.restore_bundle` | `cloud_asset_import.import_cloud_assets` and `cli.main`; copies each manifest-selected `runtime_relative_path` with `shutil.copy2`, so an import can overwrite `identity.json`, root manifest/candidate documents, or the latest pointer without naming them in code | migration/quarantine bridge only; validate against the PG owner before visibility, retain only PG-consistent mirrors, and retire arbitrary file authority |
| `AcquisitionEngine._mirror_snapshot_to_hot_cache` → `artifact_cache.mirror_tree_link_first` | `_sync_snapshot_hot_cache`; recursively hardlinks, symlinks, or copies the complete snapshot tree | mirror-only; source identity must already match the PG decision and the hot tree can never become fallback authority |
| `scripts/seed_test_env_assets._materialize_snapshot_link` | fixture `main`; copytrees or symlinks a complete source snapshot before writing its fixture latest pointer | fixture-only; cannot create/backfill a production decision or satisfy a live gate |

These are classified in addition to, not included inside, the direct filename writer counts. Any later generic archive,
rename, copytree, link, or dispatcher path is a Scout-contract change even if the lexical filename ratchet remains green.

## 4. Central reader and bypass classification

### 4.1 Shared loader: 19 calls / 8 files

`load_company_snapshot_identity` currently tries, in order, `identity.json`, `manifest.json.company_identity`, and
`candidate_documents.json.snapshot.company_identity`, then a caller-supplied `company_identity` fallback. Its complete
direct call distribution is:

| File | Calls | Lexical owners |
|---|---:|---|
| `artifact_cache.py` | 2 | `collect_hot_cache_inventory`; `_repair_hot_cache_company_dir` |
| `asset_paths.py` | 2 | `build_company_snapshot_match_entry`; `resolve_company_snapshot_match_selection` |
| `asset_reuse_planning.py` | 1 | `_load_available_organization_asset_registry_records` |
| `asset_sync.py` | 1 | `AssetBundleManager._build_candidate_generation_manifest` |
| `authoritative_serving_repair.py` | 1 | `_write_repair_snapshot` |
| `candidate_artifacts.py` | 10 | materialize, filter-group, audit, cleanup, repair, rewrite, provider-map, compatibility-export, and two `_resolve_company_snapshot` calls |
| `organization_assets.py` | 1 | `discover_normalized_company_snapshots` |
| `runtime_rebuild.py` | 1 | `rebuild_runtime_company_asset_control_plane` |

Eighteen of the nineteen calls pass a caller fallback payload: six pass `{}`, eleven pass a latest-pointer payload
(`latest_payload` in ten calls and `dict(snapshot_entry.get("latest_payload") or {})` in one), and
`authoritative_serving_repair._write_repair_snapshot` passes `baseline_payload`. Only
`candidate_artifacts.materialize_company_candidate_view` supplies no fallback. The loader is a central compatibility
reader today, not a canonical identity decision reader. During migration it must be PG-first and may surface an unmapped
file/fallback hit only as diagnostic `needs_human`; after the deletion window it retires as an
authorization/readiness source.

### 4.2 Literal surface outside the shared loader

The AST guard classifies every currently recognized Python lexical owner with an `identity.json` literal:

| Classification | Current points | Future disposition |
|---|---|---|
| Registry scan/cache | `refresh_company_identity_registry`; `_latest_company_identity_payload`; registry upsert snapshot provenance | migration bridge; scan and file-backed decision authority retire |
| Production semantic-reader bypass | `AcquisitionEngine._resolve_snapshot_hydration_identity`; `snapshot_materializer.resolve_snapshot_company_identity`; `SourcingOrchestrator._open_public_serving_artifact_store_from_candidate_source` | route through PG; file read may remain mirror metadata only |
| Production restore-reader bypass | `orchestrator._restore_search_seed_snapshot_from_snapshot_dir` | route through PG; file read may remain mirror metadata only |
| Fixture reader bypass | `scripts/seed_test_env_assets._load_company_identity` | fixture-only; no product disposition inferred |
| Operations CLI reader bypass | `scripts/sync_latest_snapshot_from_registry._load_snapshot_identity` | migration-only bridge, then retire as decision authority |
| Non-semantic existence probe | `asset_paths._snapshot_dir_serving_preference_sort_key` | only ranks artifact presence; it does not consume an identity decision |

The five production/operations writers and two Python fixture writers from §3 also belong to the literal surface. The
third fixture writer is embedded Python inside a shell script and is frozen separately by exact source evidence.

### 4.3 Resolver surface stays separate

`resolve_company_identity` has 17 direct production calls across six files:

| File | Calls |
|---|---:|
| `acquisition.py` | 5 |
| `company_asset_supplement.py` | 1 |
| `connectors.py` | 1 |
| `excel_intake.py` | 8 |
| `orchestrator.py` | 1 |
| `request_normalization.py` | 1 |

The two direct manual-helper calls are `AcquisitionEngine._resolve_company` and
`request_normalization._build_target_company_identity_preview`. The call inside
`connectors.resolve_manual_company_identity` is already one of the resolver's `17/6`; the manual helper itself remains
a preceding decision step rather than a branch hidden inside `resolve_company_identity`.

### 4.4 Loader-recognized fallback-copy writers

`load_company_snapshot_identity` authorizes three ordered file shapes today. The `identity.json` population is in §3;
the other two shapes are not harmless metadata because they win when the earlier file is absent.

Root `manifest.json.company_identity` has three current production write sites:

| Writer | Current behavior | Target disposition |
|---|---|---|
| `AcquisitionEngine._normalize_snapshot` | rewrites the root manifest from the in-memory resolved identity | consume PG decision; mirror-only file output |
| `CompanyAssetSupplementManager.merge_candidates_into_snapshot` | rewrites the root manifest after supplement merge | consume PG decision; mirror-only file output |
| `CompanyAssetSupplementManager.rebuild_linkedin_stage_1_snapshot` | rewrites the root manifest during stage-1 rebuild | consume PG decision; mirror-only file output |

Root `candidate_documents.json.snapshot.company_identity` has thirteen production/operations physical write sites:

| Writer/site | Current fallback-copy behavior | Target disposition |
|---|---|---|
| `AcquisitionEngine._ensure_anthropic_local_candidate_documents` | originates an explicit identity copy | consume PG decision; mirror-only |
| `AcquisitionEngine._materialize_local_reuse_candidate_documents` | copies a selected source snapshot record | consume PG decision; mirror-only |
| `AcquisitionEngine._enrich_profiles._reuse_delta_baseline_if_available` | copies the baseline payload | preserve only a PG-consistent mirror |
| `AcquisitionEngine._enrich_profiles` investor branch | writes a root payload without an identity copy and can erase this fallback | cannot be relied on as authority; PG remains decisive |
| `AcquisitionEngine._enrich_profiles._write_candidate_documents` | rewrites from the chosen source snapshot | preserve only a PG-consistent mirror |
| `AcquisitionEngine._normalize_snapshot` | spreads the existing payload while updating candidates/evidence | preserve only a PG-consistent mirror |
| `authoritative_serving_repair._write_repair_snapshot` | writes an explicit identity copied from the baseline loader result | consume PG decision; mirror-only |
| `CompanyAssetSupplementManager.merge_candidates_into_snapshot` | writes an explicit identity into `snapshot` | consume PG decision; mirror-only |
| `CompanyAssetSupplementManager.rebuild_linkedin_stage_1_snapshot` | copies the roster/search-seed snapshot record | consume PG decision; mirror-only |
| `SourcingOrchestrator._apply_background_reconcile_snapshot_candidate_update` | preserves the existing full payload, or creates a shell with no identity | cannot be relied on as authority; PG remains decisive |
| `search_seed_registry.project_search_seed_snapshot_to_candidate_documents` | preserves existing payload or copies `SearchSeedSnapshot.to_record()` | consume PG decision; mirror-only |
| `SnapshotMaterializer.apply_company_roster_workers_to_snapshot` | writes an explicit identity copy | consume PG decision; mirror-only |
| `SnapshotMaterializer.apply_harvest_profile_workers_to_snapshot` | writes an explicit identity copy | consume PG decision; mirror-only |

Three fixture/benchmark write sites are classified separately:

| Writer | Classification |
|---|---|
| `smoke_runtime_seed._write_candidate_documents` | scripted fixture-only |
| `smoke_runtime_seed._seed_google_large_baseline_real_asset` | scripted fixture-only |
| `scripts/run_candidate_artifact_benchmark._write_runtime_snapshot` | benchmark fixture-only |

The accepted target is not to delete every artifact copy. It is to delete their ability to decide identity: a retained
manifest/candidate-document/identity file is a mirror of a workspace-bound PG decision. During migration, any PG miss
plus unmapped file or caller-fallback hit is diagnostic-only `needs_human`; it cannot authorize local-asset reuse, clear
the gate, or suppress paid work. Precedence and deletion tests must cover all nineteen loader calls, including the
eighteen caller-fallback paths.

### 4.5 `latest_snapshot.json.company_identity` provenance and writers

Eleven loader calls consume a latest-pointer-derived fallback, so the pointer is an identity-bearing bridge rather than
harmless selection metadata. Its current direct write population is twelve sites:

| Classification | Direct writers | Target disposition |
|---|---|---|
| Production/operations (**7**) | `AcquisitionEngine._write_latest_snapshot_pointer_to_dir`; `artifact_cache._repair_hot_cache_company_dir`; `AssetBundleManager.hydrate_published_generation`; `authoritative_serving_repair._write_repair_snapshot`; `candidate_artifacts._sync_snapshot_artifact_view_to_hot_cache`; `company_asset_supplement._resolve_or_create_company_snapshot`; `scripts/sync_latest_snapshot_from_registry.main` | first six become PG-consistent mirrors only; the sync CLI is a migration bridge and retires as decision authority |
| Fixture/scripted (**5**) | the two `smoke_runtime_seed` writers; `scripts/run_candidate_artifact_benchmark._write_runtime_snapshot`; `scripts/seed_test_env_assets.main`; embedded Python in `scripts/dev_scripted_openai_agent_delta.sh` | fixture-only; never seed or authorize a production identity |

`AssetBundleManager.restore_bundle` is an additional generic writer capable of restoring the pointer and all three
snapshot identity shapes. It is not double-counted as a direct filename site. A retained latest pointer may select a
physical mirror, but `company_identity` within it must be copied from and checked against PG; no pointer reader or writer
may originate, override, or repair the canonical decision independently.

## 5. Mutation sensitivity and deferred decisions

`tests/test_d3_company_identity_persistence_surface_characterization.py` fails when its recognized direct lexical
surface changes:

1. a third physical cached-registry writer appears, either registry writer disappears, or the `3 + 1` direct caller
   surface changes;
2. a known snapshot/fixture writer loses its physical sink, or a Python `identity.json` literal owner appears,
   disappears, or changes classification without an intentional update;
3. the sole registry reader chain or its semantic consumer graph changes;
4. one of the three root manifest identity writes or thirteen production/operations + three fixture root
   candidate-document writes loses its exact physical call;
5. the seed-catalog reader/normalizer chain changes without updating its bootstrap/migration disposition;
6. a generic bundle restore, production tree mirror, or fixture copytree/symlink materializer or entrypoint changes;
7. the shared loader changes from 19 calls across eight files, or its exact `6 + 11 + 1 + 1` fallback provenance
   changes, including duplicate calls owned by the same function;
8. one of the eleven direct Python or one scripted-shell latest-pointer identity write sites changes classification;
9. a registry scan, production semantic/restore reader, fixture/CLI reader, or existence-only probe enters or leaves the
   recognized direct literal surface without classification;
10. the `resolve_company_identity` `17/6` surface changes, or the two manual-prepass calls are silently conflated with it;
11. this document loses the PG-authoritative, bridge/mirror/retire, or non-live boundary.

D3a completes the dated D3 §4a / Plan §6#1 persistence Scout and per-point target-disposition record. Its AST/source
ratchets intentionally freeze known standard direct calls; they are not a proof that a future helper, alias, dynamic
path, `open`/rename, or generic dispatcher cannot evade lexical recognition. Any such write is a contract change and
must update the inventory rather than rely on a green test.

D3a does not choose the physical PG schema, perform backfill/workspace mapping, install PG-first precedence, stop a
writer, remove a bypass, change registry refresh behavior, add a verification action/command/table, or close any D3
live/manual/milestone gate. Those steps require later bounded implementation and scope-matched independent review.

## 6. Validation

Run from `sourcing-ai-agent/`:

```bash
PYTHONPATH=src .venv/bin/python -m pytest -q \
  tests/test_d3_company_identity_persistence_surface_characterization.py
.venv/bin/ruff check tests/test_d3_company_identity_persistence_surface_characterization.py
.venv/bin/ruff format --check tests/test_d3_company_identity_persistence_surface_characterization.py
PYTHONPATH=src .venv/bin/python -m py_compile \
  tests/test_d3_company_identity_persistence_surface_characterization.py
git diff --check -- \
  tests/test_d3_company_identity_persistence_surface_characterization.py \
  docs/TRACK_D_D3A_COMPANY_IDENTITY_PERSISTENCE_SURFACE_CHARACTERIZATION.md
```

Author evidence on 2026-07-14: the new characterization passed **11 tests**;
`test_company_registry.py` + `test_asset_paths.py` passed **24 tests**; the exact hydrate, authoritative-repair, and
runtime-rebuild adjacency nodes passed **3 tests**; focused Ruff check/format, `py_compile`, and the two-file whitespace
check passed. This is characterization evidence, not a formal independent-review verdict. No full
`tests/test_pipeline.py`, provider/model, live, W6, or manual validation belongs to this batch.
