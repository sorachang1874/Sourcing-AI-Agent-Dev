# Phase C — Data-Layer Consolidation Design (v1, 2026-07-22)

> Status: Current design (partially executed 2026-07-22: C1.2/C2/C3/C1.4/C4 done, see workspace PROGRESS.md). Owner: operator.

Owner: takeover session (post-Kimi). Scope: OpenAI identity merge, authoritative-flag
repair, runtime-root/pointer unification, canonical completeness checking. All facts
below are from read-only audits (queries + file reads); nothing in this document has
been executed against PG or the asset tree unless a step is explicitly marked DONE.

Audit artifacts: operator memory `artifacts-intake-20260721/neutralization_20260721.md`;
scratchpad `phaseC/REPORT.md`, `phaseC/dup_pairs_full.json`, `phaseC/runtime_root_pointer_map.md`.

## C1. OpenAI identity merge (40 duplicate people, three id generations)

Facts. Each of the 40 supp40 people carries three deterministic ids:

| generation | derivation | where it lives |
|---|---|---|
| old 16-hex | `sha1(linkedin_url)[:16]` (harvest_connectors.py:3421) | ONLY snapshot 20260720T104157 `candidate_documents*.json` + acquisition_shard_bundles; carries the fetched profile payload (`profile_fetched=true`) |
| seed 12-hex | `sha1(name_text)[:12]` (v1 seed placeholder; minting script gone) | v1 seeds/layer map/grok+luna v1 outputs/`account_resolution_conflicts.json`/CSV v1 |
| new 16-hex | `sha1(tok(company)\|tok(name)\|profile_url)[:16]` (seed_discovery.py:3350) | everything authoritative: all PG tables (candidates/evidence/membership/materialization/projections/job_results/manual_review), snapshot 20260720T041551 docs, supp40 seeds + judge v2, CSV v2 |

The canonical merged view (3,177 docs) already de-duplicates: old ids are dropped at
materialization. PG contains zero old/12-hex rows. 16 X-handle collision groups: 14 are
the same person's 12-hex vs new-id twins (false positives — CSV v2 wrongly blanks the
handle with 账号解析冲突 on 14 rows); only `karen_li6` and `kevin_wang3290` are genuine
two-person collisions. Judge verdicts disagree old-chain-vs-new-chain for 27/40 (12-hex
side judged without source-bound profile context; supp40 side is source-bound).

Policy (recommended):

1. Survivor = new 16-hex id. No deletion, no rewriting of immutable snapshot/judge
   files. Snapshots and v1 outputs stay as history.
2. Commit an alias map `configs/identity/openai_identity_alias_map_v1.json`:
   40 rows × {name, linkedin_url, canonical_id(new16), aliases:{harvest_url_hash(old16),
   v1_seed_name_hash(12hex)}} — generated from the audited pair table, verified by
   recomputing all three hashes per row (all derivations are deterministic).
3. Judge truth: supp40 (source-bound) verdicts win where the chains disagree; recorded
   in the alias map as `verdict_source: "supp40"` — matches what CSV v2 already serves.
4. Rebuild `account_resolution_conflicts.json` down to the 2 genuine collisions;
   regenerate CSV v2 restoring the 14 wrongly-blanked X handles (local files only).
5. Profile-payload continuity: the old-id docs carry the fetched LinkedIn payloads.
   These are URL-keyed in `linkedin_profile_registry`/harvest_profiles, so the serving
   join is by URL and survives the id change; the completeness check (C4) asserts every
   canonical doc with a URL that has a fetched profile actually resolves one.

Explicitly rejected: deleting either id generation (destroys judge provenance or the
only fetched-profile-bearing docs); rewriting snapshot files in place (breaks shard
bundle/generation signatures).

## C2. Authoritative-flag repair (OpenAI)

Facts. 20260720T104157 = materialization generation sequence 6, merged view selected
["20260720T104157","20260720T041551"] (superset). 20260720T041551 = sequence 3, selected
only itself. The 2026-07-22 02:25–02:27 UTC crashed daemon tick recovered a stale Jul-19
job and promoted 041551 (seq 3) over 104157 (seq 6) — a generational REGRESSION — and
truncated the prod hot-cache `openai/asset_registry.json` to a single pointer entry.

Repair (PG ops to be displayed verbatim before execution):
`UPDATE organization_asset_registry SET authoritative=1 WHERE company_key='openai' AND snapshot_id='20260720T104157'`
and `... SET authoritative=0 ... snapshot_id='20260720T041551'` (guarded, 1 row each).
File-side pointer repair folds into C3. Consider a follow-up code fix: reconcile must
not demote a higher materialization_generation_sequence row (guard in the registry
update path).

## C3. Runtime-root and pointer unification

Facts (`phaseC/runtime_root_pointer_map.md`): three asset roots coexist — canonical
`runtime/company_assets` (stale, Apr–Jun), `runtime/hot_cache_company_assets` (holds the
ONLY copy of google/20260720T152139), `runtime/test_env_live/{company_assets,hot_cache_company_assets}`
(holds the real OpenAI 041551/104157 and TML 20260719T183049). anthropic/xai canonical
pointers dangle at `/home/sorachang/...` (another machine, April). Pointers and
`organization_asset_registry.source_path` store ABSOLUTE paths — the root cause of both
the drift and the source==destination hot-cache incident (fixed defensively in 23fb308).

Plan:
1. Single canonical root = `runtime/company_assets`. Copy (not move; verify then swap)
   the July live snapshots into it: openai/20260720T041551 + 20260720T104157 and
   thinkingmachineslab/20260719T183049 from test_env_live; google/20260720T152139 out of
   the hot cache (after its reconcile rebuild restores the 60 destroyed files).
2. Pointer contract change (code): `latest_snapshot.json` treats `snapshot_id` as the
   contract; `snapshot_dir` becomes provenance-only, resolution goes through
   `asset_paths` against the current runtime root (reader fallback already exists at
   asset_paths.py:450/562).
3. Rewrite all six labs' pointers to real snapshots in the canonical root; rewrite
   `organization_asset_registry.source_path` to canonical paths (shown before execution).
4. Only after C2+C3: daemon restart decision (with WORKER_RECOVERY_REMOTE_WAIT_ORPHAN_SECONDS=0
   until the alt-ref admission asymmetry is fixed in code).

## C4. Canonical completeness check (committed script — currently none exists for any lab)

`scripts/check_canonical_completeness.py`: for a company key, load the authoritative
merged view + registry + alias maps and assert: no linkedin_url+name duplicates; every
candidate id format-valid and alias-resolvable; fetched-profile URL join resolves;
projection membership counts match the registry row; latest_snapshot pointer resolves to
an existing directory with a manifest. Exit non-zero on violation; runs offline.
Wire into the curated CI lane once stable.

## Sequencing

C1.2 alias map + C4 script (pure committed files, no mutations) → C2 registry repair →
C3 snapshot relocation + pointer/registry rewrite → C1.4 conflicts/CSV regeneration →
daemon restart re-evaluation. Paid APIs remain untouched throughout (all quota walls
respected; every step is local).
