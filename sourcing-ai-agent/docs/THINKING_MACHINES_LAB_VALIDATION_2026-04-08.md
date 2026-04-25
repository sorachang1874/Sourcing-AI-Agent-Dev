# Thinking Machines Lab Validation 2026-04-08

> Status: Current first-party doc. Treat this file as active guidance, but keep it aligned with `docs/INDEX.md` and `PROGRESS.md` when runtime contracts change.


## Scope

This note captures a quick validation pass on the current Thinking Machines Lab asset pool and retrieval stack after the latest asset completion / daemon recovery work.

## Asset Snapshot

- Validated snapshot: `runtime/company_assets/thinkingmachineslab/20260407T181912`
- Normalized artifact summary:
  - `182` normalized candidates
  - `379` evidence records
  - `182` profiles with explicit full detail
  - `0` manual review backlog
  - `0` profile completion backlog
- SQLite still contains a broader pool:
  - `189` candidates for `target_company = Thinking Machines Lab`
  - `699` candidates across all companies

## Provider Health

`PYTHONPATH=src python3 -m sourcing_agent.cli test-model`

- model provider: `ready`
  - provider: `claude_relay_openai_compatible`
  - model: `claude-sonnet-4-6`
- semantic provider: `ready`
  - provider: `dashscope_semantic`
  - embedding model: `text-embedding-v4`
  - rerank model: `gte-rerank-v2`

This confirms the real semantic path is callable in the current environment.

## Data Quality Observations

- `170 / 182` normalized candidates have empty `team`
  - team-layer retrieval is currently weak for TML.
- `8` candidates have `role` containing `Investor at Thinking Machines Lab` but are still categorized as `employee`
  - these leak into employee retrieval.
- `138` SQLite candidates contain the boilerplate note `LinkedIn company roster baseline`
  - lexical and semantic matching can hit this note text instead of candidate-specific evidence.
- The normalized snapshot and SQLite retrieval pool are not identical
  - normalized former count is `23`
  - a structured former query over SQLite produced a pool of `41`
  - retrieval currently operates on the broader SQLite pool, not the curated normalized snapshot.

## Retrieval Validation

### 1. Structured former-member query

Request shape:

- `target_company=Thinking Machines Lab`
- `categories=['former_employee']`
- `employment_statuses=['former']`
- `retrieval_strategy='structured'`

Observed:

- structured pool count: `41`
- scored result count: `39`
- no semantic rerank involved

Conclusion:

- hard filters execute correctly
- but the retrieval base is broader than the normalized artifact set

### 2. Hybrid infrastructure / systems query

Request shape:

- `query='基础设施 系统'`
- `categories=['employee']`
- `employment_statuses=['current']`
- `retrieval_strategy='hybrid'`

Observed:

- structured pool count: `140`
- baseline lexical result count: `17`
- hybrid result count after semantic: `20`
- semantic hit count: `8`

Problems seen in top results:

- investor-like profiles can rank highly because they remain in the `employee` bucket
- semantic-only additions were low-value in this run
- boilerplate notes can contribute matches on generic tokens such as `platform`

Conclusion:

- hybrid path runs end-to-end
- current precision is limited by categorization and note contamination

### 3. Semantic multimodality query

Request shape:

- `query='multimodality'`
- `categories=['employee']`
- `employment_statuses=['current']`
- `retrieval_strategy='semantic'`

Observed:

- baseline lexical result count: `1`
- fused semantic result count: `2`
- top exact hit: `Alexander Kirillov`
- semantic added one extra related profile from a broader foundation-model signal

Conclusion:

- semantic rerank path is functional
- it can increase recall beyond exact lexical matches
- added recall still needs stronger precision controls

## Recommended Next Fixes

1. Make retrieval snapshot-scoped by default
   - retrieval should prefer the latest normalized snapshot or an explicitly selected snapshot, not the full historical SQLite pool.

2. Add role-to-category normalization before retrieval
   - examples: `Investor`, `Recruiting`, `Operations`, `Programs`.

3. Split or downweight boilerplate acquisition notes
   - keep provenance, but do not let generic runtime notes dominate lexical / semantic scoring.

4. Add a lightweight functional facet for TML members
   - examples: `research`, `engineering`, `ops`, `investor`, `founding`.

5. Re-run multi-layer retrieval validation after 1-3
   - especially `former`, `infra/systems`, `multimodal`, `research`, and `ops/recruiting` slices.

## Follow-up State

As of the latest rebuild on `2026-04-08`, fixes 1-4 above are now implemented:

- retrieval defaults to the latest company snapshot instead of the broader SQLite history pool
- investor / former / non-member normalization is applied before retrieval
- boilerplate roster notes are sanitized out of lexical / semantic matching
- functional facets and role buckets are derived into normalized artifacts and retrieval fields
- the canonical snapshot now exposes two views:
  - `canonical_merged`: folded historical-valid asset
  - `strict_roster_only`: narrower clean-member analysis view

Latest TML snapshot summary:

- snapshot: `runtime/company_assets/thinkingmachineslab/20260407T181912`
- `canonical_merged`
  - `189` candidates
  - `404` evidence
  - `4` manual review backlog
  - `3` profile completion backlog
- `strict_roster_only`
  - `148` candidates
  - `321` evidence
  - `0` manual review backlog
  - `3` profile completion backlog

Current high-signal strict-view buckets:

- `research: 38`
- `founding: 27`
- `engineering: 26`
- `leadership: 21`
- `ops: 12`
- `investor: 10`
- `infra_systems: 9`

## Strict-View Live Retrieval Check

另外做了一轮 `asset_view = strict_roster_only` 的 live retrieval spot-check：

- semantic provider: `dashscope_semantic`
- summary provider: deterministic
- result artifact:
  - `runtime/live_tests/tml_strict_asset_view_validation_20260408/summary.json`

### 1. ops / operations / chief of staff / programs

Observed:

- total matches: `22`
- semantic hits: `10`
- top results:
  - `Anthony Bertolli`
  - `Ruila M.`
  - `Hannah T.`
  - `Caroline Ricksen`
  - `Clare Birch`

Assessment:

- `ops` facet is being used in both lexical and semantic retrieval
- `programs` and `chief of staff` history can pull in adjacent operator profiles, which is acceptable for a broad ops slice

### 2. recruiting / talent acquisition

Observed:

- total matches: `8`
- semantic hits: `8`
- top results:
  - `Hannah T.`
  - `Ruila M.`
  - `Meredith Alvin`
  - `George Rider`

Assessment:

- recruiting slice is mostly clean
- one residual false-positive style result still appeared downstream because `recruiting` leaked through broader notes / facet context
- manual review queue was `1`, so this slice is improved but not fully precision-locked

### 3. infrastructure / systems / platform / gpu

Observed:

- total matches: `21`
- semantic hits: `12`
- top results:
  - `Shenxiu Liu`
  - `Daniel Xu`
  - `Lilian Weng`
  - `Aurick Qiao`
  - `Long Lian`

Assessment:

- `infra_systems` facet is active and useful
- current results mix pure infra engineers with research / founding profiles that carry strong systems history
- this is good recall, but a stricter infra-only slice may need a harder bucket filter later

### 4. multimodal / vision-language

Observed:

- total matches: `6`
- semantic hits: `6`
- top results:
  - `Alexander Kirillov`
  - `Andrea Madotto`
  - `Alexander H Liu`

Assessment:

- multimodal slice is working end-to-end
- top result quality is strong
- lower-ranked results still show some broad semantic spillover, so multimodal may benefit from a future `must_have_keywords` or facet-only constraint mode

## Hard Facet Validation

After adding `must_have_facet / must_have_facets` as a true structured filter, another TML live check was run:

- result artifact:
  - `runtime/live_tests/tml_strict_asset_view_hard_facet_validation_20260408/summary.json`

### 1. ops with `must_have_facet=ops`

Observed:

- total matches: `17` (down from `22`)
- top results:
  - `Anthony Bertolli`
  - `Hannah T.`
  - `Ruila M.`

Assessment:

- broad ops slice is still intentionally wide
- but pure non-ops spillover is lower because only candidates with a real ops facet now survive the hard filter

### 2. recruiting with `must_have_facet=recruiting`

Observed:

- total matches: `4` (down from `8`)
- top results:
  - `Hannah T.`
  - `Ruila M.`
  - `Meredith Alvin`
  - `George Rider`
- manual review queue: `0`

Assessment:

- this materially improved precision
- the earlier recruiting leak through generic notes is gone

### 3. infrastructure with `must_have_facet=infra_systems`

Observed:

- total matches: `16` (down from `21`)
- top results:
  - `Shenxiu Liu`
  - `Long Lian`
  - `Horace He`
  - `Lilian Weng`
  - `Jenya L.`

Assessment:

- infra-only slice is narrower and cleaner
- however it still includes founding / leadership profiles that legitimately carry an infra facet
- if a future workflow needs “IC infra only”, a secondary hard filter on primary role bucket will likely help

### 4. multimodal with `must_have_facet=multimodal`

Observed:

- total matches: `2` (down from `6`)
- top results:
  - `Alexander Kirillov`
  - `Damir Sartayev`

Assessment:

- this is the clearest improvement from hard facet filtering
- the multimodal slice is now much closer to a true specialty subset instead of a broad semantic neighborhood

## Primary Role Bucket Validation

After adding `must_have_primary_role_bucket / must_have_primary_role_buckets` as an exact hard filter on `derive_candidate_role_bucket(...)`, another strict-view TML check was run:

- result artifact:
  - `runtime/live_tests/tml_strict_primary_role_bucket_validation_20260408/summary.json`

### infrastructure with `must_have_facet=infra_systems` plus `must_have_primary_role_bucket=infra_systems`

Observed:

- hard-facet-only baseline:
  - total matches: `16`
  - semantic hits: `9`
  - manual review queue: `1`
  - top results included:
    - `Shenxiu Liu`
    - `Long Lian`
    - `Horace He`
    - `Lilian Weng`
    - `Jenya L.`
- with primary role bucket:
  - total matches: `9`
  - semantic hits: `5`
  - manual review queue: `0`
  - top results:
    - `Shenxiu Liu`
    - `Long Lian`
    - `Jenya L.`
    - `Daniel Xu`
    - `Tianle Li`

Assessment:

- this is the intended precision improvement for “IC infra only” style slicing
- founding / leadership profiles with secondary infra history are removed
- the remaining top set is entirely `role_bucket = infra_systems`
- this should be the default query shape whenever the operator wants true primary-function infrastructure members rather than broader systems-adjacent leadership

### post-tightening check: remove `notes` from primary-bucket ranking

After tightening lexical + semantic ranking so that `notes` no longer participates in primary-role-bucket precision queries, the same strict-view infra request was re-run:

- result artifact:
  - `runtime/live_tests/tml_strict_primary_role_bucket_validation_20260408/infra_systems_primary_role_bucket_post_notes_tightening.json`

Observed:

- total matches stayed at `9`
- manual review queue stayed at `0`
- semantic hits dropped from `5` to `3`
- top matched fields were now limited to:
  - `derived_facets`
  - `focus_areas`
  - `work_history`
- `notes` no longer appeared in top-match `matched_fields`
- `Tianle Li` no longer relied on `notes` to score into the slice

Assessment:

- this preserves the primary-bucket precision gain while removing residual note-driven boost
- for high-precision structured slices, `notes` should remain explanatory context rather than a ranking surface
