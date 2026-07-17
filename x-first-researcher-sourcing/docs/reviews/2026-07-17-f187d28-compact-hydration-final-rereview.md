# Compact discovery and profile hydration pinned final re-review — `f187d28`

Date: 2026-07-17

## Evidence header

- Review type: non-author, adversarial, read-only Git-object final re-review.
- Repository: `/Users/changyuyi/projects/Sourcing AI Agent Dev`.
- Reviewed commit: `f187d28225775765b21bc069be7a4b0d202fd8c9`.
- Exact first parent: `b17e867bd00d632f8502a5994330ace69197297a`.
- Commit tree: `9d3e641dbc96195d268acba926105b79bd043ceb`.
- Parent tree: `93c8c0fcf60060b30965a79a8ce4823a4959263c`.
- Commit subject: `fix(x-first): close compact replay review gaps`.
- Inspection source: independent `git archive` extraction at
  `/private/tmp/xfirst-f187d28-review.sQSIwK` plus exact pinned `git diff`, `git show`, `git ls-tree`, and
  `git cat-file` objects. Mutable working-tree implementation and test files were not used as review evidence.
- Exact nine-file first-parent scope:
  - `x-first-researcher-sourcing/README.md`
  - `x-first-researcher-sourcing/contracts/x.grok.compact_discovery.result.v1.schema.json`
  - `x-first-researcher-sourcing/docs/GROK_COMPACT_DISCOVERY_AND_PROFILE_HYDRATION.md`
  - `x-first-researcher-sourcing/docs/reviews/2026-07-17-b17e867-compact-hydration-final-review.md`
  - `x-first-researcher-sourcing/src/x_first/compact_grok_discovery.py`
  - `x-first-researcher-sourcing/src/x_first/grok_operator_session_replay.py`
  - `x-first-researcher-sourcing/tests/grok_raw_session_fixture.py`
  - `x-first-researcher-sourcing/tests/test_compact_grok_discovery.py`
  - `x-first-researcher-sourcing/tests/test_grok_profile_hydration.py`
- Scope size: 9 files, 460 insertions, 32 deletions.
- Scoped binary-diff SHA-256: `87a536d13af5952033389a7554116dd69be93ef61433c44cf88fd2ad98d98154`.
- Scoped name/status SHA-256: `3db2433411c57260dc2b7ced5b986fb0921c0caa5c8d8b1fe98ae6034707c530`.
- Scoped numstat SHA-256: `eecb2d9d5e3dcde65f76a1214d5a2cf1df77f439dad09dcac04f50b6ede8e6be`.
- Commit-object payload SHA-256: `76d05175d75ac6f210d52e8e72ff5023e382ab73ba25d873aaf0c576d447a873`.
- Review boundary: no provider/model call, credential source, owner-private candidate artifact, or live X data was
  accessed. All probes used deterministic `.invalid`-style synthetic fixture identities.

## Scoped blob identities

| File | Git blob OID | SHA-256 of content |
|---|---|---|
| `README.md` | `cc89a3672b8877e856c6e3e720bcdf7de040476a` | `029f755952b60474a202af2be37f621ecfdd2f65dac6135716b6efca6e20a8af` |
| compact discovery schema | `e9b5d1f95e7b2aed9019e0a35da5e0871fbe7dd5` | `e99b2829a10ef6b89f7c9672d16e2ec9e57cb0459bd3c1e27aecf56e3bad940b` |
| workflow document | `7ff21208d50dfa5f628d23e908f20fc4b79990d4` | `eec4324137fad9ab9fe72970e08b8517e8b72147806ff3e15f8c9abbae0a2570` |
| prior pinned review | `dd9d21c8011309dd2ce34b7b4c11b4a423f0bc43` | `7db2045dd313c589740ba6a3c4695aaf8f198897da8b79dbd0f67bace32a9702` |
| compact discovery runtime | `f29f62c41e4c07c98476d94abfa14c3b7d255b5a` | `ff3418bbd985c587b7f5f3654090128cc13958655961bcb9eb8d4810ad37f049` |
| shared raw-session replay | `1c651123d12ef9cbfecd54c51d46e78c9be2dd1b` | `abf06abeb46bb52be20a2712722b09339602b75d7e23601766587f8a8cf6dc94` |
| synthetic raw-session fixture | `a14e80bcec8b2008b626f50f22122b73115727b1` | `265f13bafae356a979e76429cb19ec168e5ae875d3d1be389d981c1cd7d08412` |
| compact discovery tests | `f173ba49ae62a1b324b638da54c7ff93fcf402c8` | `f24cbdd7e98fc3535430ac76cef3eec92cec4f039f09d8e20e754c7c6745e4e2` |
| profile hydration tests | `3ed2d4235fee7c31cbfdd236599f96ed4baad508` | `061e3ff059ed5224f2b77e4ed8b505238066ef8a9ce01bf3a9d611dceb311605` |

## Finding totals

| Severity | Count |
|---|---:|
| P0 | 0 |
| P1 | 1 |
| P2 | 0 |
| P3 | 2 |

## Findings

### P1-1 — new adjacent blocker after the prior P1-2 author-path repair — unresolved sidecars are not replay-bound at the hydration handoff

The fixed-forward generator now does the important identity operation correctly. A same-handle provisional group is
removed from stable identity aggregation and routed to a one-or-more-candidate sidecar
(`src/x_first/compact_grok_discovery.py:1347-1367`); stable axes, source references, history, origins, and lookup
identity are computed only from the stable group (`:1375-1452`), while the sidecar records hashes and provisional
origins (`:1458-1484`). Forward and reversed input-order probes produced byte-identical unions and summaries. The
stable lead was exactly equal to the stable-only merge, and the hydration tuple remained
`(platform:<id>, lookup handle, expected platform id, stable_platform_id)`. Thus the exact stable-lead contamination
counterexample from the prior review is closed in the builder.

The durable cross-phase contract is still open. The union validator checks that `input_shards` contain syntactically
valid projected-result hashes but receives no retained projections with which to replay those hashes
(`compact_grok_discovery.py:421-454`). A sidecar is optional, its candidate ids are reconciled only against the stable
leads currently in the union, its origins need only belong to any input shard, and its provisional lead/reference
hashes need only be well-formed SHA-256 strings (`:456-468,674-720`). It never recomputes those hashes or proves that a
sidecar exists for a provisional input removed during merge. The normal hydration entrypoint accepts a raw union
mapping after that same validator, ignores sidecars while deriving identities, and then hashes whatever union bytes it
was given as the new `discovery_union_sha256` (`src/x_first/grok_profile_hydration.py:355-404`). That digest seals the
already accepted value; it does not prove its derivation.

An independent probe started with two receipt-replayed pinned projections: one stable current/current lead and one
same-handle provisional historical/historical lead. The generated union had the correct single-candidate sidecar.
Each of the following mutations independently returned zero runtime validation errors, zero schema errors, and was
accepted by `build_profile_hydration_expectation_from_union` with the same stable hydration identity tuple:

1. remove the entire sidecar;
2. replace `provisional_lead_sha256s` with an unrelated valid digest;
3. replace `provisional_source_ref_sha256s` with an unrelated valid digest;
4. rebind `origin_shard_ids` from the provisional shard to the stable shard, which is merely another syntactically
   allowed input origin.

Removing the sidecar and adding the now-generator-dead
`same_handle_provisional_evidence_absorbed` conflict marker also passes runtime and schema validation because that
closed value remains accepted for any stable id (`compact_grok_discovery.py:100-103,608-612`). The result can therefore
erase or forge the only retained record of unresolved identity evidence and still become the normal hydration source
of truth. This contradicts the documented claim that the sidecar binds exact provisional lead/reference digests and
origins back to retained projected inputs until identity resolution
(`docs/GROK_COMPACT_DISCOVERY_AND_PROFILE_HYDRATION.md:146-152`).

Make the normal hydration handoff consume a replay-verified merge envelope rather than an unproven union mapping.
That envelope should retain or resolve the exact projected shard envelopes, deterministically rerun the merge, and
bind the resulting union, sidecar set/count, provisional lead/reference digests, candidates, origins, and merge
summary. A self-reported input hash inside the same mutable union is insufficient. Remove the stale absorbed-evidence
marker or bind it to the replayed sidecar state. Add negative consumer regressions for sidecar deletion, lead/ref hash
replacement, allowed-but-wrong origin rebinding, candidate-id changes, and marker substitution; every case must fail
before a hydration expectation is emitted.

### P3-1 — residual — candidate-free aggregate performance derivation is still not replayable

This fixed-forward changes raw-session order, assistant resource bounds, and identity sidecars; it adds no closed
aggregate schema, versioned generator, field-level source mapping, or formulas for the diagnostic aggregate described
in the prior review. Its hashes and arithmetic remain useful diagnostics, not a formal replayable performance claim.

### P3-2 — residual — six-file replay still proves shape conformance rather than Grok CLI binary identity

The workflow document continues to state that the six retained files do not contain CLI version or executable
identity, so the lane proves `x.grok.raw_session_shape.v1` conformance rather than provenance from the named binary
(`docs/GROK_COMPACT_DISCOVERY_AND_PROFILE_HYDRATION.md:129-133`). This remains an honestly documented, non-blocking
provenance boundary.

## Prior finding dispositions

| Prior finding at `b17e867` | Disposition at `f187d28` |
|---|---|
| P1-1: tool lifecycle may precede the sole user turn | Closed. Update index zero must be the unique user update; all later user updates are rejected. Independent compact and hydration probes separately moved tool start, completion, thought, and assistant rows before the user row and added a second user row. All ten cases failed with the stable lane-specific wrapper around `raw_session_user_turn_order_invalid`. |
| P1-2: unique stable-id absorption contaminates stable state/evidence | Exact contamination closed in deterministic merge: stable axes/source/history/origin and hydration identity remain equal to stable-only output in both input orders. Cross-phase preservation is not closed because sidecar deletion or rebinding is accepted; raised separately as P1-1 above. |
| P2-1: deeply nested assistant JSON escapes as `RecursionError` | Closed. Both compact and hydration return stable contract errors for depth-10,000 and over-50,000-node terminal strings. Valid progress objects containing braces/brackets inside strings, escaped quotes/backslashes, nested objects, and multiple object boundaries still select the one valid terminal in both lanes. |
| P3-1: aggregate derivation is not replayable | Residual unchanged. |
| P3-2: CLI executable identity is absent from retained six-file evidence | Residual unchanged and documented. |

## Validation

| Check | Pinned result |
|---|---|
| Compact discovery + profile hydration | 54/54 passed in 0.698s; previous baseline was 50 |
| Recall-pool regression | 31/31 passed in 1.897s |
| `PYTHONPATH=src python -m x_first.contracts` | exit 0; `errors=[]`; fixture precision 1.0 and recall 1.0 |
| Scoped Ruff on three runtime and three test/fixture modules | all checks passed with `--no-cache` |
| Compact and hydration result schemas | both passed duplicate-key-rejecting JSON parse |
| Exact nine-file first-parent `git diff --check` | passed |
| User-turn adversarial matrix | compact and hydration each rejected tool/completion/thought/assistant-before-user and multiple-user cases |
| Assistant resource matrix | both lanes returned typed depth and node-budget errors; valid string/escape/multiple-object boundaries passed |
| Stable-plus-provisional matrix | stable lead and hydration tuple remained stable-only and input-order invariant; generated sidecar digests/candidates/origins were exact |
| Sidecar mutation matrix | deletion, lead-digest replacement, ref-digest replacement, and allowed-origin rebinding were all incorrectly accepted by runtime, schema, and hydration consumer |

## Final verdict

NO-GO
