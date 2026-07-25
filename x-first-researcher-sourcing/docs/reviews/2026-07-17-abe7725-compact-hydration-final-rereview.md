# Compact discovery and profile hydration pinned final re-review — `abe7725`

Date: 2026-07-17

## Evidence header

- Review type: non-author, adversarial, read-only Git-object final re-review.
- Repository: `/Users/changyuyi/projects/Sourcing AI Agent Dev`.
- Reviewed commit: `abe77252435f64e4a5a4ea0116435ee25d673fd2`.
- Exact first parent: `41abc4e537c6f6be767f70a0ea8f4916518543d6`.
- Commit tree: `6b25c1176ded4d1afeb8dd4e949b5e6a33606f34`.
- Parent tree: `f4d87504607b2158412efceaab082925ca8759f2`.
- Commit subject: `fix(x-first): replay-bind compact hydration handoff`.
- Inspection source: detached pinned checkout at
  `/private/tmp/xfirst-abe7725-review.nl5b8F`, plus exact pinned `git diff`, `git show`, `git ls-tree`, and
  `git cat-file` objects. Mutable working-tree implementation and test files were not used as review evidence.
- Exact seven-file first-parent scope:
  - `x-first-researcher-sourcing/README.md`
  - `x-first-researcher-sourcing/contracts/x.grok.compact_discovery.result.v1.schema.json`
  - `x-first-researcher-sourcing/docs/GROK_COMPACT_DISCOVERY_AND_PROFILE_HYDRATION.md`
  - `x-first-researcher-sourcing/docs/reviews/2026-07-17-f187d28-compact-hydration-final-rereview.md`
  - `x-first-researcher-sourcing/src/x_first/compact_grok_discovery.py`
  - `x-first-researcher-sourcing/src/x_first/grok_profile_hydration.py`
  - `x-first-researcher-sourcing/tests/test_compact_grok_discovery.py`
- Scope size: 7 files, 492 insertions, 37 deletions.
- Scoped binary-diff SHA-256: `5f14fb42343aaf73104653cf3082e703ab4e61cfe2444675f6839acad39c7c4b`.
- Scoped name/status SHA-256: `dc5d6cbb7ba3c07643290e282b1cf9677ed6da01cf4f7caa7821206a67c8513a`.
- Scoped numstat SHA-256: `5ae404856f382417e815d2884658a3f5a87154d66ab9477cc761f93685c46b56`.
- Commit-object payload SHA-256: `26ef56bbc04fba2d4be4fe15cf29b1ecba62ba75114c369f639c99042cadfdc4`.
- Review boundary: no provider/model call, credential source, owner-private candidate artifact, or live X data was
  accessed. All probes used synthetic fixture handles and retained deterministic raw-session fixtures.

## Scoped blob identities

| File | Git blob OID | SHA-256 of content |
|---|---|---|
| `README.md` | `dd42c2baa62ebda241e6cf059b490251c106a33c` | `27e4342d809de5aae5224c1b09d811de2077e5b234431b5f5939fdc3507e4d65` |
| compact discovery schema | `3db3382b28ce81bc6d8aa490a4d4f32da9891aeb` | `7f1354ae78da740e444341b8c9c345311885c02f5deb7264822c0c0b8f189405` |
| workflow document | `d277bf20adfdf0100fdf9d7c38330d9d6ae8c144` | `2ba5d355c04c6555f9905bb7ecd8cf33d3b95db26cd6db37e09cbfa560050ef2` |
| prior pinned review | `3695777234393334ba69149e6752a6018e3c503a` | `f29ffcfe87bb5fa7c51a6eee7d5cf041d81332307b7fa9beaf453758cd5a866e` |
| compact discovery runtime | `93dfa76202067929ba8c66c5ab3b3ce3bd347a33` | `e6e9204367017d8f428be195d2c2ac73b5eb8bd9a7d2ec93caf72bc3dbaba190` |
| profile hydration runtime | `4437c044999d76ef3db45c4688f02cb18cbf474b` | `a45fb6fedb6ce5d010a29e75ff7527aecae1fb4d4526bfb3eee3f2ba18bc8645` |
| compact discovery tests | `2e5a8f74a5b0662eea18cc47b6acb4f1ce390920` | `782baa076b0fbfc68fa6c3c51b9ae0166e61f789ee5d8f6ad8e8defe612e5cfa` |

## Finding totals

| Severity | Count |
|---|---:|
| P0 | 0 |
| P1 | 1 |
| P2 | 0 |
| P3 | 2 |

## Findings

### P1-1 — re-raise — caller-overloadable equality can mutate the fresh replay result and inject hydration identities absent from retained sessions

The fixed-forward closes the ordinary JSON-mapping form of the prior blocker. The merge envelope retains sorted
operator projections and merge controls (`src/x_first/compact_grok_discovery.py:273-291`), reruns every projection and
the deterministic merge (`:1576-1614`), rejects a raw union at the old hydration entrypoint
(`src/x_first/grok_profile_hydration.py:412-426`), and the new built-in-container mutation tests correctly reject
sidecar deletion, lead/reference digest replacement, candidate-id replacement, allowed-origin rebinding, stale-marker
substitution, and self-consistently rehashed summary changes (`tests/test_compact_grok_discovery.py:1092-1182`). The
16-case independent ordinary-container matrix also rejected all result, summary, projection, control, order, and
diagnostic-digest mutations. Forward and reversed valid input orders reconstructed identically.

The new normal boundary still accepts a caller-authored executable container rather than a closed plain-data value.
`replay_merged_compact_discovery` checks only the outer envelope with `isinstance`, does not recursively require exact
built-in `dict`/`list`/`str` types for cached result and digest fields, and then evaluates caller-owned equality with
the fresh authoritative reconstruction as the right-hand operand
(`compact_grok_discovery.py:1584-1605,1617-1626`). Python therefore invokes a mapping subclass's `__eq__`/`__ne__`
against `recomputed.result`. The function returns that same reconstruction at `:1627`, and the hydration builder
immediately derives lookup identities from its now-mutable `result` without another validation or digest check
(`grok_profile_hydration.py:359-408`). A `str` subclass on the caller-owned diagnostic digest can likewise override
the digest equality used before the whole-envelope comparison.

The independent deterministic probe used only `dataclasses.replace` on a valid synthetic envelope. A `dict` subclass
held the union from a three-projection merge and, during `__eq__`/`__ne__`, replaced the fresh reconstructed result;
an equality-overriding `str` subclass supplied the caller digest. The envelope retained two projections, whose honest
hydration expectation contained one identity. The mutated replay was nevertheless accepted and the normal hydration
builder emitted two identities, including `platform:99302`, which existed only in the omitted third projection. A
second probe deleted the unresolved sidecar through the same mechanism and returned a replay object whose cached
`result_sha256` no longer matched its returned result. No thread race, monkeypatch, provider call, or raw-session
forgery was required.

This is not merely a misleading diagnostic: it lets a value absent from every retained session cross the exact normal
handoff and become a native-X hydration lookup. It contradicts the documented guarantees that the complete union is
compared, caller-authored hashes cannot help, and no lookup is derived before that proof
(`docs/GROK_COMPACT_DISCOVERY_AND_PROFILE_HYDRATION.md:168-185`).

Before comparing caller caches to the reconstruction, recursively reject non-exact plain JSON container/scalar types
and require exact built-in strings for all digest/control fields. Do not pass the fresh reconstructed mapping into
caller-overloadable equality; compare canonical immutable bytes or exact-type digests, then assert the returned
reconstruction's result/summary digests again. Add regressions using equality-overriding mapping/string subclasses
that attempt both sidecar deletion and extra-lead/input-shard injection, and require rejection before any hydration
expectation is emitted. As adjacent hardening, snapshot each projection before validating that snapshot rather than
validating a shallow-frozen nested value and only then deep-copying it.

### P3-1 — residual — candidate-free aggregate performance derivation is still not replayable

This fixed-forward binds compact merge inputs and the hydration handoff. It does not add a closed aggregate schema,
versioned generator, field-level source mapping, or formulas for the broader diagnostic aggregate retained in the
prior review. Those hashes and arithmetic remain useful diagnostics rather than a formal replayable performance
claim.

### P3-2 — residual — six-file replay still proves shape conformance rather than Grok CLI binary identity

The workflow document continues to state that the six retained artifacts omit CLI version and executable identity.
The lane therefore proves exact `x.grok.raw_session_shape.v1` conformance, not provenance from the named Grok CLI
binary. This remains an honestly documented, non-blocking provenance boundary.

## Prior finding dispositions

| Prior finding at `f187d28` | Disposition at `abe7725` |
|---|---|
| P1-1: sidecars are not replay-bound at the hydration handoff | Ordinary built-in mapping mutations are closed: normal hydration requires the typed merge envelope, replays retained sources, remerges, and rejects deletion/rebinding even after caller digest recomputation. Re-raised above because caller-overloadable nested equality can mutate the fresh replay result and inject a lookup absent from retained projections. |
| P3-1: aggregate derivation is not replayable | Residual unchanged. |
| P3-2: CLI executable identity is absent from retained six-file evidence | Residual unchanged and documented. |

## Validation

| Check | Pinned result |
|---|---|
| Compact discovery + profile hydration | 56/56 passed in 0.821s |
| Full offline test discovery | 464/464 passed in 103.227s |
| Recall-pool regression | 31/31 passed in 2.271s |
| `PYTHONPATH=src python -m x_first.contracts` | exit 0; `errors=[]`; fixture precision 1.0 and recall 1.0 |
| Scoped Ruff on two runtime modules and compact tests | all checks passed with `--no-cache` |
| Compact and hydration result schemas | both passed duplicate-key-rejecting JSON parse |
| Exact seven-file first-parent `git diff --check` | passed |
| Raw mapping and ordinary-container adversarial matrix | raw mapping plus 16 self-consistently rehashed result/summary/projection/control/order/digest mutations all rejected; valid baseline accepted |
| Input-order determinism | forward/reversed stable-plus-provisional and multi-stable-sidecar envelopes reconstructed identically |
| Caller-overloadable mutable-container probe | failed: two retained projections produced two hydration identities rather than the honest one; injected identity came only from an omitted third projection |

## Final verdict

NO-GO
