# Compact replay equality-overload fixed-forward pinned final re-review — `d124035`

Date: 2026-07-17

## Evidence header

- Review type: non-author, adversarial, read-only Git-object final re-review.
- Repository: `/Users/changyuyi/projects/Sourcing AI Agent Dev`.
- Reviewed commit: `d124035f922a95777ba8ab418ba90786fe5cc9d8`.
- Exact first parent: `155f6d505d7246dcc1168a404260747ba291f00c`.
- Commit tree: `a36919b5e87c0080ce436a482333bb52723f1e8d`.
- Parent tree: `4cc6d86b453e1555e36e99ec08030c692f20a70a`.
- Commit subject: `fix(x-first): snapshot compact replay inputs`.
- Inspection source: detached pinned checkout at
  `/private/tmp/xfirst-d124035-review.gdR6wb`, plus exact pinned `git diff`, `git show`, `git ls-tree`, and
  `git cat-file` objects. Mutable working-tree implementation and test files were not used as review evidence.
- Exact five-file first-parent scope:
  - `x-first-researcher-sourcing/README.md`
  - `x-first-researcher-sourcing/docs/GROK_COMPACT_DISCOVERY_AND_PROFILE_HYDRATION.md`
  - `x-first-researcher-sourcing/docs/reviews/2026-07-17-abe7725-compact-hydration-final-rereview.md`
  - `x-first-researcher-sourcing/src/x_first/compact_grok_discovery.py`
  - `x-first-researcher-sourcing/tests/test_compact_grok_discovery.py`
- Scope size: 5 files, 992 insertions, 59 deletions.
- Scoped binary-diff SHA-256: `bb81441b55ad1870e70ff2432d07100e3616f52f90daf547d3d3f11a93ac3ff2`.
- Scoped name/status SHA-256: `42d2d3b9eb39b7468f008d31bbe0d94470ea61ec2e7bf4c6430e4faaec549972`.
- Scoped numstat SHA-256: `1b6decfcec6075baa4fbd5bd30c71da32859d222a09f645bb7157506d2dbd33f`.
- Commit-object payload SHA-256: `403e407902dc1fcbae632775f97d6a78fb7f106a111d4356ce2303e16db6daef`.
- Review boundary: no provider/model call, credential source, owner-private candidate artifact, or live X data was
  accessed. All adversarial probes used synthetic fixture handles and retained deterministic raw-session fixtures.

## Scoped blob identities

| File | Git blob OID | SHA-256 of content |
|---|---|---|
| `README.md` | `e6859d1aa0c216cb23e58a3e5ca9f5f1e5482e7a` | `845f764893e9e5db8acc1730b0e210fe29caf91e595acb9cb4a969030c4d5797` |
| workflow document | `0a9738690099bf22724fd7cc56ddf9326a4c7271` | `f9e4ad136ec4a696c3a8c155573382ca97296a6aebeb5c3c0d06f31f4b14ef24` |
| prior pinned review | `efe2d77095867151852b4956a6c39304a2a6278d` | `c42c7cd54dc9c368dbc95c3ac5856f72b248a320b4ab282829400f34ed561885` |
| compact discovery runtime | `2e3ccd0c22d974cc3911c96260bb58e9b83cfa20` | `97738ee738e22395ef408ea4764147b23c643eda9d15c48e3759dcc68b236074` |
| compact discovery tests | `0f6ccc122e1954c56dd4ae5efe9b4eee73291aae` | `b07e94e1e53cb3f2fea2606d2378b4ec764cc2808cc3179defe202af49c709ed` |

## Finding totals

| Severity | Count |
|---|---:|
| P0 | 0 |
| P1 | 0 |
| P2 | 0 |
| P3 | 2 |

## Findings

No new or re-raised P0, P1, or P2 finding remains in this fixed-forward scope.

### P3-1 — residual — candidate-free aggregate performance derivation is still not replayable

This fixed-forward closes the executable-container hydration handoff. It does not add a closed aggregate schema,
versioned generator, field-level source mapping, or formulas for the broader diagnostic aggregate retained in the
prior review. Those hashes and arithmetic remain useful diagnostics rather than a formal replayable performance
claim.

### P3-2 — residual — six-file replay still proves shape conformance rather than Grok CLI binary identity

The workflow document continues to state that the six retained artifacts omit CLI version and executable identity.
The lane therefore proves exact `x.grok.raw_session_shape.v1` conformance, not provenance from the named Grok CLI
binary. This remains an honestly documented, non-blocking provenance boundary.

## Prior P1 disposition

The `abe7725` re-raise is closed.

- `_snapshot_exact_plain_json` admits only exact built-in `dict`, `list`, `str`, `int`, `bool`, finite `float`, and
  `None`, validates UTF-8 strings and keys, detects active-container cycles, and enforces depth 64 before any semantic
  replay (`src/x_first/compact_grok_discovery.py:295-361`). A nested subclass cannot reach its equality, hashing, or
  copy hook.
- The merge envelope, projection, receipt, session precommit, execution facts, raw artifacts, tool completions,
  summaries, tuple containers, strings, integers, Booleans, and bytes are rebuilt under exact-type fences
  (`:364-714`). Projection values are snapshotted before `_assert_projection` performs validation and raw-session
  replay (`:1512-1534`).
- The normal hydration entrypoint replays the typed merge before reading one lookup identity
  (`src/x_first/grok_profile_hydration.py:359-408`). The replay boundary snapshots first, remerges only those
  snapshots, and compares result/summary canonical UTF-8 JSON bytes (`compact_grok_discovery.py:1997-2047`). Cached
  digests are checked against those bytes, and the returned reconstruction's result/summary digests are checked again
  at `:2050-2056`.
- The remaining projection/envelope `!=` operations at `:1526`, `:2030`, and `:2048` compare only newly constructed
  exact dataclasses whose nested values have already crossed the exact-type snapshot. They do not receive a caller
  subclass. The remaining `deepcopy` operations at `:1394-1405`, `:1638`, and `:1655` execute only on replay-produced
  or already snapshotted plain values on the normal handoff. Canonical JSON encoding at `:717-730` likewise receives
  only operator-owned plain snapshots during replay comparison.
- The independent reproduction retained two projections with one honest hydration identity and a third omitted
  projection containing another identity. Outer and nested mapping/string hooks attempted both omitted-identity
  injection and sidecar deletion. All attacks were rejected before hydration, none of 28 independent attack cases
  executed `__eq__`, `__ne__`, `__copy__`, `__deepcopy__`, or custom string hashing, and the honest identity set stayed
  one row. Exact forward/reverse input order reconstructed identically; mutating an original projection after merge
  did not alter the retained envelope.

## Standard-path and regression audit

- A valid stable-plus-provisional merge preserves the unresolved sidecar and emits only the resolved stable lookup.
- Deleting the sidecar, self-consistently replacing cached result/summary digests, rebinding an origin, or supplying a
  result from an omitted projection fails before a hydration expectation is emitted.
- Exact built-in values still replay; forward and reverse projection order produce the same envelope.
- Caller mutations after `merge_compact_discovery_results` do not change the retained projection snapshot.
- Dict/list/string/integer/bytes/tuple subclasses, every merge/projection/receipt/precommit/facts/artifact/completion
  dataclass subclass, nested tuple subclasses, Boolean-as-integer summary/receipt fields, cycles, over-depth values,
  and non-finite floats fail closed with the candidate-free hydration boundary error.

## Validation

| Check | Pinned result |
|---|---|
| Compact discovery + profile hydration | 58/58 passed in 0.692s |
| Full offline test discovery | 475/475 passed in 53.225s |
| Recall-pool regression | 31/31 passed in 2.562s |
| `PYTHONPATH=src python -m x_first.contracts` | exit 0; `errors=[]`; fixture precision 1.0 and recall 1.0 |
| Scoped Ruff on compact/hydration runtimes and compact tests | all checks passed with `--no-cache` |
| Compact and hydration result schemas | both passed duplicate-key-rejecting JSON parse |
| Exact five-file first-parent `git diff --check` | passed |
| Equality/copy-hook adversarial probes | 28/28 attacks rejected; zero custom hook callbacks; zero extra hydration identities |
| Sidecar/order/input-mutation probes | honest sidecar retained; forward/reverse equal; post-merge source mutation isolated |
| Detached pinned checkout | clean after validation |

## Final verdict

GO
