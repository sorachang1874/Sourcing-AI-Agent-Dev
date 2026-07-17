# Source-neutral mapping pinned review

Date: 2026-07-17

## Evidence header

- Reviewed commit: `2a72290d3d5342f0910b8519abd5e4a95d457e89`
- Exact parent: `a8b308e2c6eb174c86a5eafa3e381969687b2f6a`
- Pinned tree: `480ff7bd83eb7d28f0fc9f83af4badacc40bcff9`
- Commit subject: `feat(x-first): freeze source-neutral mapping method`
- Relationship: the reviewed commit has exactly the requested parent.
- Exact scope: the 16 changed files under `x-first-researcher-sourcing`; no sibling-repository changes were reviewed.
- Scope size: 16 files, 2,923 insertions, 73 deletions.
- Scoped binary-diff SHA-256: `5d5216a109a61bebb73e45a0bd4a33e31ac94b182f2877d9abbc3caf2f3be447`
- Scoped name/status SHA-256: `203cb4628b2a54b9f6ab30551dbd14fd0628ab2302441e00fd395bc386987a20`
- Scoped numstat SHA-256: `0d89416b689c20154f52917ccdc22db3c85a7900389c8a8e38d2e7a5316a719d`
- Commit-object payload SHA-256: `eaf721d74f3b9faf8f31d220f971ad1a98399e9c9f86f7aa02a5a2d4674182bf`
- Inspection source: an independently extracted `git archive` of the pinned tree plus pinned `git show`, `git diff`,
  `git ls-tree`, and `git cat-file` objects. Mutable working-tree implementation files were not review evidence.
- Review boundary: no network, provider, model, credential, or live X call was made. Owner-private live receipts,
  summaries, candidates, and raw sessions were not opened. The checked-in candidate-free calibration aggregate was
  exercised only by the provider-free validators and tests.

## Scoped blob identities

| File | Pinned blob |
|---|---|
| `README.md` | `dfbbf2358b382e2b34cdb132a77d7c031f53397c` |
| `configs/source_neutral_mapping_policy.v1.json` | `d2a9b466fd56240134e7a0d6a96c354e4338ca68` |
| `contracts/source_neutral_mapping_contract_registry.v1.json` | `91fd0f324deaa117701bf39f97bd859dc499eeef` |
| calibration aggregate schema | `9f045f6ccf6e518bd538640ce2cfebfed7ee9240` |
| candidate-free aggregate schema | `61e737f2c7e5eb8176f3a1491250977810bc768a` |
| candidate-manifest schema | `ef7faabb293570773e843146b3aed20b41b4452f` |
| policy schema | `638cd73f8170e68c1f3812fdd17bae883a1843b3` |
| session-receipt schema | `f4305c3a5cedb637ef9aa52be3f470d75f487147` |
| wave-plan schema | `e4dd3a26b66769ca1a08c0f7b3cd2c85143e34d1` |
| `docs/SOURCE_NEUTRAL_MAPPING_CONTROLLER.md` | `87af285d10fc2faf98493de9589c7d7b085a207d` |
| checked-in candidate-free calibration aggregate | `63e14e00c2e7dae738014e0fc4317cbd996a85e2` |
| synthetic mapping manifest | `8b57cf37219502bc0233f8d882baecca7edc00a8` |
| `src/x_first/contracts.py` | `ef0df646c8408d78628babb00466f9bc40ba94c0` |
| `src/x_first/grok_operator_session_replay.py` | `9edf4ec3834399b5560d1150f67ac217a26093d4` |
| `src/x_first/source_neutral_mapping.py` | `11552cfd495d2f24129bc082307d0755fb38df0f` |
| `tests/test_source_neutral_mapping.py` | `7affdb86b0a0f96028758071a3f0eca92603a5bb` |

## Validation

| Check | Pinned result |
|---|---|
| Targeted `tests.test_source_neutral_mapping` | 20/20 passed in 0.459s |
| Full provider-free suite | 501/501 passed in 90.767s |
| `PYTHONPATH=src python -m x_first.source_neutral_mapping` | exit 0; `errors=[]`; `status=valid` |
| `PYTHONPATH=src python -m x_first.contracts` | exit 0; `errors=[]`; precision/recall both 1.0 |
| Repository-wide Ruff | passed |
| Scoped `git diff --check` | passed |
| Exact Wave P/default-query and greater-than-100 authored tests | passed: 137 candidates, 46 batches, 274 calls |
| Literal flat terminal plus old JSON-terminal replay regression | passed |
| Policy-drift probe | failed the intended fence: a rehashed plan changed call 1 to `from:unrelated (attention)` and an unrelated author; plan validation and exact six-file replay returned an accepted, committable reference for candidate 1 |
| Receipt/hydration authority probe | failed the intended fence: a self-hashed receipt and caller-authored hydration promoted `https://evil.invalid/evilhandle/status/777` and arbitrary text to `exact_source_bound` Luna input |
| Structural-stop probe | failed the intended fence: two caller-authored zero rows with arbitrary distinct SHA strings and empty queues returned `structural_convergence` without any execution evidence |
| Empty-coverage challenger probe | failed the intended semantics: zero accepted receipts produced 18 challengers for six candidates, including a `thread` challenger with no seed Post id |
| Four-denominator arithmetic probe | failed the intended closure: zero attested calls plus five stable ids, hydrations, Luna reviews, and upgrades produced a valid aggregate with two 1.0 downstream rates |

The green authored suite confirms substantial useful behavior: the checked policy and fixture are lab-neutral and
hash-bound; the default two champion queries are exact; arbitrary manifest size is not a business cap; flat terminal
blocks bind order, author, host, and stable Post id; exact six-file replay and terminal-after-tool causality execute;
the old JSON terminal path remains compatible; same-batch and cross-receipt stable-id collisions fail closed; the four
metric names are explicit; and the checked-in calibration aggregate is candidate-free, schema-valid, and self-hash
bound. The probes below expose authority and state-transition gaps not covered by those tests.

## Findings totals

| Severity | Count |
|---|---:|
| P0 | 0 |
| P1 | 3 |
| P2 | 2 |
| P3 | 0 |

## Findings

### P1-1 — The plan validator does not enforce the exact manifest/policy Wave P calls

`validate_wave_plan` verifies each batch's self-hash and candidate-ref hash, then checks only aggregate call count,
manifest ref order, global ordinals, and top-level counts
(`src/x_first/source_neutral_mapping.py:387-429`). It never reconciles a call's `candidate_ref`,
`expected_author_handle`, candidate ordinal, champion cell, alias group, aliases, tool, query, limit, or mode against the
bound manifest and policy. `build_session_precommit` does not validate the plan at all; it copies the selected batch's
calls after checking only a prompt line containing that caller-supplied batch hash
(`src/x_first/source_neutral_mapping.py:432-465`).

An independent synthetic probe changed the first valid call to the still-schema-valid
`from:unrelated (attention)`, changed its expected author and alias cell, recomputed the batch and plan hashes, and
called `validate_wave_plan`. Validation passed. A matching six-file fixture then passed raw replay and emitted
`commit_allowed=true` for `https://x.invalid/unrelated/status/90001`, associated with
`fixture_candidate_001`. Raw replay proves that the altered call ran; it does not prove that it was the policy-owned
call. This defeats complete exact Wave P coverage and permits unrelated authored evidence to enter the candidate
frontier.

Reconstruct the expected batches/calls from the validated manifest and policy and compare every field, or make the
frozen plan an exact typed value that cannot be accepted without that recomputation. `build_session_precommit` must
take and validate the manifest/policy bindings (or a separately verified frozen-plan authority), not trust a
self-consistent plan mapping and hash. Add mutations for every call field and for batch grain/candidate placement.

### P1-2 — Self-authored receipt and hydration dictionaries can become exact Luna evidence

`build_frontier_queues` accepts ordinary mappings. Its authority check is a JSON schema, literal status/authority
flags, count equalities, and an unkeyed receipt self-hash that the caller can recompute
(`src/x_first/source_neutral_mapping.py:769-809`). It receives neither the raw six files nor the Grok and mapping
precommits, and it does not bind the receipt to the supplied manifest, policy, plan, or expected author. The test helper
actually manufactures such an accepted receipt without replay and feeds it to this boundary
(`tests/test_source_neutral_mapping.py:83-108,402-416`).

The next boundary repeats the same problem. `build_luna_input_queue` treats a caller mapping as exact when its status
string is `exact_source_bound`, its source URL equals the queued URL, and its caller-supplied text hash matches
(`src/x_first/source_neutral_mapping.py:855-925`). It requires only a nonempty URL host, not the descriptor host, and
has no typed hydration receipt or raw-source replay.

The independent probe constructed a schema-valid, self-hashed accepted receipt for candidate 1 at
`https://evil.invalid/evilhandle/status/777`, then supplied arbitrary text plus its digest. Both boundaries accepted
it and emitted Luna input labelled `exact_source_bound`. This contradicts the documented raw-replay and exact-hydration
authority boundary and can drive false lab/pretraining temporal state transitions.

Carry an exact typed replay projection (including retained immutable sources and precommits) through frontier
derivation and rerun replay there, following the repository's compact-discovery handoff pattern. Hydration likewise
needs a typed receipt/source projection whose exact URL, author, Post id, and full-text bytes are replayed and bound to
the manifest candidate and descriptor. A caller-created mapping, status label, or recomputable digest cannot be the
authority.

### P1-3 — Structural convergence can be declared for unexecuted or failed waves

`structural_stop` accepts four ordinary queue lists and the last two ordinary wave mappings. A wave is considered
valid solely from field shape, two zero integers, and a syntactically valid strategy SHA; material distinction is only
inequality between the two caller-supplied strings
(`src/x_first/source_neutral_mapping.py:929-970`). No plan/campaign binding, strategy payload, accepted session
receipts, execution-compliance denominator, full manifest coverage, or Luna-result authority is present. In addition,
the four queue keys exclude pending Wave P and invalid-batch retry work
(`src/x_first/source_neutral_mapping.py:62-67,718-725`).

The independent probe passed two rows named `failed_a` and `failed_b`, zero counts, and `a*64`/`b*64` signatures with
empty queues. The function returned `stop=true` and `reason=structural_convergence`; no session had executed. A tool
failure, raw-replay rejection, incomplete manifest, or pending split can therefore be represented as zero marginal
yield and end the campaign even though it is not a valid zero-yield strategy wave.

Derive the two trailing wave facts from replayed, plan-bound receipts and Luna results; recompute the strategy digest
from a closed strategy payload; require complete attested execution and manifest coverage; and include pending initial
and split/retry work in the structural queue registry. Failed or unexecuted work must be a distinct non-zero-wave
terminal state.

### P2-1 — Missing execution coverage is misclassified as sparse evidence and creates unusable challengers

`build_frontier_queues` sees only accepted receipts. It counts retained references, then defines every candidate with
zero or one reference as sparse and emits every challenger type
(`src/x_first/source_neutral_mapping.py:819-845`). It does not know whether the candidate's two champion calls were
accepted, rejected, never run, or are waiting for the mandated split. A `thread` challenger is emitted even when
`seed_stable_post_ids` is empty.

With the checked six-candidate manifest and `accepted_receipts=[]`, the function emitted 18 challengers—three for each
entirely unexecuted candidate—including thread work with no seed. Absence of an accepted receipt is not measured sparse
authored evidence. This skips the retry/coverage state, spends later strategy work on an unmeasured population, and
makes the challenger queue internally non-executable.

Bind frontier construction to the plan and one terminal receipt per batch/call. Keep unexecuted/rejected candidate
work in a pending or split/retry queue; classify sparse only after both champion calls are attested. Emit thread
challengers only when a valid seed exists, or define a separate seed-free strategy with its own closed contract.

### P2-2 — The four-denominator path accepts impossible pipeline arithmetic and erases rejected planned calls

When a valid six-call mapping precommit lacks raw replay, `replay_flat_session` correctly rejects it, but
`_rejected_session_receipt` records `planned_native_x_call_count=0`
(`src/x_first/source_neutral_mapping.py:617-628,688-715`). Thus a planned failed batch disappears from the receipt
denominator instead of contributing six planned and zero attested calls.

The aggregate builder then checks only completed <= planned, hydrated <= stable ids, and upgrades <= Luna reviews
(`src/x_first/source_neutral_mapping.py:978-1015`). It does not reject nonzero stable ids when completed calls are
zero, or nonzero Luna reviews/upgrades when exact hydrations are zero. The independent probe supplied 10 planned, zero
attested, and five for every downstream count; it emitted a schema-valid aggregate with a null stable-retrieval rate
and 1.0 exact-hydration and Luna-upgrade rates. Those states cannot arise from this pipeline, where raw-replay-attested
calls own stable ids and exact hydration owns Luna input.

Preserve a trusted planned-call count for rejected receipts after mapping-precommit validation, with a distinct
unknown value only when the precommit itself is invalid. Derive aggregate counts from the bound plan, accepted/rejected
session manifest, hydration receipts, and Luna-result manifest rather than accepting independent integers. Enforce
zero-propagation and the legal cross-stage cardinalities; if retries permit multiple downstream attempts, expose that
as an explicit attempt denominator rather than allowing impossible stage transitions.

## Confirmed properties and residual boundary

- The default policy is lab-neutral, configuration-driven, and retains the exact two calibrated keyword cells. The
  manifest keeps lab affiliation and pretraining experience as independent axes.
- Planning 137 candidates produces 46 batches and 274 calls without a business candidate/reference cap.
- Literal terminal parsing and the exact six-file replay extension correctly bind call order, exact arguments,
  terminal bytes, URL author/host/Post id, and terminal-after-tool causality. The pre-existing JSON terminal consumers
  remain green in the full suite.
- Saturation equality is treated as a lower bound, stable ids collide fail closed across candidates, challenger output
  is explicitly non-self, and Luna output is empty until a hydration input is supplied.
- All six registered JSON schemas, the policy, synthetic manifest, candidate-free calibration aggregate, and registry
  parse and validate in the checked-in fixture path. The aggregate's private receipt/summary digests were not and could
  not be reverified without the owner-private files; the synthetic private-binding unit test passed.
- No provider cost, live validation, product write, or protected-identity inference is authorized by this scope.

## Final verdict

NO-GO
