# Compact discovery and profile hydration pinned final review

Date: 2026-07-17

## Evidence header

- Reviewed commit: `b17e867bd00d632f8502a5994330ace69197297a`
- Exact parent: `ae52cbaa30f288d834c2e8f445696254224a6d1a`
- Pinned tree: `93c8c0fcf60060b30965a79a8ce4823a4959263c`
- Commit subject: `fix(x-first): replay compact Grok session evidence`
- Relationship: the reviewed commit has exactly the requested parent; the parent is an ancestor of the reviewed commit.
- Exact scope: the 11 files changed under `x-first-researcher-sourcing`:
  - `README.md`
  - `contracts/x.grok.compact_discovery.result.v1.schema.json`
  - `contracts/x.grok.profile_hydration.result.v1.schema.json`
  - `docs/GROK_COMPACT_DISCOVERY_AND_PROFILE_HYDRATION.md`
  - `docs/reviews/2026-07-16-9b54004-compact-hydration-rereview.md`
  - `src/x_first/compact_grok_discovery.py`
  - `src/x_first/grok_operator_session_replay.py`
  - `src/x_first/grok_profile_hydration.py`
  - `tests/grok_raw_session_fixture.py`
  - `tests/test_compact_grok_discovery.py`
  - `tests/test_grok_profile_hydration.py`
- Scope size: 11 files, 4,041 insertions, 197 deletions.
- Scoped binary-diff SHA-256: `2ab72f7cd5fa5ba0baf01a1aa5cbade042475b4ce124ae68bfc55f5a9941ef50`
- Scoped name/status SHA-256: `3044b4cc78741fa180c75ff03a9fa4f4c5dcd47ecf76365aaba9588bdcd35ed9`
- Scoped numstat SHA-256: `6a9ba33f2873f827af36a4af5a0406dbc5a81afc67dddf21762c0f54b616bed0`
- Commit-object payload SHA-256: `3817d2b3491923695af898bbbdd59f0668a728d42494aec0b613c63fc6f825f0`
- Inspection source: an independently extracted `git archive` of the pinned tree plus pinned `git show`, `git diff`,
  `git ls-tree`, and `git cat-file` objects. Mutable working-tree implementation files were not used as evidence.
- Review boundary: provider/model calls, credentials, owner-private candidate sources, and live X data were not read.
  All adversarial probes used synthetic fixture identities.

## Scoped blob identities

| File | Pinned blob |
|---|---|
| `README.md` | `b6731f2773c2ac5448d1da09433756aa73e5d88a` |
| compact discovery schema | `83f7e31725a15e65a85d8746d0c8c4b2ee338001` |
| profile hydration schema | `114308b06385d104afe2349bdf56020ec32cc4eb` |
| workflow document | `f335d466699fa46e2c336e8ffd7817c2fbf47052` |
| prior pinned review | `80d0029bed3914b2c9b8790184ac0ddd3f09b543` |
| compact discovery runtime | `9d68f003125732356ba6f2f7f02fbe0e7de9785e` |
| shared raw-session replay | `ef25c482e468f94b821e89a7fbe55e098967ce08` |
| profile hydration runtime | `da7afba52b49f3cb237f6481e1adbcf971c53047` |
| synthetic raw-session fixture | `a1f5457659c82aaed2ad6e6e58a940cc9acdda70` |
| compact discovery tests | `6baabff172e55daad1483c5e993328f7a58959f2` |
| profile hydration tests | `8184ce278db7c146deb5d3a5fb73cd0a5add4b2a` |

## Validation

| Check | Pinned result |
|---|---|
| Targeted compact discovery + hydration tests | 50/50 passed in 0.558s |
| Recall-pool regression | 31/31 passed in 1.549s |
| `PYTHONPATH=src python -m x_first.contracts` | exit 0; `errors=[]`; fixture precision/recall both 1.0 |
| Ruff on three runtime and three test/fixture modules | passed with `--no-cache` |
| Both changed JSON schemas parsed with a duplicate-key-rejecting loader | passed |
| Scoped `git diff --check` | passed |
| Receipt/raw-source mutation, missing completion, unsupported tool, duplicate terminal, trailing output, prompt mode, and consumer replay tests | passed |
| Stable platform-id mismatch and missing-id tests | returned candidate-free invalid evaluations as intended |
| Wrong nested projection/receipt/result/precommit/facts/expectation tests | returned candidate-free invalid evaluations as intended |
| User-before-tool causal probe | failed the intended contract: a six-file session with tool start/completion at update indices 0/1, the only user update at index 2, and terminal at index 3 was accepted |
| Stable-plus-provisional identity probe | failed the intended fence: a valid union absorbed one provisional observation into one stable id, changed both stable axes to `ambiguous`, merged source origins, and emitted one stable hydration expectation |
| Deeply nested assistant JSON probe | failed terminal-total behavior: a roughly 60KB, 10,000-level object escaped as uncaught `RecursionError` |

Green authored tests establish substantial closure: exact six-file hashing, receipt recomputation, closed tool/chat/event
kinds, prompt-mode binding, terminal-after-tool selection, operator-facts separation, consumer replay, stable-id hydration
tuples, mismatch rejection, multiple-stable-id sidecars, and typed-envelope error redaction all execute. They do not cover
the three counterexamples above.

## Findings totals

| Severity | Count |
|---|---:|
| P0 | 0 |
| P1 | 2 |
| P2 | 1 |
| P3 | 2 |

## Findings

### P1-1 — `re-raise` of prior P1-1 — The ordered replay accepts native-X calls before the bound user turn begins

The new replay correctly closes most of the former caller-authored receipt gap, but its update-order state machine is
not closed at the beginning of the turn. `user_message_chunk` increments `user_message_count`; assistant chunks require
that count to be one, but neither the `tool_call` nor `tool_call_update` branch requires the user event to have occurred
(`src/x_first/grok_operator_session_replay.py:875-920,964-1019,1020-1097`). The final checks require only one user
event, monotonic timestamps, paired calls, and a post-tool terminal
(`src/x_first/grok_operator_session_replay.py:1099-1120`).

An independent probe began with a valid six-file fixture, removed optional thought rows, moved the only user update
after the tool start and completion, and kept metadata timestamps monotonic. The compact operator builder accepted it;
the derived receipt recorded the tool at update indices 0/1 and the terminal at 3. The exact chat prefix, request id,
backend-tool row, receipt, and consumer replay all remained self-consistent. Thus the receipt can certify a call
lifecycle that the replay's own ordered ledger says happened before the current user-turn event. This undercuts the
documented claim that the precommit plus ordered session ledger binds the native calls to that execution context
(`docs/GROK_COMPACT_DISCOVERY_AND_PROFILE_HYDRATION.md:81-112`).

Require the unique user event to precede every thought, tool, completion, and assistant update—preferably make it the
first registered update for this shape version. Add the same reordered-ledger regression to both compact discovery and
hydration because they share the replay. If the provider intentionally permits a different order, encode that exact
observed order as a new versioned shape with an alternative causal binding; monotonic caller-adjustable timestamps are
not a substitute.

### P1-2 — `re-raise` of prior P1-3 — Unique stable-id absorption lets provisional handle evidence rewrite a stable account

The previous cross-phase duplicate-handle failure is no longer an unconsumable union, but the chosen repair removes
the identity uncertainty rather than preserving it. When a provisional handle has exactly one same-handle stable id,
the merge pops the provisional group and extends the stable-id group with it
(`src/x_first/compact_grok_discovery.py:1335-1367`). It then computes temporal states, source references, and origins
across the combined group and emits one `stable_platform_id` lead
(`src/x_first/compact_grok_discovery.py:1375-1453`). The `same_handle_provisional_evidence_absorbed` marker records that
this happened, but no validator or hydration-input rule prevents the provisional observations from supporting or
changing the stable lead. The normal hydration expectation consequently emits that stable id
(`src/x_first/grok_profile_hydration.py:355-404`).

The independent probe combined a stable current/current observation with a same-handle, null-id
historical/historical observation. The union was runtime-valid, contained one stable lead, changed both axes to
`ambiguous`, merged both shard origins into its source references, and emitted one hydration tuple for the stable id.
This contradicts the function's own rule that null ids remain explicit provisional identities and never weaken the
stable-id fence (`src/x_first/compact_grok_discovery.py:1275-1283`). A handle can be reassigned; the absence of a second
known stable id does not prove that a null-id observation belongs to the one known stable account.

Keep the provisional observation out of the stable lead's state and supporting source set until identity is resolved.
The existing sidecar mechanism can be generalized to one-or-more candidate stable ids, or source/state observations
can carry an explicit identity-binding state that downstream qualification must exclude until resolved. It is fine to
avoid a second hydration request and to keep a high-recall queue; it is not safe to relabel unresolved evidence as
support for a stable platform identity.

### P2-1 — `new` — Deep but in-budget assistant JSON escapes the shared replay instead of failing closed

`_json_object_slices` scans every opening brace and catches `json.JSONDecodeError`, but not `RecursionError`, around
`JSONDecoder.raw_decode`; it then strict-parses the selected slice without a depth/node preflight
(`src/x_first/grok_operator_session_replay.py:677-714`). Both public projection builders translate only
`GrokOperatorSessionReplayError` into their stable contract errors
(`src/x_first/compact_grok_discovery.py:1027-1062`, `src/x_first/grok_profile_hydration.py:1019-1056`).

A roughly 60KB assistant string containing a 10,000-level object is well below the existing per-record byte ceiling,
yet the compact builder raised an uncaught Python `RecursionError`. Hydration uses the same function. The exact old
malformed-typed-envelope case is repaired by `evaluate_profile_hydration_batch`, but malformed raw model output is
still not terminal-total.

Add a deterministic assistant-output depth/node preflight before recursive decoding, translate `RecursionError` to a
stable replay error, and regression-test both builders. A byte ceiling alone is insufficient because depth, node
count, and the current every-brace raw-decode loop are independent resource dimensions.

### P3-1 — `residual` of prior P3-1 — Aggregate performance derivation is still not replayable

The prior artifact remains accurate: the candidate-free aggregate pins source hashes but still has no closed schema,
versioned generator, field-level source mapping, or formulas that a pinned reviewer can replay
(`docs/reviews/2026-07-16-9b54004-compact-hydration-rereview.md:156-168`). This commit adds session replay and identity
contracts, not the missing aggregate derivation path. The diagnostic label should remain, and the metrics should not
be promoted as a formal performance claim until a hash-verifying candidate-free aggregator exists.

### P3-2 — `residual` — The six-file replay proves shape conformance, not Grok CLI binary identity

The workflow document explicitly records that the six files do not contain CLI version or executable identity, so the
replay cannot prove that conforming bytes came from the named `grok 0.2.101` binary
(`docs/GROK_COMPACT_DISCOVERY_AND_PROFILE_HYDRATION.md:125-129`). This is an honestly documented provenance residual,
not a new blocker for the offline contract. Bind an operator-owned executable/version digest in a future retained
source or process receipt before making binary-specific provenance claims.

## Prior finding dispositions at `b17e867`

| Finding from the `9b54004` re-review | Disposition |
|---|---|
| P1-1 caller-authored typed receipts | Substantially repaired by six-file replay, transcript/source hashing, exact start/completion derivation, closed tool/chat/event registries, prompt modes, terminal causality, operator-facts digest, and consumer replay. Not fully closed because the update registry accepts calls before the sole user-turn event; re-raised as P1-1 above. |
| P1-2 stable identity lost at hydration | Closed for the normal evaluated path. The expectation carries lead identity, lookup alias, and expected stable id; renamed identities require an explicit alias; null/different matched ids return candidate-free invalid results. |
| P1-3 stable-plus-provisional union unconsumable | Cross-phase representability is repaired, and multiple-stable-id collisions use a no-lookup sidecar. The unique-stable-id absorption path contaminates stable state/evidence instead of preserving provisional uncertainty; re-raised as P1-2 above. |
| P2-1 malformed typed envelope crash | Closed for malformed projection, receipt, result, precommit, operator facts, and expectation members at the hydration evaluator. A distinct raw-output depth crash remains as new P2-1 above. |
| P3-1 aggregate derivation | Residual unchanged. |

## Confirmed properties and residual boundary

- The receipt is now derived from and later replayed against the exact retained six-file bytes; arbitrary receipt
  hashes, omitted completions, unsupported tools, post-terminal output, and forged projection wrappers fail closed.
- Prompt binding is a closed two-mode registry; the exact five-row prefix, system prompt, prompt context, session,
  request, model, and reasoning effort are digest-bound.
- Chat suffix and event/update kinds are closed; backend call identities and canonical arguments reconcile with the
  completion ledger; the final assistant row binds the selected strict terminal.
- Operator execution facts are separate immutable inputs, are digest-bound into the receipt, and are compared again
  at consumer replay rather than copied from model output.
- Non-null stable-id rename grouping, explicit lookup aliases, closed hydration identity tuples, and matched stable-id
  missing/mismatch errors execute as intended.
- Multiple known stable ids for one handle remain separate quarantined leads; provisional evidence in that case is
  retained by a candidate-free sidecar and does not generate hydration work.
- The exact prior malformed typed-envelope crash is repaired and evaluator errors remain candidate-free.
- Neither raw native-X response bodies nor CLI executable identity are proven by this scope. Profile fields remain
  `model_mediated_unverified`, and aggregate performance remains diagnostic-only.

## Final verdict

NO-GO
