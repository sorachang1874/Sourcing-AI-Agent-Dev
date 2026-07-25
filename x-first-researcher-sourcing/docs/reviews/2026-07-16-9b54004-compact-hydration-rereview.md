# Compact discovery and profile hydration pinned re-review

Date: 2026-07-16

## Evidence header

- Reviewed commit: `9b54004b9bc8659c37796a7782b8785ea45c643a`
- Parent: `d9f52bdf9213575331d3d5fab5584ac2352de0dc`
- Pinned tree: `b1815f4468e6d641af3d9f84c7140a4dc8730529`
- Exact scope: the 11 files changed by `d9f52bd..9b54004`:
  - `x-first-researcher-sourcing/README.md`
  - `x-first-researcher-sourcing/contracts/x.grok.compact_discovery.result.v1.schema.json`
  - `x-first-researcher-sourcing/contracts/x.grok.profile_hydration.result.v1.schema.json`
  - `x-first-researcher-sourcing/docs/GROK_COMPACT_DISCOVERY_AND_PROFILE_HYDRATION.md`
  - `x-first-researcher-sourcing/docs/live-evidence/2026-07-16-gdm-compact-strategy-matrix.aggregate-receipt.v1.json`
  - `x-first-researcher-sourcing/docs/live-evidence/2026-07-16-gdm-compact-strategy-matrix.md`
  - `x-first-researcher-sourcing/docs/reviews/2026-07-16-e7d9e04-compact-hydration-pinned-review.md`
  - `x-first-researcher-sourcing/src/x_first/compact_grok_discovery.py`
  - `x-first-researcher-sourcing/src/x_first/grok_profile_hydration.py`
  - `x-first-researcher-sourcing/tests/test_compact_grok_discovery.py`
  - `x-first-researcher-sourcing/tests/test_grok_profile_hydration.py`
- Scope size: 11 files, 2,133 insertions, 1,055 deletions.
- Scoped binary-diff SHA-256: `8db275300c123f9e33b019743ebf1346fb9d8a27d0358664fe595d59bb4e20b5`.
- Scoped name/status SHA-256: `25f860f988cb39990ec6ae557c66e688038f8d59e99a9706552466703c5a46fc`.
- Scoped numstat SHA-256: `0123387fd021f27fd06b62bcd85f9dbd6daeb0cd885df69e96e26c3c77612e9f`.
- Commit-object payload SHA-256: `39327f32ba4cb6411dfead83014cfa03497a8e1dd6d2fc5bf753086a50749457`.
- Inspection source: an independently extracted, read-only `git archive` of the pinned tree plus pinned
  `git show` / `git diff` objects. Mutable working-tree implementation files were not read.
- Review boundary: no provider/model call, credential read, owner-private source read, or candidate artifact read was
  performed. All probes used synthetic fixture identities and report aggregate outcomes only.

## Scoped blob identities

| File | Pinned blob |
|---|---|
| `README.md` | `d3f7451d0178830908956df161b9003d7aadafc4` |
| compact discovery schema | `49c6bd3fab7c3c8f22424bef1feb2578e92a6e0a` |
| profile hydration schema | `c3afebfed21254ad3caec4afeb27ea83ce1dd720` |
| workflow document | `2db1a02557c65beee6c4d453c13643dcd1b306a6` |
| aggregate receipt | `9c9c49ea64cfe1e6c6beec36ba2ed0f27fd8f1fd` |
| live-evidence document | `bc1f66f4a2af34f2be842012c3a0711b8a145a8b` |
| prior pinned review | `a01935378f6f9fedd6d39145d8fff02b299e04ee` |
| compact discovery runtime | `94659f12da86f240e01d326fed61ef3c54a00bdd` |
| profile hydration runtime | `4b907b1b352e37147fe98235e2cb3c37911e4ec8` |
| compact discovery tests | `42326e9c5fca415ff49a93b058e0901dd0aa6951` |
| profile hydration tests | `c77fc8472b64f1ba455d26dafaf35eee5f79e008` |

## Validation

| Check | Pinned result |
|---|---|
| Targeted compact + hydration tests | 41/41 passed |
| `PYTHONPATH=src python3 -m x_first.contracts` | exit 0; fixture contract result `valid` |
| Ruff on the two runtime and two test modules | passed with `--no-cache` |
| Scoped `git diff --check` | passed |
| Full provider-free discovery, fail-fast | 140 tests passed before one out-of-scope test errored because the deliberately read-only archive mode was copied into that test's writable temporary fixture; no scoped test failed |
| Fabricated hydration-receipt probe | a receipt built without any transcript or ledger input was accepted; changing its unrelated transcript hash was also accepted, both with zero evaluator errors |
| Fabricated compact-receipt probe | two receipts carrying different arbitrary transcript hashes both entered a one-lead union |
| Cross-phase duplicate-handle probe | a runtime-valid two-lead union had one case-folded handle and was rejected by the Phase 3 input-set constructor |
| Cross-phase platform-id probe | hydration returned `valid` with zero errors when its platform id differed from the stable id in the exact discovery union to which it claimed binding |
| Malformed-envelope probe | a nominal projection whose receipt was a mapping raised uncaught `AttributeError` instead of returning a candidate-free invalid result |
| Tracked aggregate receipt | file-byte SHA-256 `0eb5e2cd3db4ed1c5d7e1e35351d3734ee09536d019647105ac97cd6d68b07f0` matches the document; owner-private source hashes were not accessed or re-derived |

The authored green tests establish closed JSON shapes, deterministic authored merges, status normalization, and no
fixed business array cap. They do not establish receipt provenance or close the discovery-to-hydration identity
boundary.

## Findings totals

| Severity | Count |
|---|---:|
| P0 | 0 |
| P1 | 3 |
| P2 | 1 |
| P3 | 1 |

## Findings

### P1-1 — `re-raise` of prior P1-3; trust prerequisite for prior P1-4 — Typed receipts remain caller-authored assertions, not transcript-derived evidence

The new types improve shape but never consume the transcript or ledger whose existence they claim. Compact receipt
validation checks that `transcript_sha256` looks like a digest, recomputes only the parsed result's canonical JSON
digest, and trusts the caller's `terminal_selected_after_last_tool_completion` boolean
(`src/x_first/compact_grok_discovery.py:582-621`). It has no exhaustive tool-event collection at all. Hydration checks
call ids, event-digest shape/uniqueness, and integer ordering, but it receives no raw start/completion events and never
recomputes those digests or proves their inclusion in `transcript_sha256`
(`src/x_first/grok_profile_hydration.py:480-582`). It also cannot prove that the tuple omitted no other native-X call,
failed start, or later terminal. Both test helpers construct these receipts directly from result rows and constants,
rather than through a ledger parser (`tests/test_compact_grok_discovery.py:128-145`,
`tests/test_grok_profile_hydration.py:102-153`).

The synthetic probes supplied no transcript or event ledger. Arbitrary 64-hex event hashes, an arbitrary transcript
hash, invented sequences, and a digest of the model result produced a `valid` hydration evaluation. Replacing the
transcript hash with a second unrelated value remained valid. The compact equivalent entered the union under either
arbitrary transcript hash. Therefore the same model-only answer that the gate is meant to reject can be upgraded by
constructing a dataclass. The documentation's claims of transcript binding, start/completion pairing, exhaustive tool
compliance, and terminal-after-last-completion proof are not yet executable.

Build the receipt from immutable raw transcript/event bytes at one operator-owned boundary: hash those bytes, parse an
exhaustive session ledger, pair start/result events by call id, bind every event digest into the transcript root, prove
the selected raw terminal byte range occurs after the final completion, and reject omitted or unsupported tool
families. The projection consumer must verify that proof or replay the retained ledger; public construction of a
self-consistent summary cannot be the trust boundary.

### P1-2 — `re-raise` of prior P1-2 — Stable platform identity is discarded at hydration, allowing a different account to pass

The compact merge now correctly groups non-null platform ids before handles, but it chooses the canonical handle as
the lexicographic minimum of un-timestamped handle-history proposals
(`src/x_first/compact_grok_discovery.py:912-955`). The hydration expectation then carries only input handles, not the
stable platform ids already known by the discovery union
(`src/x_first/grok_profile_hydration.py:169-180,724-772`). Result validation rejects duplicate platform ids inside the
batch but never compares a returned id with the corresponding discovery identity
(`src/x_first/grok_profile_hydration.py:343-372`).

A synthetic renamed stable identity was merged, its lexicographically selected alias was hydrated, and the hydration
row returned a different non-null platform id. Even though the result claimed the exact discovery-union digest, the
evaluator returned `valid` with zero errors. If an old alias has been reassigned, this can attach the new owner's Bio,
affiliations, links, and verification state to the prior stable account. The union digest is inert because the
evaluator receives no expected identity tuple against which to reconcile it.

Carry a closed `(lead identity, lookup handle, expected stable platform id or explicit provisional state)` expectation
from the exact union into hydration. A non-null id mismatch must quarantine or invalidate the row. Renamed identities
also need an operator-resolved current lookup alias; lexical ordering is deterministic but is not evidence of current
account ownership.

### P1-3 — `new` — The identity policy emits a valid union that its required next phase cannot consume

The merge deliberately keeps a null-id observation and a stable-id observation under the same case-folded handle as
two identities (`src/x_first/compact_grok_discovery.py:912-929,941-955`), and the authored regression test locks in
that result (`tests/test_compact_grok_discovery.py:438-451`). This preserves uncertainty, but both leads serialize the
same lookup handle. Phase 3's canonical input-set constructor rejects case-folded duplicate handles
(`src/x_first/grok_profile_hydration.py:230-238`).

The synthetic probe produced a compact union with zero runtime errors, two `unique_lead_count` entries, and one unique
case-folded handle; constructing its hydration input failed with `expected_handles_invalid`. Deduplicating just before
hydration would silently choose which discovery identity receives the returned row and would leave the union's unique
lead and recall metrics inflated.

Represent the null-id observation as unresolved evidence/conflict state beside the stable identity, or add an explicit
pre-hydration identity-resolution sidecar that retains both proposals while issuing exactly one lookup and defining
how its result reconciles them. A normal union must not be contract-valid yet unrepresentable in the mandatory next
phase.

### P2-1 — `new` — The public hydration evaluator crashes on a malformed typed envelope instead of failing closed

`evaluate_profile_hydration_batch` catches `_assert_projection` failures, but later tests only the outer dataclass type
and unconditionally executes `getattr(projection.receipt, field)`
(`src/x_first/grok_profile_hydration.py:714-749`). Python annotations do not prevent a caller or deserializer from
putting a mapping in that field. The synthetic malformed envelope reached an uncaught `AttributeError` rather than a
candidate-free `{status: invalid, errors: ...}` response.

After projection validation fails, use only locally validated values. Guard every nested envelope member before
access, and turn malformed receipt/result/expectation shapes into stable closed error codes. Add a regression probe
covering wrong nested types, not only forged digest strings inside an otherwise valid helper-built envelope.

### P3-1 — `residual` of prior P3-1 — The aggregate receipt pins claims beside source hashes but does not make their derivation reproducible

The tracked JSON is candidate-free, its byte hash matches the document, and it records six private-source hashes. This
is useful progress. However, it has no closed schema, generator/transform version, field-level source mapping, or
derivation formula. The test checks digest syntax, selected constants, one arithmetic equality, and the diagnostic
label; it neither opens the sources nor re-derives the remaining counts/rates
(`tests/test_compact_grok_discovery.py:158-180`). Arbitrary changes to most metrics can still satisfy that test while
the six source hashes remain unchanged. A pinned reviewer without the private sources can verify only the tracked
receipt's bytes, not the claimed aggregate-to-source transformation.

Keep the diagnostic-only label. For a formal performance claim, generate the receipt with a versioned candidate-free
aggregator that verifies every source hash, emits closed metric provenance/formulas, and has a replay test against
candidate-free source summaries or an owner-run hash-verified audit result.

## Prior finding dispositions

| Prior finding | Disposition at `9b54004` |
|---|---|
| P1-1 campaign/target binding absent | Closed for authored merge/evaluation paths: both result contracts carry the fields, merge requires equality, and hydration compares an owner expectation. Receipt provenance remains blocked under P1-1 above. |
| P1-2 handle-only identity merge | Partially repaired: non-null-id rename grouping and conflicting-id quarantine are deterministic, but stable identity is lost at hydration and the stable-plus-provisional policy is cross-phase invalid; re-raised as P1-2 and P1-3 above. |
| P1-3 loose completed-call dictionary | Raw dictionaries are rejected, but the replacement dataclass is still freely synthesized and unbound to replayable events; re-raised as P1-1 above. |
| P1-4 optional operator projection | Mechanically closed at merge/evaluator entry and record repair is projected. Its claimed operator authority still depends on the unresolved receipt-provenance finding. |
| P2-1 projection after full coherence | Closed: structural/domain validation precedes projection and full coherence follows it. |
| P2-2 unsupported ambiguous axis | Closed: every retained lead now requires both support dimensions, including ambiguous axes. |
| P2-3 contradictory lookup limitations | Closed: the status-specific lookup failure code is exact and other failure codes are rejected. |
| P2-4 missing per-shard membership | Closed for unions produced by the in-memory merge: input projected-result digests and sorted origin ids are retained on history, lead, and reference records. Replaying a serialized union still requires retaining its exact projected inputs. |
| P3-1 unpinned live metrics | Partially repaired by the tracked candidate-free receipt; derivation reproducibility remains P3-1 above. |

## Confirmed properties and residual boundary

- Campaign, descriptor, policy, shard/run/batch, discovery-union, and input-set fields are present and closed in their
  respective JSON schemas; cross-campaign authored merge and expectation tests fail closed.
- Both schemas retain no business `maxItems` cap for leads, references, rows, links, or affiliations. The exact tests
  exercise populations above 100.
- Profile, Bio, self-Post, Reply, quote, mention, official-Post, and thread references remain first-class, and every
  compact lead requires evidence on both independent professional dimensions.
- Recycled non-null platform ids under one alias are kept in separate quarantined groups without merging their source
  references; same-platform-id rename history is deterministic and origin membership survives the union.
- Operator limitation projection now occurs before full status coherence, removes model-owned technical/repair
  claims, and is mandatory at the authored merge/evaluator entrypoints.
- Profile fields and source references remain explicitly `model_mediated_unverified`; no source-bound candidate truth,
  population exhaustion, or formal performance claim was established by this review.
- The tracked aggregate remains diagnostic-only, and the owner-private source availability and interpretation were
  intentionally outside this pinned review.

## Final verdict

NO-GO
