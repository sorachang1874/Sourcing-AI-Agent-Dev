# Source-neutral mapping fixed-forward pinned review

> This artifact reviews pinned commit `892c76e91079e5c1e9f01e40f22e26bb8d29b7d0` only. It does not review, validate, or characterize the current lock fixed-forward working tree.

Date: 2026-07-17

## Evidence header

- Reviewed commit: `892c76e91079e5c1e9f01e40f22e26bb8d29b7d0`
- Exact parent: `da396e62151bcb824036574a6433c845993049a6`
- Pinned tree: `605dbb8b79a7bc1f27b08c2457a5664a8047ad6d`
- Commit subject: `fix(x-first): harden mapping evidence lineage`
- Relationship: the reviewed commit has exactly the requested parent.
- Exact scope: 10 changed files under `x-first-researcher-sourcing`; no sibling-repository files were reviewed.
- Scope size: 1,741 insertions and 34 deletions.
- Scoped binary-diff SHA-256: `421476699b1e02003a5230ea5ed8393e0cbbafea6050d831a50f10a170bc934e`
- Scoped name/status SHA-256: `e3926f340a58928f86ef7ab9a4853c577744f180fd5b98fcea2ac2cd9e76b797`
- Scoped numstat SHA-256: `4ab1f2a9d142f417b7baaef426190f435b174bf77c6364265cd04eb24bff0c28`
- Inspection source: clean detached worktree `/private/tmp/x-first-892c76e-review.8Et3Ey` at the exact pinned commit plus pinned Git objects.
- Review boundary: no network, provider, model, credential, private artifact, or live-X access occurred. The mutable working tree was not used as implementation evidence, the detached worktree remained clean, and the review made no implementation edit.

Changed blob identities:

| Path | Blob |
|---|---|
| `contracts/x.source_neutral.mapping.candidate_manifest.v1.schema.json` | `4cc4db44fa958ea53971b92b3deda814e623244d` |
| `contracts/x.source_neutral.mapping.exact_post_hydration.v1.schema.json` | `8ef12bdf30d39d94bcdb6e7e0870736e7f1324eb` |
| `contracts/x.source_neutral.mapping.luna_state_review.v1.schema.json` | `39cfa799b79f12cab95e5439e2f4e83997f30ffc` |
| `contracts/x.source_neutral.mapping.session_receipt.v1.schema.json` | `58abe5875c09c4b801fcb6e5ee6734ff9aa82394` |
| `docs/SOURCE_NEUTRAL_MAPPING_CONTROLLER.md` | `be99a4c137f17cd927cb0ee0ca6c39e3b7e65d6a` |
| `docs/reviews/2026-07-17-da396e6-source-neutral-mapping-round2-review.md` | `0905d55f763a47ebe3b3742861cc5351e02df311` |
| `src/x_first/source_neutral_campaign_store.py` | `20b4d42b7eaf87cd7bb6c83873f42f3d08dd9d89` |
| `src/x_first/source_neutral_mapping.py` | `3bb2ffe1aae565689c62198338f73775233202cd` |
| `tests/test_source_neutral_campaign_store.py` | `2c555c3d6630c9b795018ce9c050a839542faadb` |
| `tests/test_source_neutral_mapping.py` | `f0706c3368ef565d645537f899f1c797fbc3f088` |

## Validation

| Check | Pinned result |
|---|---|
| `PYTHONPATH=src .../.venv/bin/python -m unittest tests.test_source_neutral_mapping tests.test_source_neutral_campaign_store -v` | 40/40 passed in 6.395s; 26 mapping and 14 campaign-store cases |
| `PYTHONPATH=src .../.venv/bin/python -m unittest discover -s tests -v` | 521/521 passed in 108.864s |
| `PYTHONPATH=src .../.venv/bin/python -m x_first.source_neutral_mapping` | exit 0; `errors=[]`; `status=valid` |
| `PYTHONPATH=src .../.venv/bin/python -m x_first.contracts` | exit 0; `errors=[]`; precision/recall both 1.0 |
| Ruff check for the four changed Python files | passed |
| Ruff format for the four changed Python files | passed; 4/4 already formatted |
| Scoped `git diff --check` | passed |
| Duplicate hydration and omission probes | duplicate task rejected as `exact_hydration_projection_duplicate`; omitted queued task rejected as `executed_wave_hydration_incomplete` |
| Diagnostic lineage probe | two ordinal-1 siblings accepted; one-ID sibling could be omitted while the zero branch continued; identical direct execution projections were accepted as another campaign root |
| Temporal reducer probe | complete `current + unsupported` proposals reduced both diagnostic axes to `ambiguous` |
| Alias-normalization probe | NFKC, case, whitespace, and order variants produced the same signature; normalized collision rejected |
| Lock-inode swap concurrency probe | a replacement lock admitted writer B while writer A still held the original inode; B returned a commit, then A produced a second sequence-1 journal and the store became unreplayable |
| File-boundary probe | a hardlinked journal with `st_nlink=2` replayed; a pre-existing `0666` lock was silently repaired and accepted |

## Findings totals

| Severity | Count |
|---|---:|
| P0 | 0 |
| P1 | 1 |
| P2 | 1 |
| P3 | 0 |

## Findings

### P1-1 — Lock-path replacement defeats store-global serialization and can corrupt an acknowledged commit

`_GlobalLock` opens and locks whichever inode the pathname names at that instant, then checks only that the opened FD is a regular file. It never checks current ownership, link count, or that the canonical pathname still names the same device/inode after acquisition (`src/x_first/source_neutral_campaign_store.py:286-313`). Every append then treats that inode lock as store-global authority while replaying the current journal and publishing the next global sequence (`src/x_first/source_neutral_campaign_store.py:467-520`).

The adversarial probe paused writer A at `after_bundle_publish_before_journal` while it still held the original lock, unlinked and recreated `.store.lock`, and let writer B enter through the replacement inode. Writer B returned committed head `1a6b7246c5d7661f44dc6ec7fccbe4a4756829b8e7967ac827316a13d1ca6391`. Writer A then resumed and published another global sequence `1`. The authoritative journal ended with two distinct sequence-1 files; A failed `journal_sequence_gap`, and every subsequent open/replay failed with the same corruption.

This invalidates the Phase-1 claims of one store-global lock, successor compare-and-swap, and one append-only global journal: a successful commit can become unreadable after another writer enters through a replacement inode. Anchor the lock to a stable owner-only directory/inode, verify pathname-to-FD device/inode identity plus current UID and `nlink == 1` before and after acquisition, and add a held-lock inode-swap concurrent-append regression.

### P2-1 — Private authority accepts hardlinked or non-owner files and silently repairs an unsafe lock mode

Directory validation checks only type, symlink status, and numeric mode (`src/x_first/source_neutral_campaign_store.py:560-580`). Authority-file reads check only regular-file type and exact `0600`, not `st_uid == os.getuid()` or `st_nlink == 1` (`src/x_first/source_neutral_campaign_store.py:875-898`). Lock acquisition calls `fchmod(0600)` before validating the opened file and likewise omits ownership and link-count checks (`src/x_first/source_neutral_campaign_store.py:291-298`). The positive test asserts modes only (`tests/test_source_neutral_campaign_store.py:104-121`).

An authoritative journal file with an external hardlink and `st_nlink=2` replayed successfully. A pre-existing lock broadened to `0666` was silently changed back to `0600` and accepted. This leaves authoritative bytes mutable through an untracked alias and conceals a pre-existing permission violation. Require current-UID ownership and one-link authority for the lock, manifest, journal, objects, heads, and temporary files; require current ownership for every store directory; reject rather than repair an existing unsafe lock; and add hardlink, owner, and mode negative tests.

## Prior-finding dispositions

1. **Duplicate hydration substitution and exact task-set closure — closed.** A duplicate task with fresh session, request, and projection identities fails `exact_hydration_projection_duplicate`. Omitting another queued task from a complete wave fails `executed_wave_hydration_incomplete`. Aggregate hydration coverage now counts unique validated tasks.
2. **Sibling fork, omitted nonzero sibling, and cross-campaign proof reuse — explicitly deferred, not closed in the mapping controller.** An independent probe still built two ordinal-1 siblings, omitted a sibling that added one stable Post while continuing the zero branch, and reused the root's exact mapping/hydration/Luna execution projections as another campaign root. The Phase-1 store rejects a stable-path sibling CAS loser and caller-declared proof-digest reuse, but it does not derive or admit mapping facts. The controller documentation accurately keeps this as a promotion blocker (`docs/SOURCE_NEUTRAL_MAPPING_CONTROLLER.md:103-134`).
3. **Cross-wave source/candidate ownership — closed.** The cumulative frontier now retains `(stable_post_id, candidate_ref, casefolded author_handle)` and rejects reassignment. Exact manifest equality across predecessors preserves the candidate's platform-user binding.
4. **Temporal no-signal reduction — retained expected residual.** Hydration still carries no publication time, observation time, or adjudication `as_of`; a complete `current + unsupported` proposal set still reduces to `ambiguous`. This is bounded because the output is diagnostic-only, resolved state remains the frozen prior, and no transition is authorized.
5. **Commutative OR normalization and collision — closed.** Reordering plus NFKC fullwidth forms, case changes, and whitespace changes preserved the strategy signature. Two aliases that normalize to one value failed `strategy_alias_normalized_duplicate`.
6. **Canonical positive decimal IDs — closed.** The four changed schemas and runtime parser require `[1-9][0-9]{0,31}`; zero and leading-zero aliases cannot enter the frontier or hydration lane.
7. **Saturation/challenger execution representation — retained expected residual.** Executed strategy facts remain hardcoded to Wave P, candidate-authored/self-authored topology, and no time window. Saturation children and challenger strategies still lack an executable, replayable source-neutral plan. The documentation lists general strategy v2 as an explicit promotion blocker.

## Confirmed controls and explicit limitations

- `ValidatedWaveBundle` and `DirectProof` are intentionally caller-asserted in Phase 1. The documentation clearly states that the store neither derives them from raw mapping/hydration/Luna projections nor supplies authoritative stopping.
- With an unchanged lock pathname, the pinned implementation correctly enforced global hash-chain replay, per-campaign parent CAS, mutation idempotency, wave-ID uniqueness, global proof-digest collision, stale-head repair, sequence-gap detection, and both injected crash boundaries.
- Proof identity is conservatively global by digest: the same digest under a second proof kind is rejected rather than treated as a distinct namespace. Semantic proof derivation and explicit cache-consumer projections remain Phase-2 work.
- Tail rollback is detected only while a materialized global-head witness remains. Deleting both the journal tail and every materialized head witness rolled a two-wave probe back to one wave. The documentation's `witnessed` qualifier is accurate; stronger coordinated-deletion resistance requires an external monotonic anchor.
- The mapping controller still admits diagnostic siblings and independent roots because `structural_stop` does not read the store. The pinned commit does not claim that Phase-1 storage clears this integration blocker.
- Receipt-first Luna authority, temporal candidate snapshots, cache reuse, and executable saturation/challenger strategy v2 remain explicit blockers rather than silently promoted capabilities.
- Owner-private calibration files and historical Live artifacts were not opened or revalidated.

## Final verdict

NO-GO
