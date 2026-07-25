# Campaign-store publication-order fixed-forward pinned review

> This artifact reviews pinned commit `f38692870246ff8dfa4fab79106492910ac4fac6` only. It does not review, validate, or characterize any later commit or mutable working-tree implementation.

Date: 2026-07-17

## Evidence header

- Reviewed commit: `f38692870246ff8dfa4fab79106492910ac4fac6`
- Exact parent: `b963922a534ba9307e8d72caebb1ad53e9b3ef58`
- Pinned tree: `95e07e0f68d6d1071c82caf915e99854963c745f`
- Commit subject: `fix(x-first): recover interrupted store publication`
- Relationship: the reviewed commit has exactly the requested parent.
- Exact scope: four changed files under `x-first-researcher-sourcing`; no sibling-repository files were reviewed.
- Scope size: 387 insertions and 5 deletions.
- Scoped binary-diff SHA-256: `e3e8fce7b60759d41e7bea4821626edba49955b01a03ea97683ef6e151193a38`
- Scoped name/status SHA-256: `305fd9f2b1e2f764647c70812dc264eac035ff18fd263175375109edb0766c55`
- Scoped numstat SHA-256: `d8e722627813441a7db1ade742e3bc04017bfd04a5a9a352c9aea37bdb5e09b8`
- Inspection source: clean detached worktree `/private/tmp/x-first-f386928-review.N8Hqqx` at the exact pinned commit plus pinned Git objects.
- Runtime used for local probes: Darwin 25.3.0 arm64 and the repository's configured Python environment.
- Review boundary: no network, provider, model, credential, owner-private artifact, or Live-X access occurred. The mutable working tree was not used as implementation evidence, and the detached review worktree remained clean.

Changed blob identities:

| Path | Blob |
|---|---|
| `docs/SOURCE_NEUTRAL_MAPPING_CONTROLLER.md` | `84d3103a87cdd9ac6d2b921902e8a292f4fbc97d` |
| `docs/reviews/2026-07-17-b963922-campaign-store-authority-review.md` | `bbac5454307555e6dd113394e16823281b5e7963` |
| `src/x_first/source_neutral_campaign_store.py` | `662bf3e36845a9aed8aef0943a01167c0147b822` |
| `tests/test_source_neutral_campaign_store.py` | `775ee65381add5e4726041b8b7c665bceea770bf` |

## Validation

| Check | Pinned result |
|---|---|
| `PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src .../.venv/bin/python -m unittest tests.test_source_neutral_mapping tests.test_source_neutral_campaign_store -v` | 52/52 passed in 7.539s; 26 mapping and 26 campaign-store cases |
| `PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src .../.venv/bin/python -m unittest discover -s tests -v` | 533/533 passed in 105.549s |
| `PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src .../.venv/bin/python -m x_first.source_neutral_mapping` | exit 0; `errors=[]`; `status=valid` |
| `PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src .../.venv/bin/python -m x_first.contracts` | exit 0; `errors=[]`; precision and recall both 1.0 |
| Ruff check for the two changed Python files | passed with Ruff 0.15.11 |
| Ruff format check for the two changed Python files | passed; 2/2 already formatted |
| Scoped `git diff --check` | passed |
| Post-link/pre-unlink object probe | real child exit left a two-link temp/target intermediate; open removed only the temp alias, retained a single-link object, and an exact retry committed one sequence-1 wave |
| Post-link/pre-unlink journal probe | real child exit left a two-link temp/target intermediate; open retained the journal as committed, removed only the temp alias, repaired the head, and exact retry returned the same wave |
| Manifest intermediate probe | real child exit at the manifest link recovered to one single-link manifest; open and same-id create retry returned the original store |
| Recovery re-exit probes | exit after target-directory fsync but before temp unlink replayed idempotently; exit after unlink but before temp-directory fsync reopened with the target single-linked and no temp |
| Invalid-alias probes | non-target alias, mismatched bytes/hash, mismatched target, and a third hardlink all failed closed without accepting an authority row |
| Rehashed semantic-invalid journal probe | recovery preserved the exact bytes at the target and removed only the duplicate temp name; replay then rejected `journal_intent_hash_invalid` and admitted no wave |
| Non-finite timeout probe | Boolean, negative, NaN, both infinities, and an integer overflowing float conversion failed `lock_timeout_invalid` before create made a root or open accessed layout |
| Post-unlink/pre-target-fsync object probe | child exit left object count 1, temp count 0, and journal count 0; open plus exact append returned sequence 1 while no `objects` directory fsync occurred |
| Post-unlink/pre-target-fsync journal probe | child exit left journal count 1, temp count 0, and head count 0; open plus exact retry returned sequence 1 while no `journal` directory fsync occurred |
| Post-unlink/pre-target-fsync manifest probe | open returned the stored manifest with no root-directory fsync |

## Findings totals

| Severity | Count |
|---|---:|
| P0 | 0 |
| P1 | 1 |
| P2 | 0 |
| P3 | 0 |

## Findings

### P1-1 — Normal publication deletes its recovery marker before the authoritative directory entry is durable

The new recovery path uses the correct durable order: validate the two-link intermediate, fsync the target directory, revalidate, unlink only the temporary alias, verify the target has become single-link, and fsync the temp directory (`src/x_first/source_neutral_campaign_store.py:1265-1305`). Normal `_publish_new_file` still uses the opposite critical order. It links the temp inode to the target, then its `finally` block unlinks the temp name, and only afterward fsyncs the target and temp directories (`src/x_first/source_neutral_campaign_store.py:1125-1142`).

A process exit after the unlink and before target-directory fsync therefore leaves a visible single-link target but no marker proving that its directory entry reached stable storage. A target-directory fsync error creates the same state because the `finally` block has already removed the temp alias. Neither `_cleanup_temps_unlocked` nor normal replay can distinguish that target from a fully durable one.

Three separate forked probes exercised this exact adjacent window by exiting immediately after the real temp unlink:

- The object probe left one object, no temp, and no journal. A fresh open plus exact append returned a committed sequence-1 wave. Instrumented directory fsync calls were `journal`, `tmp`, `heads`, and `tmp`; `objects` was never fsynced before the journal commitment became durable.
- The journal probe left one journal entry, no temp, and no materialized head. A fresh open repaired the head and exact retry returned the existing sequence-1 wave. Instrumented directory fsync calls were only `heads` and `tmp`; `journal` was never fsynced.
- The manifest probe left one manifest and no temp. A fresh open returned the store while performing zero directory fsync calls.

Fsync of the temporary file before hardlinking establishes inode bytes, not the later target directory entry. Thus a subsequent power loss may discard the un-fsynced object, journal, or manifest link after open/retry has already acknowledged it. The object case can leave a durable journal referencing an object link that disappears, making authoritative replay corrupt. The new process-exit tests stop only at the injected post-link/pre-unlink point (`tests/test_source_neutral_campaign_store.py:81-99,487-522`), so they cannot detect this no-marker window.

Normal publication must use the same ordering as recovery: link the target, durably fsync its directory, revalidate the temp/target binding, unlink the temp alias, and fsync the temp directory. If target-directory fsync fails, the two-link marker must remain for retry rather than being removed in an unconditional `finally`. Regression coverage must terminate a separate process after unlink at each manifest/object/journal publication and prove that no successful open or exact retry depends on an un-fsynced authority directory.

## Prior-finding dispositions

1. **Post-link/pre-unlink process-exit recovery — closed for the exact two-link state.** Real child exits for object and journal publication recovered correctly, and an independent manifest probe did the same. The recovered target was single-link, the temp alias was removed, object exact retry committed once, and journal exact retry returned the original commitment. A second process exit during recovery both before and after temp unlink remained idempotently recoverable.
2. **Non-target alias, wrong bytes, and extra links — closed.** A temp linked only to a non-target path, a claimed object with mismatched bytes/hash, a separate inode at the expected target, and an exact temp/target pair with a third link all failed without deleting the retained evidence or admitting authority. A top-hash-valid but intent-invalid journal kept identical bytes at the authoritative target, removed only the duplicate temp alias, and then failed replay with `journal_intent_hash_invalid`; no wave was admitted.
3. **Non-finite timeout boundary — closed.** `_normalize_lock_timeout` rejects non-finite, negative, Boolean, and overflow values in `CampaignStore.__init__`, before create/open reaches layout preparation or validation (`src/x_first/source_neutral_campaign_store.py:48-57,471-480`). The regression matrix proves invalid create leaves no root.
4. **Filesystem and operating-system support boundary — closed as a declaration.** The documentation now limits this backend to local POSIX filesystems with working `fcntl.flock`, hardlinks, effective UID/permission bits, and file/directory fsync, records Darwin as the tested environment, and explicitly excludes Windows and network/distributed filesystem claims (`docs/SOURCE_NEUTRAL_MAPPING_CONTROLLER.md:148-151`). No cross-platform result is implied.

## Expected residuals

- Phase 1 still accepts caller-supplied `ValidatedWaveBundle` and `DirectProof` inventory. It does not derive either from raw mapping, hydration, or Luna projections.
- The mapping controller and `structural_stop` do not yet use the durable store as their sole authority. Diagnostic sibling roots and store-independent stopping remain outside this fixed-forward's closure claim.
- Temporal candidate snapshots, receipt-first Luna transition authority, explicit cache-consumer binding, and executable source-neutral saturation/challenger strategy v2 remain promotion blockers.
- Tail rollback detection remains witness-dependent; coordinated deletion of both a journal tail and every materialized witness requires an external monotonic anchor for stronger guarantees.
- Runtime filesystem capability detection remains operator-owned inside the declared local-POSIX support boundary. Only local Darwin behavior was exercised.
- No owner-private calibration file, historical Live artifact, network service, provider, or model was opened or revalidated.

## Final verdict

NO-GO
