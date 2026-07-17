# Campaign-store authority fixed-forward pinned review

> This artifact reviews pinned commit `b963922a534ba9307e8d72caebb1ad53e9b3ef58` only. It does not review, validate, or characterize any later commit or mutable working-tree implementation.

Date: 2026-07-17

## Evidence header

- Reviewed commit: `b963922a534ba9307e8d72caebb1ad53e9b3ef58`
- Exact parent: `3714c6eb739b2f99c383c7516dbcf377fe841a22`
- Pinned tree: `0037041059e8e52798e6234d097a0d0b908774c0`
- Commit subject: `fix(x-first): bind campaign store authority`
- Relationship: the reviewed commit has exactly the requested parent.
- Exact scope: four changed files under `x-first-researcher-sourcing`; no sibling-repository files were reviewed.
- Scope size: 491 insertions and 79 deletions.
- Scoped binary-diff SHA-256: `cce19949d354ea2487af897a886450ff5e1a417ab9e1197f92c2f2db332a8642`
- Scoped name/status SHA-256: `fc61ac791b445a1a9f20cd17165bea80de84d5014f06815705511d172aa9852e`
- Scoped numstat SHA-256: `eca428ab937b21843933d12f47a17a2f12301f5fd18d99b9630ffc354c06fa73`
- Inspection source: clean detached worktree `/private/tmp/x-first-b963922-review.YfmPaJ` at the exact pinned commit plus pinned Git objects.
- Runtime used for local probes: Darwin 25.3.0 arm64 and the repository's configured Python environment.
- Review boundary: no network, provider, model, credential, owner-private artifact, or Live-X access occurred. The mutable working tree was not used as implementation evidence, and the detached review worktree remained clean.

Changed blob identities:

| Path | Blob |
|---|---|
| `docs/SOURCE_NEUTRAL_MAPPING_CONTROLLER.md` | `656da5050b5b68f5b60fb8f4361c3861097e7fdd` |
| `docs/reviews/2026-07-17-892c76e-source-neutral-mapping-fixed-forward-review.md` | `c439eb6066257d9e1371b5a2fe46568d81b2bc86` |
| `src/x_first/source_neutral_campaign_store.py` | `beb51734cd925af484dd60b78321b43e319fd20e` |
| `tests/test_source_neutral_campaign_store.py` | `5bee83465783cd7c22d673bc5dc6a934c17818cf` |

## Validation

| Check | Pinned result |
|---|---|
| `PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src .../.venv/bin/python -m unittest tests.test_source_neutral_mapping tests.test_source_neutral_campaign_store -v` | 47/47 passed in 7.642s; 26 mapping and 21 campaign-store cases |
| `PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src .../.venv/bin/python -m unittest discover -s tests -v` | 528/528 passed in 65.322s |
| `PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src .../.venv/bin/python -m x_first.source_neutral_mapping` | exit 0; `errors=[]`; `status=valid` |
| `PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src .../.venv/bin/python -m x_first.contracts` | exit 0; `errors=[]`; precision and recall both 1.0 |
| Ruff check for the two changed Python files | passed with Ruff 0.15.11 |
| Ruff format check for the two changed Python files | passed; 2/2 already formatted |
| Scoped `git diff --check` | passed |
| Root-only lock probe | a separate process holding only the store-root directory lock forced the contender to return `campaign_store_lock_busy`; after release, replay succeeded |
| Finite-deadline probe | a configured 50ms root-lock contention deadline returned in 50.836ms |
| Lock-path replacement probe | replacement `.store.lock` did not admit a second writer; the contender returned busy, the active writer failed `store_lock_path_binding_invalid` before journal publication, journal count remained zero, and a post-release retry committed sequence 1 |
| Concurrent-create probe | four simultaneous same-store-id creators all succeeded and replayed one store; two different store ids produced exactly one winner and one `store_manifest_conflict` |
| Object-publication process-exit probe | child exit immediately after `link(temp,target)` left one temp and one object with link count 2; the next open failed `temporary_file_invalid` |
| Journal-publication process-exit probe | child exit immediately after `link(temp,target)` left one temp and one journal entry with link count 2; the next open failed `temporary_file_invalid` |
| Non-finite-timeout probe | under a held root lock, a NaN-timeout contender was still running after 80ms and required termination |

## Findings totals

| Severity | Count |
|---|---:|
| P0 | 0 |
| P1 | 1 |
| P2 | 1 |
| P3 | 1 |

## Findings

### P1-1 — A normal process exit inside append-only publication leaves the store permanently unreplayable

`_publish_new_file` publishes an append-only target by hardlinking a fully fsynced temporary inode to the target and then unlinking the temporary pathname (`src/x_first/source_neutral_campaign_store.py:1108-1124`). During the interval after `os.link(temp, target)` and before `temp.unlink()`, both names legitimately reference one regular inode with `st_nlink == 2`. The new cleanup contract rejects every temporary file whose link count is not exactly one before it can remove that publication alias (`src/x_first/source_neutral_campaign_store.py:1158-1184`). Authority reads also correctly reject a target while its second link remains.

Two forked-process probes exited the child immediately after the real `os.link` call: one during content-addressed object publication and one during journal publication. Both produced exactly one temp and one target with link count two. In both cases, a fresh `CampaignStore.open` failed `temporary_file_invalid`; no API could replay or repair the otherwise structurally recognizable publication state.

The tracked crash tests do not exercise this interval. Their fault points run only after `_publish_new_file` has already removed the temporary name and fsynced both directories (`tests/test_source_neutral_campaign_store.py:402-435`). Therefore the documentation's statement that crash tests cover orphan bundles and committed journal entries (`docs/SOURCE_NEUTRAL_MAPPING_CONTROLLER.md:122-127`) is incomplete for the actual publication primitive. This is a regression from adding the unconditional temporary-file single-link rejection: the parent implementation's cleanup could remove the retained alias before authority reads.

The publication protocol needs a mechanically bound recovery rule for its own two-link intermediate state, without accepting unrelated hardlinks, or a different durable no-replace protocol. Regression coverage must terminate a separate process at the post-link/pre-unlink object and journal boundaries and prove deterministic reopen, replay, and exact-retry behavior.

### P2-1 — Non-finite lock timeouts bypass the promised monotonic acquisition boundary

`_GlobalLock` rejects Boolean, nonnumeric, and negative values, but accepts both NaN and infinity because neither satisfies `timeout_seconds < 0` (`src/x_first/source_neutral_campaign_store.py:336-358`). With NaN, the deadline and remaining interval stay NaN, `remaining <= 0` never becomes true, and the loop continues sleeping. A forked contender configured with NaN remained blocked after 80ms under a held root lock and had to be terminated. Infinity has the same unbounded-deadline effect by construction.

The finite positive-path test proves the normal default mechanism but does not cover non-finite numeric values (`tests/test_source_neutral_campaign_store.py:276-284`). The public create/open boundary must reject every non-finite timeout before opening or locking any authority path, and its regression test should cover NaN, positive infinity, and negative infinity.

### P3-1 — The filesystem and operating-system support boundary is implicit rather than executable or documented

The store imports `fcntl`, acquires exclusive advisory locks on a directory descriptor and a regular-file descriptor, requires hardlink publication, compares effective UID and POSIX permission bits, and fsyncs directory descriptors (`src/x_first/source_neutral_campaign_store.py:17-24,45-91,327-452,1108-1124,1186-1194`). These are coherent on the reviewed Darwin local filesystem, but they do not form a cross-platform Python contract: Windows cannot import the module's `fcntl` dependency, and distributed or non-POSIX filesystems need not provide the assumed directory-lock, hardlink, ownership, or durability semantics.

The scoped documentation describes POSIX link-count behavior but does not define the supported filesystem class, reject an unsupported runtime through a stable preflight, or provide a second-platform result (`docs/SOURCE_NEUTRAL_MAPPING_CONTROLLER.md:129-136`). This review therefore validates Darwin local-filesystem behavior only. Before broader deployment claims, the store should either declare and fail-fast on an explicit local-POSIX support boundary or supply a portable locking/publication backend with platform-specific tests.

## Prior-finding dispositions

1. **Lock-path replacement and duplicate writer lane — closed.** The store-root directory inode is acquired first and remains the serialization anchor. In a separate-process replacement probe, a newly created canonical `.store.lock` did not admit the contender because the original writer still held the root inode. The contender returned `campaign_store_lock_busy`; once released, the original writer detected that its lock descriptor no longer matched the canonical pathname and failed before journal publication. The empty journal then accepted exactly one sequence-1 retry.
2. **Authority ownership, link count, and mode — closed for static authority inputs.** The pinned code binds root/lock descriptors to canonical path device and inode, requires effective-UID ownership and exact modes, rejects hardlinked lock/manifest/journal/object/head files, rejects an existing public-mode lock without repairing it, and validates newly created temp files as owner-only regular files. Directory link counts are correctly not fixed to one. The P1 finding above is a distinct crash-recovery regression created by applying the static temp single-link rule to the publication protocol's own transient hardlink alias.

## Expected residuals

- Phase 1 still accepts caller-supplied `ValidatedWaveBundle` and `DirectProof` inventory. It does not derive either from raw mapping, hydration, or Luna projections.
- The mapping controller and `structural_stop` do not yet use the durable store as their sole authority. Diagnostic sibling roots and store-independent stopping therefore remain outside this fixed-forward's closure claim.
- Temporal candidate snapshots, receipt-first Luna transition authority, explicit cache-consumer binding, and executable source-neutral saturation/challenger strategy v2 remain promotion blockers.
- Tail rollback detection remains witness-dependent: coordinated deletion of both a journal tail and every materialized witness requires an external monotonic anchor for stronger guarantees.
- Only local Darwin filesystem behavior was exercised. No owner-private calibration file, historical Live artifact, network service, provider, or model was opened or revalidated.

## Final verdict

NO-GO
