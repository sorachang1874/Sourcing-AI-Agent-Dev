# Pinned independent adversarial re-review — Stage 2A field capability

## Evidence header

- Verdict: `NO-GO`
- Reviewer: non-author, adversarial, read-only.
- Base: `a2d33f5444df68ecaad8e28a8a2c3b475dab321f`
- Reviewed: `b79df16654ec22ac4dd3bcd4ca5a49871696fb2c`
- Reviewed commit has the stated base as its immediate parent.
- Reviewed tree: `ce71cc3c7aa5171e0f8ca549dbcbf7e69db1bff2`
- Binary diff SHA-256: `0fb45793b538719188cd780205385757d33d2c57b1a31c86daa2f41f917f1451`
- Scope: 10 files, 2,002 insertions, 619 deletions.
- Detached clean worktree: `/private/tmp/x-first-stage2-rereview-b79df16`
- No live call, provider/model invocation, network access, credentials, repository edit, staging, or commit occurred.

## Validation

- Python 3.12.13 focused Stage 2: 45/45 passed.
- Python 3.14.4 focused Stage 2: 45/45 passed.
- Python 3.12.13 full offline suite: 317/317 passed.
- Python 3.14.4 full offline suite: 317/317 passed.
- Deterministic Stage 2 generator check passed on both Python versions.
- Ruff 0.15.11 lint passed; three relevant Python files were already formatted.
- Compileall passed on both Python versions.
- Six Stage 2 JSON artifacts parsed successfully.
- `git diff --check` passed.
- Head, tree, binary-diff hash, direct-parent relationship, and clean detached state matched the review request.

These green checks do not override the blocking adversarial results below. Both P1 findings were reproduced with complete, internally coherent mutations that recomputed every affected source, receipt, quarantine, incident, collection, profile, and evaluation identity.

## Findings

Severity totals: P0=0, P1=2, P2=1.

### P1-1 — Re-raise of prior P1-5: Post-only mappings bypass collection-wide handle/ID conflicts

The collection-wide reducer correctly marks all tasks participating in one-ID/multiple-handle or one-handle/multiple-ID evidence as conflicted. It passes that fact into `_derived_task_guardrail`.

However, the profile-free Post branch returns `profile_source_unavailable_post_retained` before consulting `cross_handle_conflict`. Only the profile branch checks that flag.

Evidence:

- `src/x_first/stage2_field_capability.py:1193-1240`
- `src/x_first/stage2_field_capability.py:2031-2085`
- `src/x_first/stage2_field_capability.py:2410-2418`
- `docs/STAGE2_FIELD_CAPABILITY_CONTRACT.md:150-154`

Adversarial reproduction:

1. The existing `conflicting_platform_user_ids` profile source was coherently changed to use the Post-only task’s handle at the exact same observation timestamp.
2. Its existing multi-ID precedence remained `conflicting_platform_user_ids`, so its request-frozen expectation did not need relabelling.
3. The Post-only row exposed the same handle under a different stable platform ID.
4. All affected source, receipt, quarantine, incident, task-row, retention, collection, and evaluation identities were recomputed.
5. `validate_collection` returned zero errors.
6. `validate_fixture_bundle` returned zero errors.
7. The evaluation remained `offline_fixture_expectation_conformant`.
8. `unquarantined_identity_conflict` remained hardcoded to zero.
9. The Post-only mapping remained `completed_post_only` rather than quarantined.

This directly contradicts the documented rule that every affected mapping is quarantined.

Required remediation:

- Apply collection-wide `cross_handle_conflict` before the Post-only success return.
- Derive the evaluator’s identity-conflict guardrail from the account reducer instead of hardcoding zero.
- Add a regression combining a Post-only source with a same-time conflicting profile or Post mapping, including a peer whose higher-priority local quarantine remains unchanged.

### P1-2 — Re-raise of prior P1-1: credential and live-URL privacy validation remains bypassable

Raw record shapes are now closed, and HTTP(S) live URLs plus a small token vocabulary are rejected. The privacy scanner, however, recognizes only `http://`/`https://` URLs and a narrow list of credential patterns.

Evidence:

- `src/x_first/stage2_field_capability.py:144-152`
- `src/x_first/stage2_field_capability.py:913-934`
- `docs/STAGE2_FIELD_CAPABILITY_CONTRACT.md:145-148`

A coherently rehashed exact profile Bio containing all of the following remained fully conformant:

- an OpenSSH private-key marker;
- a `ghp_...` credential-shaped token;
- `ftp://x.com/live-user`.

After recomputing the raw/source SHA, source ID, receipt ID, normalized Bio hash, profile ID, task-row references, retention manifest, collection ID, and evaluation, `validate_fixture_bundle` returned zero errors and the decision remained `offline_fixture_expectation_conformant`.

This violates the documented claim that credential-like text and live URLs reject before persistence.

Required remediation:

- Recognize common high-confidence secret families and private-key markers.
- Treat URI schemes generically, permitting only explicitly reserved fixture URI forms rather than scanning HTTP(S) alone.
- Add coherent full-bundle regression cases, not only mutations with unexpected metadata keys.

### P2-1 — New: retention TTL is incorrectly also a Post-age limit

`_retention_contract_violations` requires `post_authored_at` to fall between raw-storage `created_at` and `delete_after`.

Evidence:

- `src/x_first/stage2_field_capability.py:1411-1443`
- `docs/STAGE2_FIELD_CAPABILITY_CONTRACT.md:175-177`
- `docs/STAGE2_FIELD_CAPABILITY_CONTRACT.md:193-199`

A Post authored before evidence acquisition is normal, especially when sourcing historical pretraining experience. Requiring its authorship time to fall inside a 24-hour storage interval rejects otherwise valid historical Posts and turns an operational evidence-retention TTL into a business content-age cap.

The retention finding from the prior review is mechanically closed—the evaluator now derives violations—but the selected timestamps have the wrong semantics.

Recommended remediation:

- Bind retention to acquisition and storage timestamps: receipt start/completion, source observation, profile observation, and any explicit persistence timestamp.
- Continue enforcing `post_authored_at <= source.observed_at`.
- If a campaign needs a content-time window, validate that separately against the experiment/search contract rather than the storage TTL.

## Prior finding disposition

- Prior P1-1 raw privacy: partially remediated, re-raised by P1-2.
- Prior P1-2 quarantined-record deep validation: closed.
- Prior P1-3 native-X provenance: closed for the reviewed offline generic-web/result-type attack.
- Prior P1-4 Post-only terminal state: closed locally.
- Prior P1-5 bidirectional handle/ID conflict: partially remediated, re-raised by P1-1.
- Prior P1-6 retention/evaluator derivation: mechanically closed; P2-1 records the new timestamp-semantics defect.
- Prior P1-7 cross-experiment identities: closed for target, window, registry, selection, technical-limit, retention-policy, and authority changes.
- Prior P2-1 strict bool/int handling: closed.
- Prior P2-2 profile-helper source identity: closed.

## Verified non-findings

- Exact raw shapes now apply before terminal-state derivation, including quarantined and metadata-only sources.
- Generic-web transport and mismatched result-type relabelling reject.
- Post-only evidence has a typed terminal state and retains only source-bound Post fields.
- Task identity changes with target lab, frozen window, and reviewed interpretation digests.
- Nested boolean/integer aliases reject.
- `validate_profile_source_binding` now validates raw hash and source identity.
- The committed fixture remains deterministic, synthetic, `.invalid`-only, and provider-free.
- Authority, canonical-write, and outreach surfaces remain closed.

## Promotion boundary

Commit `b79df16654ec22ac4dd3bcd4ca5a49871696fb2c` must not become the accepted Stage 2A capability contract, support a live field canary, or receive milestone/manual signoff until both P1 findings are remediated and independently re-reviewed on a new pinned commit.

The P2 retention correction should be included in that bounded remediation because leaving it in place would force a contract rewrite before any realistic historical-Post canary.

Unrelated offline development remains unblocked. Even after remediation, approval would establish deterministic offline fixture semantics only—not live X transport capability, search quality, real purge ownership, Batch behavior, or product authority.

NO-GO
