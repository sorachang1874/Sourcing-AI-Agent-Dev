# Query-policy protected boundary and issuance-history pinned review

## Evidence header

- Verdict: `NO-GO`
- Reviewer: non-author, adversarial, read-only.
- Base: `2700d10833463e822ff5b7d80debd9991d1fa244`
- Reviewed: `dccb61f19cddb756461de06beb029641349017aa`
- Reviewed commit parent matches the stated base.
- Tree: `4ce8be49e16483badb37836144e5eff0a63fe961`
- Binary diff SHA-256: `c6749e69916c97950b3281e0211f0c72aaa1928e5429915fc977cce41b492d59`
- Scope: 10 files, 392 insertions, 20 deletions.
- Detached clean worktree: `/private/tmp/x-first-query-policy-review-dccb61f`
- No current dirty adaptive/Stage2 files were read.
- No network, live/provider/model call, credential use, edit, staging, or commit occurred.

## Validation

- Python 3.12.13 targeted query-policy/migration suite: 41/41 passed.
- Python 3.14.4 targeted query-policy/migration suite: 41/41 passed.
- Six focused schema, privacy, lineage, prefix, and protected-query tests passed on both Python versions.
- Six changed JSON config/schema files parsed successfully.
- Ruff lint passed.
- Compileall passed on both Python versions.
- `git diff --check` passed.
- Ruff 0.15.11 format check failed on three changed Python files; the same files pass at the base commit.

Severity totals: P0=0, P1=3, P2=3.

## P1 findings

### P1-1 — Exact-prefix validation is not a global append-only snapshot chain

The runtime compares only the selected registry lineage with `history[:len(lineage)]`. It does not inspect sibling snapshots, bind a predecessor snapshot, or require the newest admitted snapshot to cover the full canonical history.

Two mutations reproduced the gap:

1. With canonical history `[row0,row1]` and a valid full v3 predecessor, a later v4 snapshot containing only `[row0]` was accepted.
2. With a full v3 snapshot still present but canonical history truncated to `[row0]`, selecting the old v2 prefix was accepted.

Selecting a full v3 snapshot against truncated/reordered history correctly rejected, and an old v2 exact prefix against intact longer history correctly replayed. An explicit monotonic snapshot/predecessor chain must validate the complete admitted set before selection; the latest head must cover the complete history.

### P1-2 — Issuance history does not bind immutable policy content

The canonical history row binds policy version, issuance, run, key, and nonce but not descriptor SHA-256, manifest digest, policy path, or protected-boundary version.

A v2 and v3 snapshot were both accepted with identical policy/issuance/run/key/nonce identities but different descriptor hashes and exact query manifests. Bind `policy_sha256` and semantic/version digests into every history row; inherited rows must reproduce immutable policy identity byte-for-byte.

### P1-3 — Common direct protected operands still pass

Full coherent descriptor/registry/HMAC mutations rejected `women`, `Black`, `disabled`, and `gay`, but accepted `LGBTQ`, `autistic`, and `wheelchair users`.

Extend the versioned concept/value registry and its multilingual regression matrix. The same shared predicate governs supporting evidence, so common protected synonyms must also be blocked there.

## P2 findings

### P2-1 — Neutral technical queries are rejected by polysemous tokens

Coherently approved `race condition`, `blind evaluation`, `white paper`, and `straight-through estimator` were rejected. Basic pretraining and China/Asia professional-experience queries were accepted.

Use exact reviewed neutral collocations or phrase context bound to query commitments; do not use a broad global allowlist.

### P2-2 — Deterministic issuance-ID derivation is not verified

The migration owner derives the ID from policy version, run, key ID, and nonce ID, but runtime checks only pattern and uniqueness. Coherently replacing it everywhere with `qci_ffffffffffffffffffffffff` was accepted. Runtime/history validation must recompute the canonical derivation.

### P2-3 — The commit is not Ruff-format clean

Ruff format would change `src/x_first/grok_cli_exploration.py`, `tests/test_grok_cli_exploration.py`, and `tests/test_grok_cli_query_commitment_migration.py`; the base versions pass.

## Verified non-findings

- Duplicate issuance ID, policy version, run commitment, key ID, or nonce ID across canonical history rows is rejected.
- Reordered history and truncated history against a selected full snapshot are rejected.
- Historical snapshots replay when their lineage is an exact prefix of intact longer history.
- Listed protected examples are rejected.
- Basic lab/pretraining and China/Asia professional-context operands remain accepted.
- Public history/schema files expose only hashes and opaque IDs.
- Schema outer objects and rows are closed; lineage positions are strict integers.

## Promotion boundary

Only this query-policy slice is blocked. Adaptive, Stage 2, and unrelated offline work may continue. A new pinned review must cover the complete snapshot chain, descriptor binding, protected/neutral matrix, deterministic issuance ID, and format cleanliness.

`NO-GO`
