# Pinned non-author adversarial review — remediation `8087465`

## Evidence header

- Verdict: `NO-GO`
- Reviewer: `/root/next_lane_scout` — non-author, read-only.
- Base: `c9a26ee92c4c497e336c8851d44a89cb595d75dc`
- Reviewed commit: `8087465523bbfe0dbbc27d1f4442fa1f79ef441b`
- Sole parent: exact base above.
- Reviewed tree: `d3c3399f1486313bd58a1a0ba2a045c0f47c42aa`
- Binary diff SHA-256: `b6ade575b3f3458e76de80d9c5062caaa22d0c1cd4e856f23c813a7d2bd6c320`
- Diff: 49 files changed, 8,873 insertions, 1,610 deletions.
- Isolated detached worktree: `/private/tmp/x-first-8087465-review-20260714/x-first-researcher-sourcing`
- Worktree remained clean. Stage 2 and later commits were absent and not reviewed.
- No network, live provider, Grok, Luna, credentials, or main-worktree writes were used.

## Validation

- Focused remediation suites on Python 3.12.13: 143/143 passed.
- Same focused suites on Python 3.14.4: 143/143 passed.
- Full pinned suite on Python 3.12.13: 265/265 passed.
- First full Python 3.14.4 run: 264/265; the timeout escalation test expected `-SIGKILL` but observed `-SIGTERM`.
- That exact node then passed 5/5; a second complete Python 3.14.4 run passed 265/265.
- Ruff 0.15.11 passed.
- `compileall` passed.
- `git diff --check` was clean.
- Commit, parent, tree, diff hash, file count, and subject were independently recomputed.

Severity count: P0=0, P1=4, P2=2.

## Findings

### P1-1 — Protected-targeting is not closed across prompt, target, and actual tool subject

The versioned value set omitted ordinary gender, race, disability, age, and sexual-orientation values. Mutation probes accepted `women`, `female`, `Black`, `disabled`, and `gay` as native-X query subjects.

The adaptive lane is broader: arbitrary `target.scope` and bound prompt text can direct protected targeting while issuing neutral-looking searches. Direct query scanning alone cannot prove the effective selection objective. Validate user-controlled prompt/target inputs before compilation, bind production execution to a versioned approved effective-prompt/target owner, and validate every actual native-X operand through one shared owner.

### P1-2 — Pre-intent live failure creates an unrecoverable run root

The runner creates the run root, workspace, compiled prompt, and staged executable before loading the preissued grant. The canonical request and intent do not yet exist.

A missing or invalid grant left `compiled-prompt.txt`, `executable`, `run.lock`, and `workspace`. Recovery failed on missing private JSON and global TTL purge aborted on that inventory entry. Load and validate the grant before retained state, or create a minimal recoverable journal before the first retained artifact.

### P1-3 — Post-executor failures can retain copied OAuth state

Only executor exceptions were covered by immediate ephemeral-tree deletion. Spool reads, artifact publication, parsing, and receipt construction were outside the lifecycle-wide cleanup.

A fake-live executor with a conflicting canonical `raw.stdout` raised `FileExistsError` and left `ephemeral-home/auth.json` until explicit manual recovery. Disk-full and publication failures have the same shape. The complete post-copy lifecycle needs guaranteed auth deletion while preserving enough evidence for recovery.

### P1-4 — The claimed complete session-tree ceiling excludes directories and sockets

Directories were checked but never counted, every Unix socket was skipped, and traversal had no monotonic deadline. A direct probe with `max_files=16`, 40 empty directories, and 20 bound sockets returned `file_count=0` with no limit.

Count every entry, explicitly allow only the expected leader socket, cap depth and directory/entry count, and make traversal deadline-aware so inode, path, traversal, and cleanup work are bounded.

### P2-1 — Query-commitment issuance lineage is local to one snapshot

The runtime loaded only the selected registry and enforced uniqueness inside that file. It did not verify predecessor identity or append-only prefix continuity.

A synthetic v3 containing only a new Anthropic row reused v2's public `commitment_key_id`, used a fresh nonce, and evaluated successfully. Bind every snapshot to an exact predecessor/prefix, or use one canonical append-only all-history issuance owner.

### P2-2 — Cross-runtime timeout escalation test is timing-sensitive

The first full Python 3.14 run observed `-SIGTERM` while the test required `-SIGKILL`; five isolated reruns and the second full suite passed. The child may receive TERM before installing its handler under load even though KILL escalation was attempted.

Synchronize explicit child readiness before the short deadline, or assert the actual cleanup contract rather than a deterministic final signal code.

## Closed or materially improved areas

The reviewed commit materially closed the inspected portions of grant binding and expiry, run lease and process identity, transcript-backed completion, emergency-envelope replay, evidence subject binding, generic-web rejection in recall merge, fail-closed adaptive campaign bridge, tracked UUID removal, actual argv binding, calendar validation, confidence/caveat retention, bounded descriptor reads, source-stable semantic implementation digest, HMAC query commitments, business candidate/call cap removal, stable-ID conflict quarantine, and Luna full-artifact validation.

These improvements do not offset the four live-boundary blockers above.

## Promotion boundary

Offline fixtures, contract refinement, tests, and unrelated non-live development may continue. Commit `8087465523bbfe0dbbc27d1f4442fa1f79ef441b` must not authorize adaptive Grok execution, new provider-costing validation, query-policy promotion, campaign admission, product/outreach writes, or milestone signoff. Stage 2 was outside this review.

`NO-GO`
