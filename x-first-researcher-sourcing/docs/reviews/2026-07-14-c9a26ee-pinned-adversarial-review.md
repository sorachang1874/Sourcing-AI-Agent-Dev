# Pinned adversarial review — `c9a26ee92c4c497e336c8851d44a89cb595d75dc`

Verdict: **NO-GO**

This is the durable record of two non-author, read-only reviews against the exact commit above. It is not a review of
the subsequent working-tree fixes, not a provider/live authorization, and not a milestone sign-off. The repository
does not yet have a project-owned independent-review runner, so this record cannot substitute for a later
hash-verified promotion artifact.

## Scope A — adaptive Grok runner

Reviewer: isolated non-author agent `/root/review_hardened_runner`.

Validation on the pinned object: focused adaptive suites `21 + 20` passed; full X-first suite `223` passed; Ruff and
compile checks passed. Mutation probes reproduced the findings below.

Severity count: `P0=0`, `P1=11`, `P2=6`.

### P1

1. Live grant omitted the auth/account fingerprint, executable/CLI policy, result-schema digest, token/cost policy and
   retention scope.
2. A grant could be consumed after real expiry because expiry was compared with a stale timestamp.
3. Auth/evidence retention was not a closed lifecycle: the pre-auth intent, SIGKILL orphan window, TTL, purge and
   deletion proof were incomplete.
4. Recovery could race the original operator because there was no active-owner lock/lease.
5. Adaptive completion trusted model JSON rather than mechanically proving the terminal output from the transcript.
6. The writable Grok/XDG tree had no file-count, per-file or aggregate disk ceilings.
7. Bundle replay did not bind the emergency/process envelope captured by the receipt.
8. Recall merge accepted a wave reporting `generic_web_used=true`.
9. Evidence was not bound to its candidate subject, so a `self` item could cross candidates.
10. There was no executable adaptive-result-to-campaign adapter.
11. A tracked live-evidence table published seven raw session and request UUID pairs.

### P2

1. `cli_flags_sha256` was not a digest of the actual canonical argv.
2. Recovery validated only the receipt rather than the complete bundle.
3. Impossible dates such as `2026-02-31` were accepted.
4. Candidate confidence and caveats were validated and then dropped.
5. Default 1 GiB / 1M candidate / 10M evidence ceilings could create avoidable memory pressure.
6. Prompt/prior-file reads had a TOCTOU window and no bounded-growth contract.

## Scope B — semantic/query/evaluation contracts

Reviewer: isolated non-author agent `/root/recall_pool_merge`.

Validation on the pinned object: scoped suites `88/88` and full X-first suite `223/223` passed on Python 3.12. Cross-
runtime mutation probes reproduced the failures below on system Python 3.14.

Severity count: `P0=0`, `P1=4`, `P2=2`.

### P1

1. Protected-identity values could still affect exclusions/caveats because only object keys were scanned.
2. The semantic digest hashed CPython bytecode, worked on 3.12 only, and contradicted the declared Python `>=3.11`
   compatibility.
3. Public unsalted query-call hashes were dictionary recoverable; one query was recovered from a 20,520-input probe.
4. The evaluator retained fixed business caps of 25 candidates and 32 calls.

### P2

1. The same numeric platform ID across different handles was counted twice instead of being quarantined from unique
   and precision denominators.
2. The Luna result-only validator accepted internally inconsistent mutated status/error fields even though the full
   artifact validator rejected them.

## Gate consequence

Both scopes remain `NO-GO` for provider-costing validation, product promotion and milestone sign-off at this pinned
commit. Fixes may proceed asynchronously. A new non-author review must pin the eventual fix commit and must not reuse
this verdict as evidence that the fixes were reviewed.
