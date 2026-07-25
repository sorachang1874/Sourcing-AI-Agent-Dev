# Grok OAuth freshness repair — pinned independent review

## Evidence header

| Field | Reviewed value |
| --- | --- |
| reviewer | non-author adversarial subagent; implementation checkout was never used |
| review date | 2026-07-16 Asia/Singapore |
| target commit | `d0878ce64c8485b6c7479571681fd342478a85f7` |
| base commit | `e6e07efe2a7e64d981bbcb4b540865fae1cf4e61` |
| target tree | `ceaafde04312b3881931f0fdf0b64e8eda74cc2a` |
| target subject | `fix(x-first): gate Grok OAuth freshness` |
| review checkout | detached clean worktree created from Git objects under `/private/tmp`; `git status --porcelain` was empty before and after validation |
| expected / observed binary diff SHA-256 | `e63625ae4f05725e518da607e18d5becdd883cf4277c5fbae7064a837ed4b1f4` / exact match |
| provider or live calls | `0` |

The reviewed diff is exactly these five files and no others:

1. `x-first-researcher-sourcing/README.md`
2. `x-first-researcher-sourcing/docs/ADAPTIVE_GROK_WAVE_RUNNER_CONTRACT.md`
3. `x-first-researcher-sourcing/docs/live-evidence/2026-07-15-google-deepmind-v5-oauth-lifecycle-failure.md`
4. `x-first-researcher-sourcing/src/x_first/adaptive_grok_wave_runner.py`
5. `x-first-researcher-sourcing/tests/test_adaptive_grok_wave_runner.py`

`e6e07ef` is an ancestor of the target. No request, schema, effective-prompt row, Grok binary, credential, approval,
retained artifact, or implementation file was changed during this review.

## Exact validation evidence

From the detached target checkout:

```text
git diff --binary e6e07ef d0878ce64c8485b6c7479571681fd342478a85f7 | shasum -a 256
=> e63625ae4f05725e518da607e18d5becdd883cf4277c5fbae7064a837ed4b1f4  -

git diff --name-status e6e07ef d0878ce64c8485b6c7479571681fd342478a85f7
=> 4 modified + 1 added; exactly the five paths listed above

git diff --check e6e07ef d0878ce64c8485b6c7479571681fd342478a85f7
=> exit 0, no output

PYTHONPATH=src /Users/changyuyi/projects/Sourcing\ AI\ Agent\ Dev/sourcing-ai-agent/.venv/bin/python \
  -m unittest tests.test_adaptive_grok_wave_runner -v
=> Ran 75 tests in 14.065s; OK

PYTHONPATH=src /Users/changyuyi/projects/Sourcing\ AI\ Agent\ Dev/sourcing-ai-agent/.venv/bin/python \
  -m unittest discover -s tests -v
=> Ran 364 tests in 34.883s; OK

/Users/changyuyi/projects/Sourcing\ AI\ Agent\ Dev/sourcing-ai-agent/.venv/bin/ruff check src tests scripts
=> All checks passed!
```

The target validator was also run read-only against the two real retained bundles named by this slice, using the
module-owned approval root:

```text
grok_wave_live_f41ad0d4206f4435baceb4bfe0b0e924  => validate_operator_bundle(...) == []
grok_wave_live_c4ec7140840942b2b1e62b5d078562f1  => validate_operator_bundle(...) == []
```

That proves both the rejected v4 diagnostic and the first v5 OAuth failure remain replayable under the target code.
The v5 receipt independently reports `process_failed`, exit `1`, elapsed `8,156 ms`, no timeout, no TERM/KILL, and a
missing session proof; it does not claim model or native-X work.

A receipt-safe approval inventory found `9` grant files, `9` matching consumption files, and `0` unconsumed grants
(`0` unexpired and `0` expired). No grant identifier or credential content was emitted. Thus there is no ambient
pre-repair live authority that can bypass issuance-time freshness in the next experiment.

A synthetic owner-only auth matrix additionally exercised duplicate top-level keys, duplicate `expires_at`, an offset
instead of `Z`, an impossible calendar date, and seven fractional digits. Every case failed as
`grok_auth_session_invalid`. The committed tests separately prove expired, sub-boundary, exact-boundary, malformed,
and multiple-row rejection, plus one-microsecond-past-boundary acceptance. No real credential value was printed or
copied by the review.

## Contract audit

### JSON, row ownership, and time boundary

The new parser uses the existing duplicate-key-rejecting JSON loader and an owner/one-link/regular/`0600`/64,000-byte
descriptor read. It accepts exactly one top-level credential row, requires a nonempty string `key`, and accepts only a
real UTC `Z` expiry with zero through six fractional digits. Offset forms, invalid dates, duplicate keys, empty or
multi-row objects, and non-string fields fail locally. The comparison is deliberately strict: an access expiry equal
to the required horizon is rejected; only a later instant passes.

This is a local freshness precondition, not a claim that the provider has not revoked the token. That distinction is
preserved below as a residual rather than being upgraded into credential-validity evidence.

### Horizon arithmetic

The runtime window is exactly:

```text
deadline_ms + term_grace_ms + kill_grace_ms + 600 seconds
```

Grant issuance requires access expiry strictly after `issue_clock + grant_TTL + runtime_window`. Because the grant
itself expires no later than `issue_clock + grant_TTL`, every newly issued grant retains a complete process/grace
window plus the refresh-avoidance margin even if target release occurs at the end of the grant window. The default
request therefore covers its 1,800,000 ms deadline, both 1,000 ms cleanup graces, and the fixed 600-second margin; no
candidate, query, observation, or native-X business cap was introduced.

### Ordering and clock changes

The new-run ordering is fail-closed:

1. issuance checks the request-pinned auth SHA, parses freshness, and only then publishes the one-shot grant;
2. execution validates the effective-prompt row and grant, then checks the exact request-bound auth before prompt
   loading or run-root creation;
3. after intent publication, binary staging, and descriptor-copying auth into the disposable home, it rechecks copied
   SHA and freshness using a new wall-clock sample;
4. only then can atomic grant consumption publish;
5. the existing post-link wall/monotonic checks and gated-launcher checks still prevent target release after grant
   expiry or after a backward-clock inconsistency.

If canonical auth bytes do not match the request SHA, the early freshness parser is intentionally not trusted against
those unbound bytes. The later exact-SHA preflight still deletes the ephemeral home and fails before consumption and
before any provider execution. No mismatch can convert a stale credential into an authorized run.

For newly issued grants, forward movement anywhere inside the grant interval remains covered by the issuance-time
`TTL + runtime_window` proof. Backward movement before consumption is rejected by the existing issued/consumed/link
ordering. Host UTC correctness and provider-side revocation cannot be derived from a local file and remain explicit
residuals.

### Compatibility and documentation

The patch does not change receipt, grant, consumption, request, result, command-policy, or replay schema versions.
That is consistent with the successful retained v4/v5 bundle replays and avoids reinterpreting their terminal status.
The consumed v5 grant remains consumed. The approval inventory proves every other retained grant is also consumed;
even without that inventory, an unconsumed grant has a hard maximum lifetime of 3,600 seconds. The next experiment
must therefore use a grant issued by the reviewed code.

The README and runner contract accurately label the rotation diagnosis as an inference, not replayable provider fact.
They accurately exclude the failed v5 attempt from performance comparisons, describe the three freshness checkpoints,
and require a new login/auth SHA, request, grant ID, and one-shot grant. They do not claim that this offline repair is a
native-X result or product milestone.

## Findings

- P0: none.
- P1: none.
- P2: none.

## Accepted residuals

1. `expires_at` proves only the local file's declared access-token horizon. Server revocation, provider clock skew, or
   a provider changing its proactive-refresh threshold cannot be proven without the replacement live run.
2. OAuth expiry and the freshness-policy version are not serialized into the grant or terminal receipt. Replay proves
   the unchanged grant/consumption/bundle contracts, while freshness enforcement depends on issuing the replacement
   grant with this pinned runner. This is nonblocking for the next run because the approval inventory contains no
   unconsumed grant, but it should remain visible if independent post-hoc proof of OAuth preflight is later required.
3. The 64,000-byte auth read is byte-bounded but does not impose a separate JSON depth/node budget. A synthetic deeply
   nested irrelevant field can raise `RecursionError`; the public CLI catches and redacts it before grant publication,
   prompt loading, run-root creation, or provider execution. This is an owner-local robustness improvement, not an
   authorization bypass.
4. No provider call was made in this review. The repair authorizes only one replacement experiment after a fresh OAuth
   login and freshly bound request/grant; it does not prove Grok login success, X-search quality, recall, or precision.

## Gate consequence

The reviewed scope may proceed to one bounded replacement Grok/X experiment only after fresh OAuth state, a new auth
SHA/request/grant identity, and a one-shot grant issued by this pinned implementation. A completed run must still pass
transcript proof and bundle replay before entering performance comparison. Product promotion, source-bound hydration,
canonical writes, outreach, and milestone claims remain outside this verdict.

GO
