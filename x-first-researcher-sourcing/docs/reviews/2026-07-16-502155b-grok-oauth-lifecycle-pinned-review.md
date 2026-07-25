# Grok OAuth lifecycle — pinned independent review

## Evidence header

| Field | Reviewed value |
| --- | --- |
| reviewer | non-author adversarial subagent `/root/oauth_d2_pinned_review` |
| review date | 2026-07-16 Asia/Singapore |
| target commit | `502155ba51bbc9a4454b5a26d7f469b7e3ad0c09` |
| base commit | `1cb829f81b4c060d0af0a1059af0d6a271264a1d` |
| target tree | `113e49302f49a044d1fcb4b9b5228a89b4c1b174` |
| target subject | `fix(x-first): bind Grok OAuth lifecycle` |
| review checkout | detached clean worktree `/private/tmp/x-first-oauth-d2-review-502155b`; the ambient implementation working tree was not used as review evidence |
| expected / observed binary diff SHA-256 | `cb4bfb8179a94b83185fe498accf48baaab0644d1612be536671ac8af3bf8a44` / exact match |
| provider, Grok, X, model, or connector calls | `0` |

The base is the target's sole parent. The reviewed diff is exactly these five modified files:

1. `x-first-researcher-sourcing/README.md`
2. `x-first-researcher-sourcing/docs/ADAPTIVE_GROK_WAVE_RUNNER_CONTRACT.md`
3. `x-first-researcher-sourcing/docs/live-evidence/2026-07-15-google-deepmind-v5-oauth-lifecycle-failure.md`
4. `x-first-researcher-sourcing/src/x_first/adaptive_grok_wave_runner.py`
5. `x-first-researcher-sourcing/tests/test_adaptive_grok_wave_runner.py`

The diff is `1,997 insertions / 72 deletions`. This artifact is the reviewer's only tracked-file write. No request,
prompt, schema, credential, approval, retained bundle, runtime implementation, or provider state was modified.

## Exact validation evidence

All implementation commands ran from the detached target checkout with the pinned source first on `PYTHONPATH`.

```text
git rev-parse HEAD
=> 502155ba51bbc9a4454b5a26d7f469b7e3ad0c09

git merge-base --is-ancestor 1cb829f81b4c060d0af0a1059af0d6a271264a1d \
  502155ba51bbc9a4454b5a26d7f469b7e3ad0c09
=> exit 0

git diff --binary 1cb829f81b4c060d0af0a1059af0d6a271264a1d \
  502155ba51bbc9a4454b5a26d7f469b7e3ad0c09 | shasum -a 256
=> cb4bfb8179a94b83185fe498accf48baaab0644d1612be536671ac8af3bf8a44  -

git diff --name-status 1cb829f81b4c060d0af0a1059af0d6a271264a1d \
  502155ba51bbc9a4454b5a26d7f469b7e3ad0c09
=> exactly the five modified paths listed above

git diff --check 1cb829f81b4c060d0af0a1059af0d6a271264a1d \
  502155ba51bbc9a4454b5a26d7f469b7e3ad0c09
=> exit 0, no output
```

Targeted OAuth/runner suite:

```text
PYTHONPATH=src .../.venv/bin/python -m unittest tests.test_adaptive_grok_wave_runner -v
=> Ran 90 tests in 22.265s; OK
```

Full standard-library suite:

```text
PYTHONPATH=src .../.venv/bin/python -m unittest discover -s tests -v
=> exit 0

PYTHONPATH=src .../.venv/bin/python -m unittest discover -s tests -q
=> exit 0

PYTHONPATH=src .../.venv/bin/python -c \
  'import unittest; print(unittest.defaultTestLoader.discover("tests").countTestCases())'
=> 379
```

The verbose and quiet executions independently passed; the loader count establishes the exact `379/379` total after
the verbose terminal stream was truncated by the command transport.

Contract and static gates:

```text
PYTHONPATH=src .../.venv/bin/python -m x_first.contracts
=> status=valid, errors=[]
=> precision=1.0, recall=1.0, predicted_count=20, relevant_count=20, false_merge_count=0

.../.venv/bin/ruff check src tests scripts
=> All checks passed!

PYTHONPATH=src .../.venv/bin/python -m py_compile \
  src/x_first/adaptive_grok_wave_runner.py tests/test_adaptive_grok_wave_runner.py
=> exit 0, no output
```

The target validator was then imported from this checkout and run read-only against the two named real retained
bundles, with the module-owned approval root. Only validator error arrays were printed:

```text
grok_wave_live_f41ad0d4206f4435baceb4bfe0b0e924 => []
grok_wave_live_c4ec7140840942b2b1e62b5d078562f1 => []
```

Thus both the successful-but-rejected Google DeepMind v4 diagnostic and the first v5 OAuth failure retain their
sealed interpretations under the new code. No credential value or retained private evidence was printed.

A filename-only default approval inventory emitted counts, not IDs or contents:

```text
grant_files=9
consumption_files=9
unconsumed_grants=0
auth_active_use_files=0
auth_taint_files=0
auth_lock_files=0
```

## Contract audit

### OIDC/JWT payload and freshness

The parser descriptor-reads one owner-owned, one-link, regular `0600` auth file under the 64,000-byte ceiling and
rejects duplicate JSON keys. It requires exactly one xAI OIDC row, exact issuer/client locator, required metadata,
identity agreement across metadata and claims, integer `iat`/`exp` and optional `nbf`, and the required scope subset.
Unknown future scopes remain forward-compatible. JWT length, payload-segment length, decoded bytes, depth, and node
count are bounded. The effective expiry is the earlier of metadata expiry and JWT `exp`.

This is deliberately payload decoding and consistency checking, not JWT signature verification. The code and all
three changed documents say that directly; no local cryptographic-authenticity claim is made. Provider revocation and
signature validity remain provider-owned checks.

Issuance requires a strictly later horizon than grant TTL plus process deadline, TERM/KILL grace, and 600 seconds.
Execution rechecks the exact canonical digest and full credential before prompt access or run-root creation, then
checks the descriptor-copied bytes with a fresh clock before consumption. Existing post-link wall/monotonic and gated
launcher checks remain in force. Exact-boundary, stale, future-`iat`/`nbf`, wrong-identity/scope, duplicate-claim,
multi-row, and metadata/JWT-minimum cases are covered by passing regressions.

### Active-use exclusion, taint, and cleanup ordering

The per-digest lock uses nonblocking `flock`, a 250 ms acquisition budget, and an inode check both before and after
acquisition. It is held only for short registry/publication transactions. Under that lock, grant consumption publishes
the exact auth-digest/run/request/run-lease/grant claim before the one-shot consumption record. A real two-thread
regression admits one executor and blocks the sibling; grant issuance and recovery also reject a sibling claim.

The normal terminal order is mechanically `claim ownership -> auth audit/taint -> durable ephemeral-tree deletion ->
exact claim resolution`. Audit/taint failure leaves the home and claim; deletion failure leaves the claim. Recovery
binds or synthesizes the exact claim before reading the consumption record, verifies any process ledger and ownership,
then follows the same order. A current `live_consumption` claim with a missing home can resolve because D2 places claim
resolution after deletion. A synthesized legacy claim with a missing home and provider ledger is instead tainted
before resolution because pre-D2 ordering cannot be proved. The dedicated regression for that legacy case passed.

An exact unchanged copy remains reusable after a clean provider exit or a known pre-release failure. Mutation,
deletion, unreadability, nonzero provider exit, post-release exception, and recovery after a provider-capable ledger
are tainted. Taint is replay-independent, contains only closed metadata, and does not reinterpret a retained bundle.

### Compatibility

Request, grant, consumption, intent, receipt, result, and command-policy wire shapes are unchanged. Active-use and
taint are operator recovery registries rather than retained-bundle dependencies. The two real replay results above
verify the intended compatibility boundary; no old failure was silently promoted.

## Findings

Severity totals: `P0=0 / P1=0 / P2=2`.

### [P2][re-raise][accepted residual] Hard death after intent publication but before auth copy causes a conservative availability false positive

The intent is durably published before `_copy_private_auth`. If the process is killed in that narrow interval,
recovery sees a live intent and an existing empty ephemeral home, synthesizes a `legacy_recovery` claim, and audits the
not-yet-created `auth.json` as `copied_auth_deleted`. It publishes a taint and blocks the otherwise unchanged canonical
digest. The changed contract explains that intent precedes copy, but its provider-deletion wording does not explicitly
disclose this pre-provider false-positive case.

The reviewer independently reproduced the exact boundary with a synthetic temp root, a fork, and `SIGKILL`; the
executor and provider gate were never reached:

```text
intent_published=true
auth_copy_present_before_recovery=false
consumption_present_before_recovery=false
recovery_taint_reason=copied_auth_deleted
next_grant_blocked=grok_auth_digest_tainted
```

This is nonblocking for the bounded live gate. It cannot release Grok, reuse stale credential state, forge evidence,
or weaken cleanup. Its consequence is operator availability only: a rare local hard death can require a fresh login,
new auth digest, request, and grant. A bounded follow-up should add a durable copy-phase discriminator (or equivalent
ordering proof) and state this conservative false-positive explicitly in the runner contract; it must not weaken the
current fail-closed behavior merely to preserve reuse.

### [P2][residual] Cross-process freshness still trusts the host wall clock

The implementation detects not-yet-valid JWTs and several in-process backward-clock transitions, but issuance and a
later process cannot share a trusted monotonic epoch. This is accurately disclosed in the runner contract and failure
note. It is nonblocking for the owner-triggered local experiment provided the host clock is synchronized; it is not a
general distributed-authorization proof.

## Accepted boundaries

- Local JWT parsing proves bounded shape, claim consistency, and time-window sufficiency only; xAI/Grok still owns
  signature verification, revocation, and actual login acceptance.
- A tainted old digest has no automatic clearing path. Recovery requires fresh OAuth bytes, a new digest-bound
  request, and a new one-shot grant.
- Active-use and taint coordinate future authority; they intentionally do not change historical receipt validity.
- This scope does not approve a discovery prompt, candidate quality, product promotion, hydration, campaign import,
  canonical identity, ranking, CRM, export, or outreach behavior.

## Gate consequence

No P0 or P1 defect was found. The OAuth-lifecycle slice may support a replacement bounded experiment only after a
fresh user login, new auth SHA, new request and grant, and every separate prompt/scope review required by that
experiment. The two accepted P2 residuals remain visible and do not authorize broader promotion or milestone claims.

GO
