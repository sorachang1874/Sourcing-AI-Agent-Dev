# Google DeepMind v5 OAuth lifecycle failure — 2026-07-15

> Author diagnostic evidence. This is not an independent-review artifact, a performance sample, or a native-X
> retrieval result.

## Decision

The first reviewed Google DeepMind v5 attempt must be excluded from recall, precision, latency-per-lead, and
cost-per-lead comparisons. The Grok process exited before any model or native-X work. The retained bundle is valid and
honestly reports `process_failed`; the failure exposed an authorization-time OAuth freshness gap in the local runner.

## Exact run evidence

| Field | Observed value |
| --- | --- |
| run | `grok_wave_live_c4ec7140840942b2b1e62b5d078562f1` |
| request | `xwave_req_e932328e1f08463ca019afa39fdadf1d` |
| status | `process_failed` |
| process | exit `1`, `8,156 ms` |
| native-X/model work | `0` events, `0` tool calls, `0` tokens |
| emergency behavior | no timeout, fallback, TERM/KILL, or technical-limit transition |
| stderr class | Grok CLI `Not signed in` |
| bundle replay | `validate_operator_bundle(...) == []` |
| pinned auth SHA-256 | `84abeab853de5e235f356dccbeeb93f5f9c2d931bfd3451f0274b0e82e500bd4` |
| auth access expiry | `2026-07-15T11:21:36.930394Z` |
| process start | `2026-07-15T16:06:44.800Z` |
| expiry gap at start | `17,107.869606 s` |

The request, intent, receipt, and canonical OAuth file all bound the same auth digest. `HOME` and `GROK_HOME` were the
same isolated directory and the auth copy occupied its documented `auth.json` location. The failure was therefore not
a wrong path, missing copy, or SHA drift.

## Lifecycle diagnosis

The immediately preceding v4 process used the same expired auth bytes at `2026-07-15T14:48:30.234Z`, exited `0`, and
ran 35 native-X calls. Its isolated home was then deleted while the canonical auth file remained unchanged. The most
consistent explanation is that Grok refreshed and rotated credential state inside the isolated home; deletion removed
that refreshed state, and v5 later retried the now-stale canonical refresh state. The retained transport does not
preserve OAuth mutation payloads, so rotation causality is an evidence-backed inference rather than a replayable
provider fact.

The repair does not depend on that inference: it requires the existing access token to cover the complete grant wait
and maximum process window, so no isolated refresh is needed. Grant issuance and execution fail locally on malformed,
multi-row, expired, or near-horizon auth state; execution rechecks the copied bytes before consuming the grant. The
runner does not log in, refresh, or write OAuth state back to the canonical home.

## Next valid experiment

The consumed grant cannot be reused. After the repair passes targeted/full validation and a pinned non-author review,
a replacement experiment requires a fresh user OAuth login plus a new auth SHA, request ID, grant ID, request artifact,
and one-shot grant. Only a completed, transcript-verified, bundle-valid replacement run can be compared with v4.
