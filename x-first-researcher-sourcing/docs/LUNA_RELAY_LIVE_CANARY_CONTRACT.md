# Luna Relay Live Canary Contract

Status: v2 receipt-first implementation is author-complete and offline-tested.
It has not been executed live. Independent review remains a hard prerequisite
for the v2 provider-costing live gate. The v1 artifact contract is frozen and
validation-only at the operator CLI.

## Version boundary

| Lane | Semantic assets | Approval/run identity | Operator status |
| --- | --- | --- | --- |
| legacy v1 | frozen semantic v2.1 source, prompt, output schema, request, and model-output fixture under `legacy/luna_canary_v1/` | `...one_shot_v1`; `luna_canary_run_*` | existing private bundles may be validated; no CLI live execution |
| receipt-first v2 | current SHA-pinned semantic v2.2 prompt, output schema, and professional-experience proxy policy | `...receipt_first_v2`; `luna_canary_v2_run_*` | offline fake-transport tests only until independent GO |

The v1 result schema was not edited to accept semantic v2.2. Instead,
[`src/x_first/luna_live_canary.py`](../src/x_first/luna_live_canary.py) imports
[`profile_bio_semantic_legacy_v21.py`](../src/x_first/profile_bio_semantic_legacy_v21.py)
and reads only the immutable assets in
[`legacy/luna_canary_v1/`](../legacy/luna_canary_v1/). A later semantic release
therefore cannot silently change replay of a v1 artifact.
The v1 freeze manifest also hashes the legacy runner, v1 result schema, and
shared operator CLI, so a validator or entrypoint change is visible in the same
regression gate rather than leaving only the semantic assets pinned.

The active v2 implementation is
[`src/x_first/luna_live_canary_v2.py`](../src/x_first/luna_live_canary_v2.py).
It binds all of these current identities in `result.json`:

- request: `x.profile.bio_semantic.request.v2.2`;
- review: `x.profile.bio_semantic.review.v2.2`;
- prompt: `profile-bio-semantic-prompt-v2.2` plus canonical SHA-256;
- strict output schema: `x.profile.bio_semantic.model_output.v2.2` plus
  canonical SHA-256; and
- professional-experience proxy policy v1 plus canonical SHA-256.

No v1 approval, result, or run ID is accepted by v2, and vice versa.

## Purpose and authority

The v2 lane answers one narrow operational question: does the fixed chshapi
OpenAI-compatible relay catalog expose the exact `gpt-5.6-luna` model, and can
that exact model complete the synthetic `.invalid` profile-Bio semantic-v2.2
request through `/responses`?

It does not fetch X, discover or rank people, decide eligibility, confirm
employment, write canonical data, authorize outreach, or infer a protected
identity. Semantic output remains an unverified professional-context proposal.
All authority fields in the request, semantic review, and outer result remain
false.

## Closed network and budget boundary

There is no configurable provider, model, route, retry, or fallback:

- base URL: `https://api.chshapi.org/v1`;
- handshake: exactly one attempted `GET /models` after approval consumption;
- inference: at most one attempted `POST /responses`, only after one exact
  catalog match;
- requested and returned model: exact `gpt-5.6-luna`;
- redirects, retries, fallback, tools, and response storage: disabled;
- total deadline: 30 seconds;
- response limits: 256 KiB catalog and 1 MiB response; and
- semantic limits: the current v2.2 request budgets, including at most one
  model call.

The production HTTP client rejects redirects. Strict UTF-8 JSON parsing rejects
duplicate keys and non-finite values. Catalog bodies are not retained; only
bounded status/counts and a canonical digest of sorted model IDs survive.
Both successful catalog and model responses require the exact media-type
grammar `application/json` with, at most, `charset=utf-8`; substring matches
such as `application/jsonp`, `text/application/json`, and non-UTF-8 charsets
fail closed.

Production provenance is not an argument accepted from an arbitrary caller.
The internal runner receives one of two module-owned lane capabilities. The
production capability additionally requires the exact stdlib HTTP client,
production runtime/approval owners, UTC wall clock, and monotonic clock;
injected dependencies cannot be labelled `production_http`. The fixture lane
rejects the production HTTP client and passes the supplied fake client
directly, including when that client is falsey, so `client or real_client`
cannot create an accidental network fallback.

## Observed execution receipt

V2 removes the v1 semantic accounting ambiguity. The semantic layer never
receives an object whose `is_live` attribute is treated as proof of execution.
`ObservedHttpTransport.execute` returns an `ObservedHttpAttempt` consisting of
a sanitized receipt and, only when received, an in-memory bounded HTTP
response. Each receipt records:

- sequence and operation (`model_catalog` or `semantic_response`);
- exact method, endpoint, and requested model;
- SHA-256 of the exact outbound payload bytes (empty bytes for catalog);
- canonical start/completion timestamps, monotonic elapsed milliseconds, and
  the actual bounded timeout;
- either HTTP status plus response-body SHA-256 or the closed
  `transport_failure` outcome; and
- `retry_used=false` and `fallback_used=false`.

The persisted `execution-receipt.json` binds the ordered attempts, total
attempt count, overall deadline, model, payload digest, run ID, provider, and
zero retry/fallback policy. The model response raw receipt must bind the same
HTTP status and body digest.

Only after this observed POST receipt validates does the v2 adapter construct
the semantic execution facts. It then deterministically adjudicates the
retained response against current semantic v2.2 assets. Generation and offline
validation use the same receipt-bound pure adjudication function; neither calls
`run_semantic_review` with a live transport. A response transport failure is
also materialized as one observed provider attempt and one reproducible failed
semantic review, with no fabricated HTTP status or body digest.

The semantic validator grants no live or billing authority. It rejects every
caller-supplied `execution_attempt` as evidence—including the review's own
execution object and a deep copy—and refuses a completed live review with a
direction to this outer validator. `validate_artifact_directory_v2` is the
production authority boundary: it binds the canonical approval ledger, ordered
observed HTTP receipt, raw-response body digest, artifact hashes, result call
arithmetic, and deterministic semantic replay. The semantic implementation
digest carried by these artifacts is only a trusted-runtime drift signal; it is
not described as tamper-proof or as an independent integrity root.

## Credential, approval, and privacy contract

The key is read only from `CHSHAPI_API_KEY` after the explicit
`--execute-live-v2` gate. There is no key CLI argument or key-file path. The key
and provider exception details are never printed or written. Secret detection
covers literal key-shaped bytes and key-shaped strings recovered by strict JSON
decoding, including JSON-escaped punctuation. A secret-containing provider
body becomes a digest-only private receipt and a terminal failure.

The distinct v2 approval is:

```text
user_approved_2026_07_14_chshapi_luna_receipt_first_v2
```

Immediately before the catalog attempt, it is consumed with `O_EXCL` under:

```text
~/.local/state/x-first-researcher-sourcing/luna-live-approvals-v2/
```

The owner ID is
`user_state:x-first-researcher-sourcing/luna-live-approvals/v2`. The directory
must be owner-only `0700`, and the ledger receipt must be `0600`. An existing
receipt blocks replay before any external attempt. Concurrent attempts can
therefore have only one winner. No flag or a missing/malformed key makes zero
external calls and zero filesystem writes.

The exact approval fields and production/fixture identity combinations are
declared by
[`x.profile.bio_semantic.live_canary.approval_receipt.v2.schema.json`](../contracts/x.profile.bio_semantic.live_canary.approval_receipt.v2.schema.json)
and mirrored by the strict runtime validator. The receipt pins the semantic
request/review versions, pure-adjudication API and trusted-runtime drift digest,
request digest, run ID, owner, probe, consumption time, and consumed state.

## Artifact and replay contract

V2 bundles are atomically materialized under the git-ignored owner:

```text
runtime/luna-live-canaries-v2/luna_canary_v2_run_<uuid>/
```

Directories are `0700`; files are `0600`. Every bundle contains:

- `request.json` — exact synthetic semantic-v2.2 live-canary request;
- `approval-receipt.json` — consumed v2 approval;
- `catalog-receipt.json` — sanitized catalog result;
- `execution-receipt.json` — exact ordered observed-attempt receipt; and
- `result.json` — terminal outer result and hashes.

When the POST is attempted, `semantic-review.json` is also present. When an
HTTP response is received, `raw-response.json` additionally retains the bounded
body in base64 or a digest-only redacted record. A transport failure has a
semantic review but no raw-response file.

`result.json` declares the exact inventory and canonical SHA-256 of every other
artifact, plus dedicated approval and execution receipt hashes. Private raw
evidence has a 24-hour delete-after timestamp and a pending-deletion state; the
contract does not falsely claim deletion already happened.

## TTL purge, recovery, and scheduling

Expiry is enforced mechanically. Once `delete_after` is reached, ordinary
artifact validation fails closed until the operator runs:

```bash
PYTHONPATH=src python3 scripts/run_luna_live_canary.py \
  --purge-expired-v2-directory runtime/luna-live-canaries-v2/luna_canary_v2_run_<uuid>
```

Purge is idempotent and crash-recoverable. Before moving evidence, it writes a
private `*.deletion.pending.json` journal under the owner-only deletion root.
It then atomically renames the bundle to the deterministic
`.luna_canary_v2_run_<uuid>.deleting` quarantine, removes that quarantine, and
only then creates the durable `*.deletion.json` tombstone. A retry with the
original bundle path resumes after any of those durable transitions, including
the window after removal but before the final tombstone. Once the final
tombstone exists, a retry returns the same receipt and removes any already-bound
orphan journal.

The public deletion-receipt validator is production-only. It accepts only a
final `state=deleted` tombstone at the canonical owner-only deletion root, binds
its run and approval digest to the canonical approval ledger, requires all
digests to be nonzero SHA-256 values, and proves that both the original bundle
and deterministic `.deleting` quarantine are absent. A matching pending journal
is accepted only internally during crash recovery and is removed before final
validation succeeds. Offline tests use the separate
`validate_deletion_receipt_v2_fixture(...)` API with explicit fixture runtime,
approval and deletion owners; a fixture tombstone cannot pass the production
validator. The in-progress journal has a separate declarative schema and is
never proof of deletion. The final and journal schemas enumerate the exact
allowed artifact inventories and production/fixture run-ID pairs.

An operator scheduler should enumerate only direct children of the fixed
`runtime/luna-live-canaries-v2/` owner that match
`luna_canary_v2_run_[0-9a-f]{32}`, invoke the bounded purge command once per
expired directory, and retry failures with the identical path. Hourly cadence
is sufficient for the 24-hour TTL. It must not delete approval ledgers,
deletion journals, tombstones, unknown paths, or bundles that have not reached
their recorded deadline. A nonzero command exit is an operator incident and
must not be replaced by raw `rm -rf`.

The offline validator performs no network or credential read. It verifies
owner-only modes, exact inventory and hashes, the global approval ledger,
current semantic asset bindings, request-payload digest, ordered route/model
attempt grammar, response/body binding, call arithmetic, retention, zero
authority, and deterministic semantic replay.

The single validation command auto-detects v1 or v2:

```bash
PYTHONPATH=src python3 scripts/run_luna_live_canary.py \
  --validate-directory runtime/luna-live-canaries-v2/luna_canary_v2_run_<uuid>
```

## Live promotion gate

No live v2 call is authorized by green author tests alone. Before the operator
may run the following command, the contract-heavy v2 diff needs an independent
GO artifact covering the v1 freeze, approval separation, receipt grammar,
semantic replay, secret handling, and zero-authority boundary:

```bash
CHSHAPI_API_KEY='<operator-owned value>' \
  PYTHONPATH=src python3 scripts/run_luna_live_canary.py --execute-live-v2
```

Do not place a real key in shell history, documentation, tickets, artifacts, or
fixtures. Approval consumption is terminal even when the provider is
unavailable. Host crash, filesystem loss, or disk failure after the durable
approval write can leave an orphaned consumed approval; that is a fail-closed
operator incident, not permission to delete the ledger and retry.

## Offline regression coverage

[`tests/test_luna_live_canary.py`](../tests/test_luna_live_canary.py) preserves
the v1 regression suite and asserts the validator is frozen to v2.1 assets.
[`tests/test_luna_live_canary_v2.py`](../tests/test_luna_live_canary_v2.py)
covers current v2.2 asset binding, zero-call gates, successful receipt replay,
catalog early stop, response transport failure, payload/body digest binding,
coherently rehashed route/model tampering, owner-only artifacts, and one-winner
approval concurrency. The no-dependency project mini Draft-2020-12 validator
executes the declared result, execution, approval, deletion-journal and
deletion-tombstone schemas against generated success, failure and purge payloads,
in addition to the strict runtime validators. All HTTP activity is supplied by an injected fake client;
the tests read no operator key and perform no network access.
