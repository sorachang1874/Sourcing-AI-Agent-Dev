# Luna Relay Live Canary Contract

Status: implemented and offline-validated; independent review remains a live
prerequisite. No live call is evidence until the owner explicitly runs the
one-shot CLI and the resulting bundle passes the offline validator.

## Purpose and authority

This lane answers one narrow operational question: does the fixed chshapi
OpenAI-compatible relay catalog expose the exact `gpt-5.6-luna` model, and can
that exact model complete the existing profile-Bio semantic-v2 synthetic
fixture through `/responses`?

It does not fetch an X profile, discover or rank people, confirm employment,
write canonical data, authorize outreach, or make eligibility decisions. Its
input URL is under `.invalid`; all semantic output remains an unverified
professional-context proposal governed by the public semantic-v2 contract.

## Closed execution boundary

The executable boundary is
[`src/x_first/luna_live_canary.py`](../src/x_first/luna_live_canary.py). It has
no configurable provider or model route:

- base URL: `https://api.chshapi.org/v1`
- handshake: one `GET /models`
- inference: at most one `POST /responses`, only after the catalog contains
  exactly one `gpt-5.6-luna` entry
- requested and returned model: exact `gpt-5.6-luna`
- fallback, retries, redirects, tools, and response storage: disabled
- total deadline: 30 seconds
- response limits: 256 KiB catalog and 1 MiB response
- JSON limits: depth 64, nodes 16,384, leaves 12,288
- semantic limits: the current versioned semantic-v2 request budgets, including
  at most one model call

The production transport rejects redirects. Both real and injected transports
are checked again at the runner boundary, so a 3xx response fails closed.
Catalog bodies are strict UTF-8 JSON with duplicate and non-finite values
rejected. Only model counts, exact-match count, and a canonical digest of the
sorted model IDs survive in the catalog receipt; the catalog body is not
retained.

## Credential and approval contract

The key is read only from `CHSHAPI_API_KEY` after the explicit `--execute-live`
gate. It must be a bounded `sk-` token containing only ASCII letters, digits,
underscore, or hyphen. There is no CLI key argument and no key-file path. The
key and provider exception details are never printed or written. A provider
body or retained content-type value containing either the exact key or another
key-shaped token is retained only as a private redacted receipt containing the
body digest; the content type and body are removed, and the canary terminates
failed.

The sole approval is
`user_approved_2026_07_14_chshapi_luna_one_shot_v1`. Immediately before the
catalog call, the runner creates this global ledger entry with `O_EXCL`:

```text
~/.local/state/x-first-researcher-sourcing/luna-live-approvals/
  user_approved_2026_07_14_chshapi_luna_one_shot_v1.json
```

The ledger owner is
`user_state:x-first-researcher-sourcing/luna-live-approvals/v1`. Its directory
must be mode `0700` and its receipt mode `0600`. Existing ledger state blocks
all replay before either external call. Concurrent attempts therefore have one
possible winner, not two independent per-process approvals.

No execution flag or a missing/malformed key produces zero external calls and
zero filesystem writes.

## Call and terminal-state accounting

The outer result separately records:

- `catalog_external_calls` (exactly 1 after approval),
- `model_external_calls` (0 or 1),
- `total_external_calls` (at most 2), and
- the semantic review's `semantic_model_calls` (0 or 1).

It also records the requested/returned model identity, exact-match boolean,
provider-reported token usage when available, monotonic elapsed milliseconds,
and the zero-authority envelope. A POST attempt counts as a model external call
even when transport or response validation fails. There is no retry that could
hide or inflate that count.

The runtime owner directory is created and checked as private before approval
is consumed. Every provider, protocol, or semantic failure that reaches the
materialization boundary becomes a terminal bundle. Catalog failures stop
before POST. A response transport failure has no response evidence. Once an
HTTP POST response exists, the bundle contains both its private raw-response
envelope and a completed or failed semantic review. Invalid, rerouted,
oversized, malformed, or secret-echoing responses cannot become successful
semantic evidence.

No local process can guarantee a terminal file after a host crash, filesystem
loss, or disk failure that occurs after the durable approval write. Such an
orphaned approval remains consumed and is a fail-closed operator incident; it
does not authorize deleting the ledger or retrying.

## Private artifact bundle

Bundles are written atomically under the git-ignored runtime owner:

```text
runtime/luna-live-canaries/luna_canary_run_<uuid>/
```

The runtime and bundle directories are `0700`; every file is `0600`. All bundles
contain:

- `request.json` — the exact synthetic semantic-v2 live-canary request;
- `approval-receipt.json` — a copy of the durable consumed approval;
- `catalog-receipt.json` — sanitized catalog status/count/digest; and
- `result.json` — the outer terminal result.

When an HTTP POST response exists they additionally contain:

- `raw-response.json` — private status, content type, body digest, and exact
  base64 body, or a no-body redacted receipt if a key-shaped token was found;
- `semantic-review.json` — the semantic-v2 completed or failed review.

`result.json` declares the exact filename inventory and canonical SHA-256 of
every other artifact, and separately binds the approval receipt. Raw evidence
has a 24-hour delete-after timestamp and pending deletion state. This contract
does not claim deletion has happened; an operator must remove expired private
bundles under the recorded deadline.

## Validation and operator entrypoints

The validator uses no credential and performs no network request. It checks
private ownership/modes, exact inventory, hashes, the global approval ledger,
the fixed synthetic request, catalog and redaction state combinations,
timestamps/retention, type-strict call arithmetic, identity, and zero authority.
It deterministically recomputes completed semantic reviews
with the public `validate_review` entrypoint. Failed semantic artifacts are
replayed through the public `run_semantic_review` boundary and must equal the
stored terminal result.

```bash
PYTHONPATH=src python3 scripts/run_luna_live_canary.py \
  --validate-directory runtime/luna-live-canaries/luna_canary_run_<uuid>
```

The only live operator entrypoint is:

```bash
CHSHAPI_API_KEY='<operator-owned value>' \
  PYTHONPATH=src python3 scripts/run_luna_live_canary.py --execute-live
```

Do not put the real key in shell history, documentation, tickets, artifacts, or
test fixtures. The command consumes the dated approval even if the provider is
unavailable or the catalog does not contain Luna. A failed result is evidence
of that bounded attempt, not permission to delete the ledger and retry.

## Offline regression contract

[`tests/test_luna_live_canary.py`](../tests/test_luna_live_canary.py) uses only
an injected fake HTTP client and a fake key. It covers zero-call gates, catalog
absence and replay blocking, a successful two-call bundle, reroute and malformed
responses, catalog/response transport failures, secret non-retention, artifact
tampering/extra files/permissions/hash failures, and a concurrent one-winner
approval race. Running those tests does not read operator key files, inspect the
real environment key, or access the network.
