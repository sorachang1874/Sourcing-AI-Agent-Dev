# Stage 1 bounded live Grok/X capability contract

> Status: Author hardening in progress; independent review and first execution remain pending. The user approved a
> bounded live Grok/X test on 2026-07-14. This does not approve researcher mapping, Stage 2, identity merging,
> product writes, outreach, or scale-up. Grok CLI 0.2.99 cannot enforce a hosted-tool call count before transport;
> the runner therefore detects and kills a second call but must not describe that monitor as a provider-side cap.

## Decision and scope

The first live action is still an account-level capability handshake against the official `@OpenAI` X account. It is
designed to answer two separate questions:

1. did the Grok CLI execute its hosted native `x_search` tool and return canonical public X post evidence; and
2. did that evidence bind every observation to one stable numeric author `platform_user_id`?

The result distinguishes `post_retrieval_only` from `x_native_identity_ready`. A handle and canonical post URL can
prove bounded post retrieval, but they cannot authorize researcher identity work. `stage2_eligible_for_owner_review`
is true only for `x_native_identity_ready`, and it is still an owner-review input rather than an automatic Stage 2
transition.

## Owner and source-of-truth matrix

| Concern | Owner/source of truth | V2 rule | Forbidden fallback |
| --- | --- | --- | --- |
| User trigger | closed request + durable consumption ledger | Exact 2026-07-14 approval is atomically consumed once before spawn | Inferring approval from OAuth/login state or replaying the flag |
| Legal/privacy basis | request `owner_decisions.legal_privacy` | Public-professional, minimized official-account evidence | Private content, protected identity, full-body retention |
| Access mode | runner command + structured Grok session updates | Pinned Grok CLI digest, `grok-4.5`, OAuth, successful artifact requires exactly one completed `x_search` call | `web_search`, `web_fetch`, model memory, Apify, another provider |
| Request identity | canonical sorted-JSON SHA-256 | Result binds exact closed request | Mutable prompt-only approval |
| Tool proof | ephemeral `updates.jsonl` parser + minimized tool receipt | Structured call/update, exact tool id, model id, raw result post/author ids and completion | Searching prompt/response prose for the string `x_search` |
| External account | X platform | Numeric `platform_user_id` plus handle history | Handle/name as canonical person identity |
| Post identity | X platform | Numeric post id and exact `https://x.com/OpenAI/status/{id}` | Snippet URL, search-result redirect, reconstructed id |
| Usage | Grok envelope + monotonic clock + live process monitor | One accepted execution/call/result set, max 5 observations/4 turns/180s; a detected overrun is retained only as failed evidence | Missing cost represented as `$0` or a detected second call treated as success |
| Retention | private ignored atomic artifact bundle | 24h, excerpts <=280 chars, approval/tool receipts, explicit expiry and purge receipt; raw transcript deleted on exit | Checked-in live artifact or unbounded full-body transcript |
| Product state | existing product owners | All candidate/link/assertion/write arrays empty | PersonAsset, CRM, projection, export, outreach |

The executable owners are `src/x_first/live_probe.py`, the v2 request/result JSON schemas, and
`scripts/run_live_capability_probe.py`. The declarative schemas cannot weaken executable validation.

## Execution topology

```text
closed v2 request -- atomic one-shot approval consumption
      |
      v
verified CLI binary copy + fd-bound OAuth copy + isolated env/cwd
      |
      v
temporary GROK_HOME -- grok-4.5 -- generic web/local tools disabled
      |                                      |
      |                                      +-- one hosted X Search call maximum
      v
bounded stdio/process-group monitor + structured session-tool proof
      |
      +-- invalid/ambiguous --------> capability_unavailable, observations=[]
      |
      +-- canonical posts, no user id -> post_retrieval_only
      |
      +-- canonical posts + one stable user id -> x_native_identity_ready
      v
atomic private bundle: request/result/approval/tool receipts; no raw transcript or product writer
```

The runner uses an explicit `--execute-live` gate and atomically consumes the single approval in a mode-`0600`
ledger before spawning the provider process. Replays and concurrent contenders fail before another external process
starts. It copies a current-user-owned, non-writable reviewed Grok binary into the temporary home and verifies SHA-256
`01bcacec...9ff81`; the OAuth cache is opened with no-follow semantics, checked by file descriptor, and copied without
rendering its contents. The child gets a closed environment, isolated non-project working directory, disabled updater,
generic web disabled, and all known local tools removed. It does not inherit API keys, proxy variables, project
instructions, or repository access.

The CLI does not accept hosted `x_search` in its local `--tools` allowlist. The process monitor therefore watches
structured session updates and kills the entire process group when it observes an unknown/non-X tool, a second X call,
oversized output/evidence, or the deadline. This limits further work but cannot prove that a second
server-side call was stopped before transport. A successful artifact still requires exactly one completed call and
exact raw-result-to-observation reconciliation; any ambiguity is a failed probe. The temporary transcript is deleted.
Only a minimized call receipt (ids, statuses, canonical post ids/URLs, author ids and model id) is retained.

## Budgets and kill switch

- one Grok execution;
- exactly one completed `x_search` call for success; a second observed call trips the kill switch and fails the run;
- one completed native-search result set (derived from the structured call receipt, not model prose);
- at most five observations;
- at most four model turns;
- 180-second subprocess deadline;
- reported monetary cost at most `$0.25`;
- no fallback, retry, graph expansion, or researcher task.

OAuth often omits monetary cost. The result must then record `cost_status=unreported` and `cost_usd=null`; capability
proof may still be measured with calls, turns, tokens and elapsed time, but cost efficiency remains unproven and cannot
be reported as free.

Any generic-web/local tool, second X call, budget overrun, invalid/duplicate post identity, missing structured provenance,
unsafe OAuth file, unbounded output, protected/proxy content, full-body retention, or product-writer field fails closed.
Failed results discard all observations.

Request/result/approval/tool files are written to a private staging directory and renamed as one atomic bundle. Each
result records its exact 24-hour deletion deadline. `--purge-expired` deletes expired bundles and leaves a
non-sensitive deletion receipt. `runtime/` is ignored by Git.

## Commands

Offline validation and tests:

```bash
PYTHONPATH=src ../sourcing-ai-agent/.venv/bin/python -m unittest \
  tests.test_x_first_live_probe_and_evaluation -v
../sourcing-ai-agent/.venv/bin/ruff check .
```

The only live entrypoint is:

```bash
PYTHONPATH=src ../sourcing-ai-agent/.venv/bin/python \
  scripts/run_live_capability_probe.py --execute-live
```

Existing private artifacts can be revalidated without external execution:

```bash
PYTHONPATH=src ../sourcing-ai-agent/.venv/bin/python \
  scripts/run_live_capability_probe.py \
  --validate-directory runtime/live-probes/<run_id>
```

Delete expired artifacts and record the purge:

```bash
PYTHONPATH=src ../sourcing-ai-agent/.venv/bin/python \
  scripts/run_live_capability_probe.py --purge-expired
```

## Stage 2 remains separate

Stage 1 cannot identify or rank researchers. Even `x_native_identity_ready` only makes a bounded Stage 2 design
eligible for an owner decision. Stage 2 requires its own request/result contract, query-family registry, independent
golden set, adjudication workflow, cost/rate budget, retention/deletion evidence, and independent review.

For a later scalable transport, the official xAI Responses API exposes only `x_search`, handle allowlists (up to 20),
date filters, structured X tool-call records, and sources. The official Batch API can queue many independently keyed
Responses requests and supports cancellation, per-request status, result pagination and cost tracking. Both require a
supported xAI API credential; the current Grok OAuth cache is not repurposed as an API key. Official `max_turns` limits
assistant/tool turns, not individual parallel calls, so even the API design must reconcile actual call records rather
than claiming a nonexistent `max_tool_calls` control.

Official references: [X Search](https://docs.x.ai/developers/tools/x-search),
[tool-call turns](https://docs.x.ai/developers/tools/tool-usage-details), and
[Batch API](https://docs.x.ai/developers/advanced-api-usage/batch-api).
