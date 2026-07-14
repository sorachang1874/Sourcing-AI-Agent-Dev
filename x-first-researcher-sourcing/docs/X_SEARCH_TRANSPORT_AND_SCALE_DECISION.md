# X Search transport and scale decision

> Status: Architecture decision for staged validation. No large live run, profile enrichment, researcher mapping, or
> scale claim has executed. Stage 1 remains review-pending.

## Decision

Use two separate transports rather than stretching the Grok CLI into a batch service:

1. **Grok CLI OAuth** is only the Stage 1 capability handshake. It may demonstrate that this installed client and
   account expose hosted native `x_search`, but that remains unproven before the live handshake. In pinned CLI 0.2.99,
   `--tools` is a built-in internal-id allowlist; neither the documented public streaming events nor binary strings
   establish that `--tools x_search` is a supported enforcement boundary. The runner does not depend on it, and the
   CLI cannot enforce a provider-side maximum number of tool calls.
2. **xAI Responses/Batch API** is the intended Stage 2/scale transport after a supported xAI API credential, a
   provider adapter, and a separate independent review exist. Requests expose only `x_search`, use handle/date filters,
   carry unique idempotency keys, and retain structured call/source receipts.

This avoids treating model prose as a scraper response and keeps provider execution separate from evidence
normalization, identity proposals, classification, adjudication, and product adapters.

## What official X Search does and does not establish

Official xAI documentation says `x_search` supports keyword search, semantic search, user search, and thread fetch.
API tool-call records may name `x_user_search`, `x_keyword_search`, `x_semantic_search`, or `x_thread_fetch`, but
server-side tool output is not returned; downstream evidence must bind model output to returned citations, tool-call
invocations, and server-side usage. Those API function names do not establish CLI internal ids. Conversely, the pinned
local binary's lack of an exact `x_keyword_search` string does not disprove hosted keyword search or prove/deny a CLI
`--tools` mapping.
It accepts `allowed_x_handles`/`excluded_x_handles` (up to 20) and date bounds. Responses expose structured
`x_search_call` entries and sources.

That proves a supported native search surface exists. It does **not** by itself prove that every call returns a full
post body, a complete user profile, Bio text, stable user id, handle history, relationship graph, or exhaustive result
set. Those are field-level capabilities that the live contract must observe and version. Model-generated summaries
never substitute for source records.

Stage 1 therefore asks for only five official-account posts and separately distinguishes:

- native post retrieval with stable post ids/URLs;
- stable external-account identity;
- profile/Bio field availability;
- explicit source/call receipts.

The current Stage 1 result retains a 280-character excerpt and deliberately rejects full-body persistence. A later
private raw-evidence lane may temporarily retain more provider fields only under its own schema, TTL, deletion receipt,
and review.

## Scalable task topology

```text
versioned query-family registry
        |
        v
normalized task ledger (lab + family + window + handle/source/turn cap)
        |
        +-- Responses API: bounded real-time experiments
        |
        +-- Batch API: many uniquely keyed asynchronous requests
        v
structured call/source receipts
        v
post normalizer + account/profile/Bio normalizer + mention edges
        v
affiliation / pretraining / region-evidence classifiers
        v
stable-account dedupe + quarantine + human adjudication
        v
evaluation artifact; no canonical product write
```

Each normalized task owns one lab, one query family, one frozen time window, one handle filter set, and explicit
source/turn caps. Retries reuse the same idempotency key and never count as new yield. Query families can run concurrently only
when cost/rate budgets and the task ledger remain atomic and observable.

The Batch API supports independently keyed requests, cancellation, per-request status, paginated results, cost
tracking, and file batches up to 50,000 requests. Those platform limits are not product defaults. Initial Stage 2 stays
at one lab, at most eight tasks, two tool turns per task, and 100 accepted raw observations; scale increases only from
measured unique yield, precision, recall, latency, review time, and cost.

Official `max_turns` limits assistant/tool-call turns, not individual parallel tool calls. Neither CLI nor current API
documentation exposes a `max_tool_calls` control. Every run must reconcile actual structured call records, fail an
overrun, and include the incurred calls in cost evidence. A prompt instruction or post-hoc counter is not a transport
cap.

## Bio, mentions, and regional-professional leads

Profile fields are decomposed instead of converted into one model judgment:

- display name and handle history: raw alias/identity lookup only;
- Bio language: directly observed language, not nationality or ethnicity;
- `Head of ... @org`: subject-claimed current affiliation proposal;
- `Prev @org`: subject-claimed previous affiliation proposal;
- explicit regional platforms/channels: regional-professional ecosystem verification lead;
- explicit worked/studied/researched/lived location: proposed physical region-experience evidence.

Mentioned organization accounts become reversible graph edges and follow-up tasks. Subject claims require organization
or independent professional evidence before confirmation. Chinese text or a China-platform reference can increase the
recall of a **verification queue**, but cannot by itself establish physical China experience, candidate eligibility,
ranking, nationality, or ethnicity. Real public profiles are kept out of repository fixtures; equivalent synthetic
Bio patterns test the parser and evidence state machine.

## Evaluation and iteration

Every query family reports marginal unique stable accounts and accepted evidence, not raw post volume. Primary gates
are evidence-qualified precision, conditional recall against an independently built golden set, and qualified unique
account yield per normalized task. Guardrails include false identity merges, unbound source claims, fallback calls,
duplicate tasks, full-body/TTL violations, product writes, and protected-identity inference.

Observed-language and regional-ecosystem lead families are evaluated separately:

- incremental verified region-experience recall;
- false-positive and unresolved rate;
- review minutes per additional confirmed account;
- overlap with stronger affiliation/publication/mention families.

A weak lead family is retired when it adds no independently verified account across two frozen windows or its review
cost exceeds the current champion without a material recall gain.

## Credential boundary

The official Responses and Batch examples use `XAI_API_KEY`. The existing Grok OAuth cache is private CLI state and is
not treated as, exported as, or reverse-engineered into an API credential. API implementation waits for a supported
credential path and separate owner decision.

## Official references

- [X Search](https://docs.x.ai/developers/tools/x-search)
- [Tool usage details](https://docs.x.ai/developers/tools/tool-usage-details)
- [Batch API](https://docs.x.ai/developers/advanced-api-usage/batch-api)
- [Pricing](https://docs.x.ai/developers/pricing)
