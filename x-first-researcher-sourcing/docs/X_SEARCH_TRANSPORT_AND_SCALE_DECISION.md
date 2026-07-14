# X Search transport and scale decision

> Status: Architecture decision plus a seven-wave bounded live campaign. Native-X CLI search and adaptive
> multi-strategy expansion are empirically proven; source-bound profile/Post enrichment, durable batching, and product
> promotion remain unproven.

## Decision

Use two separate transports rather than stretching the Grok CLI into a batch service:

1. **Grok CLI OAuth** is the interactive capability and bounded exploration transport. Seven CLI 0.2.99 sessions have
   now exercised hosted native keyword, semantic, user, and thread search. It is not promoted into a batch service. In CLI 0.2.99,
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

The official surface and reconciled live sessions prove a native search path exists. They do **not** prove that every call returns a full
post body, a complete user profile, Bio text, stable user id, handle history, relationship graph, or exhaustive result
set. Those are field-level capabilities that the live contract must observe and version. Model-generated summaries
never substitute for source records.

Stage 1 therefore asks for only five official-account posts and separately distinguishes:

- native post retrieval with stable post ids/URLs;
- stable external-account identity;
- explicit source/call receipts.

The later researcher exploration completed eight calls (`4` keyword, `2` semantic, `2` user search), reported 68
observations, and retained eight leads. The raw session proves tool/call/query execution, while returned provider Post
bodies remain encrypted in model context. Consequently its candidate excerpts are useful discovery leads but not
replayable source records. Model-mediated numeric account-ID presence was `12.5%` and Bio presence was `62.5%`;
neither is a source-bound field-capability claim.

The subsequent adaptive campaign expanded this to `702` raw native-X calls, `99` candidate rows, and `98` unique
handles across seven distinct strategies. It had no candidate, observation, or per-wave call success ceiling; each
process retained only emergency turn/deadline/kill controls. Waves 4–5 rebounded to `20` and `25` new handles after
an apparent decline, while the final two strategies fell to `5/104` and `1/70` new handles per raw call. This
supports coverage-aware, call-normalized diagnostics instead of a fixed packet limit. The operator paused for
hydration, but the replay evaluator remains `insufficient_proof / continue_expansion`; it does not claim exhaustive
coverage. It also does not upgrade the model-mediated Bios, IDs, or excerpts into source records.

Stage 1 recognizes provider post evidence only at the versioned closed, direct `rawOutput.posts[*]` path. Unread
provider metadata is tolerated but cannot become evidence. Nested diagnostic/request-echo objects, prose URLs,
incomplete registered child paths, duplicate records, and ambiguous author containers are not alternative evidence
paths. Process/inner failures still retain any separately parsed bounded outer request, turn, usage, cost, and partial
tool-call receipt; those failure receipts never become capability proof. Provider JSON is admitted only after an
iterative depth/node budget. If structured updates are missing or malformed but the outer envelope is valid, an
outer-only receipt keeps its request id, command/outer session ids, token usage, turns, and reported cost while
recording a null update digest, zero update bytes, and zero calls.

Failure evidence is terminal-total without becoming unbounded. Each capped provider collection carries a closed
`observed/relation/retained/truncated` projection shared by result usage/provenance and the tool receipt. The parser
retains the reviewed sample cap plus one in-memory overflow sentinel, publishes only deterministic bounded samples,
and uses `at_least` when the sentinel cannot prove an exact total. Consequently parallel-call, model-id, author-id,
post, unexpected-tool, and evidence-error overruns still publish one valid failed audit bundle after approval
consumption; they cannot disappear, claim the retained cap is the total, or become capability proof. The bundle is
validated while private staging still owns it and is renamed into the runtime only after that validation passes.

The current Stage 1 result retains a 280-character excerpt and deliberately rejects full-body persistence. It does not
probe or make any claim about profile/Bio availability. That requires an independently owner-approved later
field-capability probe with its own request/result schema, field-level source receipts, TTL, deletion receipt, and
independent review. A later private raw-evidence lane may temporarily retain more provider fields only under those
separate controls.

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
tracking, and file batches up to 50,000 requests. Those platform limits are not product defaults. A future Stage 2
transport can use finite operational batches, deadlines, cost/rate budgets, and emergency ceilings, but must not treat
a candidate/observation/call count as a recall target or success boundary. Expansion and convergence depend on
strategy coverage plus measured unique yield/call, precision, recall, latency, review time, and cost.

Official `max_turns` limits assistant/tool-call turns, not individual parallel tool calls. Neither CLI nor current API
documentation exposes a `max_tool_calls` control. Every run must reconcile actual structured call records, fail an
overrun, and include the incurred calls in cost evidence. A prompt instruction or post-hoc counter is not a transport
cap.

## Bio, mentions, and regional-professional leads

Profile fields are decomposed instead of converted into one model judgment:

- display name and handle history: raw alias/identity lookup only;
- Bio language: directly observed professional/technical language context, not nationality or ethnicity;
- `Head of ... @org`: subject-claimed current affiliation proposal;
- `Prev @org`: subject-claimed previous affiliation proposal;
- subject-owned China digital platforms/channels used for professional activity: strong China/Asia professional-
  experience proxy proposal;
- explicit worked/studied/researched/lived location: proposed physical region-experience evidence.

Candidate value likewise stays decomposed: target-lab affiliation temporality and pretraining-experience temporality
form a configured 2×2 matrix. Current/current is the narrow precision tranche; current/historical, historical/current,
and historical/historical remain experience-recall segments. `ambiguous|unsupported` rows stay in a bounded evidence-
hydration queue. Changing the target lab or segment priorities changes reviewed policy/config, not Python branches.

Mentioned organization accounts become reversible graph edges and follow-up tasks. Subject claims require organization
or independent professional evidence before confirmation. Chinese-language professional/technical content is a weak
China/Asia professional-experience proxy; subject-owned China digital-ecosystem professional activity is a strong
proxy. A versioned semantic policy, not a substring rule, owns that roll-up. Either may enter a high-recall human-
verification queue, but neither establishes physical China experience, protected identity, confirmed employment, or
outreach authority. Real public profiles are kept out of repository fixtures; equivalent synthetic Bio patterns test
the contract and state machine.

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

The current CLI raw-session owner also needs correction before another evidence-bearing gate: project-sanitized files
are owner-only, but the original `~/.grok` session tree was observed as group-readable. The next runner must isolate raw
evidence under owner-only permissions and bind TTL/purge evidence without mutating network, VPN, proxy, or DNS state.

## Official references

- [X Search](https://docs.x.ai/developers/tools/x-search)
- [Tool usage details](https://docs.x.ai/developers/tools/tool-usage-details)
- [Batch API](https://docs.x.ai/developers/advanced-api-usage/batch-api)
- [Pricing](https://docs.x.ai/developers/pricing)
