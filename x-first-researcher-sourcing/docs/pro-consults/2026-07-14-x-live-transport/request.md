Purpose: architecture
Secondary question sets: approach_review,milestone_audit
Authority: ADVISORY_ONLY
Surface required: Chat
Model required: GPT-5.6 Sol
Mode required: Pro
Browser required: Codex in-app browser
Local state: dirty
Connector sees dirty scope: false
Dirty scope provided to Pro: false
Branch: none
Branch requirement: not_applicable
Repository: none
Commit authority: none
Consultation status: planned
Connector status: unused
Redaction status: verified
CONNECTOR_SCOPE_JSON: {"mode":"unused"}

## Objective

Decide whether to keep, adjust, or pivot the staged transport and evidence architecture for an X-first AI-researcher
sourcing experiment before consuming one user-approved Grok/X live capability probe.

ADVISORY_ONLY — not an independent-review artifact or formal GO. Codex owns local facts, implementation, validation,
and disposition. The repository's non-author review gate remains authoritative. This consultation is not CI evidence,
legal/privacy/security approval, live-provider approval, or milestone signoff.

## Verified facts

- Local author commit `fee369923fac3db924cf97e55e90cf89fb740323` exists but is not transferred to ChatGPT or visible through a
  Connector in this consultation. Treat the following as a redacted architecture manifest, not repository proof.
- The installed Grok CLI is version 0.2.99 and its reviewed binary SHA-256 is pinned. It exposes structured session
  updates containing model id, tool-call id, tool metadata/name, status, and raw output.
- This CLI rejects hosted `x_search` as a local `--tools` allowlist entry. The current runner instead disables generic
  web search, removes known local tools, uses no subagents/memory/plan/updater, and fails any unexpected tool receipt.
- Official xAI documentation says native `x_search` supports keyword/semantic/user search and thread fetch, accepts
  handle allowlists of at most 20 and date bounds, and exposes structured `x_search_call` records/sources.
- Official xAI documentation explicitly says `max_turns` limits assistant/tool turns, not individual tool calls; one
  turn can invoke several tools in parallel. No documented `max_tool_calls` control was found.
- Official xAI Batch API supports uniquely keyed asynchronous Responses requests, cancellation, per-request status,
  paginated results, cost tracking, and file batches. Official examples require `XAI_API_KEY`. The existing CLI OAuth
  cache is not treated as or reverse-engineered into an API credential.
- The user approved one bounded Stage 1 live capability probe against the official OpenAI X account, not researcher
  mapping, retries, graph expansion, product writes, outreach, or scale-up.
- The Stage 1 runner atomically consumes that approval once before spawn; replays/concurrent contenders fail. It
  copies the verified binary and mode-0600 user-owned OAuth cache by file descriptor into an ephemeral home, uses a
  closed environment and isolated non-project cwd, caps stdio/session evidence/deadline, and kills the process group
  on an observed second X call or any non-X call.
- A successful artifact requires exactly one completed structured X call, observed model `grok-4.5`, stable numeric
  post ids and canonical URLs found in raw tool output, at most five retained observations, and raw author-id binding
  for stable-account readiness. Model prose alone cannot prove access or identity.
- Raw session updates are deleted. A private atomic bundle retains request/result, a one-shot approval receipt, and a
  minimized tool receipt containing call ids/statuses/model id/post ids/URLs/author ids. It expires after 24 hours and
  has a purge command/deletion receipt. Full post bodies are not persisted.
- The runner has nine targeted offline tests covering request mutation, structured tool proof, web/local/unknown and
  duplicate-call rejection, process-group kill on a second X call, raw-result mismatch, stable-id separation,
  binary/auth/environment isolation, approval replay, receipt tamper detection, private modes, and TTL purge. Ruff and
  JSON parsing pass. No live call has executed.
- The first executable workflow evaluator was withdrawn after adversarial review found zero-task acceptance,
  caller-supplied recovered ids, and self-reported guardrails. Only a methodology document remains.
- Current product truth: there is no live Bio/profile enrichment and no batch scheduler. Display names/handles are raw
  aliases only. A subject-authored Bio may later yield separate proposed records for observed language, current or
  previous `@organization` mentions, regional-professional ecosystem activity, and explicit physical region
  experience. Ecosystem/language leads can open verification work but cannot alone establish physical experience,
  eligibility, ranking, nationality, or ethnicity.

## Inferences

- Killing on the structured start event should limit continued execution, but cannot prove a second server-side call
  was stopped before transport; the artifact describes that limitation explicitly.
- CLI OAuth is convenient for the one-call capability handshake but is a poor batch-service boundary. Responses/Batch
  API is likely the cleaner scalable owner once a supported credential exists.
- `x_search` platform support for user search does not prove that every result exposes complete Bio/profile fields;
  field-level capability must be observed and versioned in a later contract.

## Unknowns

- Whether the current OAuth session is valid at execution time.
- The exact raw-output shape and field completeness of the installed CLI's first native X Search call.
- Whether one prompt can cause more than one parallel `x_search` call before the local monitor reacts.
- Whether a supported xAI API credential will be supplied for the future Responses/Batch adapter.
- The first measured precision, recall, unique-account yield, latency, review time, and cost.

## Constraints and non-goals

- Fixture validation remains offline and cannot read credentials or call a provider.
- No fallback to generic web, Apify, another provider, model memory, or unstructured self-report.
- No canonical person/evidence/assertion, CRM, projection, export, outreach, or product write.
- Stable X account identity is numeric platform user id plus handle history, not a canonical person identity.
- No name-based regional or protected-identity inference. Directly observed language/ecosystem activity remains a
  separate proposed verification lead, not a final classification.
- Stage 2 requires its own owner decision, schemas, scheduler, independent golden set, evaluator, live budgets, and
  non-author review.

## Validation already run

- `python -m unittest tests.test_x_first_live_probe_and_evaluation.XFirstLiveCapabilityContractTest -v` -> 9 passed in the project environment.
- `ruff check` on the live source, script, and test -> passed.
- `jq empty` on the four live request/result/approval/tool-receipt schemas -> passed.

## Questions

1. For exactly one low-cost capability handshake, should we keep the CLI runner with explicit post-hoc call
   reconciliation and kill-on-observation, adjust its contract, or pivot and require a supported Responses API key
   before any live call? Identify the decisive risk and the smallest discriminating validation.
2. Which source receipts and state transitions are minimally sufficient to distinguish native post retrieval, stable
   account identity, profile/Bio availability, subject-claimed organization mentions, corroborated affiliation, a
   regional-professional ecosystem lead, and explicit physical region experience?
3. What is the smallest Stage 2 Responses/Batch slice that proves multi-query unique-account yield without
   overfitting to one lab, prompt, Bio pattern, or provider response shape?
4. How should a future evaluator mechanically compute precision, conditional recall, marginal query-family yield,
   latency/review/cost efficiency, and guardrails without caller-reported recovered ids or self-attested incidents?
5. Which P0/P1 failure modes, ownership gaps, concurrency/idempotency concerns, retention risks, and field-level
   capability tests are still missing from this architecture manifest?

Return exactly one self-contained Markdown artifact between the markers below. Use exactly one unfenced lower-case
directive `Raw Pro verdict: keep`, `Raw Pro verdict: adjust`, or `Raw Pro verdict: pivot`. Do not say GO or formal
approval.

BEGIN_ARTIFACT path=x-first-researcher-sourcing/docs/pro-consults/2026-07-14-x-live-transport/response.md
ADVISORY_ONLY — not an independent-review artifact or formal GO.

# Verdict
Raw Pro verdict: <keep|adjust|pivot>

# Scope understood
...

# Assumptions and missing information
...

# Findings
## P0
...
## P1
...
## P2
...

# Recommended sequence
...

# Validation and failure modes
...

# Deferred decisions
...

# Owner decisions required
...
END_ARTIFACT
