# Stage 2 experiment and evaluation design

> Status: design-only, not an execution approval. Stage 1 native-X access remains subject to a pinned independent
> review and one successful bounded handshake. No Stage 2 provider adapter, live profile probe, Batch runner, durable
> task ledger, executable evaluator, API credential path, or canonical product writer exists yet.

## Decision this design supports

Stage 2 answers three separate questions. A positive answer to one never implies the others:

1. **Field capability** — which Post and profile fields are returned exactly, bounded, absent, or still unverified?
2. **Search quality** — does a frozen set of native-X queries find evidence-qualified, current pretraining researchers
   with useful precision, conditional recall, and marginal yield?
3. **Batch transport** — can many independently keyed requests reach terminal states with complete call, usage, cost,
   cancellation, pagination, and result reconciliation?

The goal is an AI-native evidence pipeline: Grok plans and executes native X searches, while deterministic contracts
own identity, provenance, budgets, state, dedupe, evaluation, and promotion boundaries. Model prose is never a source
record, a stable identity, an employment assertion, or proof of exhaustive coverage.

This sibling stops before canonical materialization. It may produce reviewed versioned artifacts, but it must not
write PersonAsset, evidence, assertions, CRM, projection, export, outreach, or another `sourcing-ai-agent` owner.

## Gate sequence

```text
Stage 1 native-X handshake
        |
        | exact x_search receipt + bounded source-bound Post evidence
        v
Stage 2A offline contracts and adversarial fixtures
        |
        +--> Post/profile field canary ----> field-capability verdict
        |
        +--> frozen search canary --------> search-quality verdict
        |
        +--> five-request Batch canary ---> batch-transport verdict
        v
owner-reviewed multi-window / multi-lab experiment
```

Every arrow fails closed. Batch is deliberately last: a high-throughput queue must not be the first place a basic
field, identity, provenance, or query-semantics drift is discovered.

## Future artifact owners

The smallest Stage 2A slice introduces four independent envelopes. Names below are proposed; implementation requires
schema, executable validator, deterministic fixtures, tests, and a new non-author review in one batch.

| Envelope | Owner / source of truth | Required contents | Forbidden authority |
| --- | --- | --- | --- |
| `x.stage2.experiment.request.v1` | Experiment owner | frozen lab/window, registry and prompt digests, model/tool policy, handle filters, source/turn/observation/cost/deadline caps, task keys | No provider result or caller-reported KPI |
| `x.stage2.collection.v1` | Collection/task-ledger owner | terminal task rows, provider call/source/citation receipts, accounts, Posts, profiles, source-match refs, quarantine, incidents, coverage, retention | `assertions=[]`, `canonical_writes=[]`; no inferred exhaustive coverage |
| `x.stage2.gold_and_adjudication.v1` | Independent evaluation owner | pre-provider golden manifest, two reviewers, conflicts, development/blind split, evidence hashes, hard negatives | Provider discoveries cannot create or relabel gold rows |
| `x.stage2.evaluation.v1` | Deterministic evaluator | mechanically recomputed KPIs, intervals, guardrails, segment results, decision and reason codes | No caller aggregates, provider calls, identity merge, or product write |

The source-neutral patterns to reuse from `sourcing-ai-agent` are semantic, not runtime dependencies:

- asset -> evidence -> assertion separation;
- stable external identity distinct from execution and shard facts;
- deterministic queue item identity and provider lineage;
- explicit coverage proofs rather than mode-string inference;
- row-level quality recomputation and incident/retention ledgers.

Integration remains a versioned artifact adapter. This sibling must not import `sourcing_agent` runtime modules.

## Deterministic task identity

A normalized task is the retry, dedupe, progress, cost, and evaluation grain. Its idempotency key must bind all fields
that can change the external request or its interpretation:

```text
lab_id
query_family_id + query_family_version
query_text_digest + prompt_version + prompt_digest
model_id + provider_transport + tool_policy_digest
frozen_from + frozen_to
allowed/excluded handle sets
source_cap + turn_cap + observation_cap
field_capability_version + normalization_contract_version
```

Retry attempt, Batch id, worker id, queue position, latency, and execution timestamp are facts, not identity inputs.
One logical task keeps one idempotency key across retries. A retry cannot be counted as new yield, and an exact duplicate
external call is a hard violation.

## Field-capability contract

Every requested field has one explicit state:

- `present_exact` — source-bound value was independently verified byte-for-byte or identifier-for-identifier;
- `present_bounded` — a declared excerpt or normalized projection is available, but the full source value is not;
- `absent` — the reviewed provider response explicitly lacked the field;
- `unverified` — the transport or receipt cannot establish presence or absence.

The first Post field registry should include stable Post id, canonical URL, numeric author id, handle, authored time,
text, reply/quote/thread relation, media references, engagement snapshot, and language observation. The first profile
registry should include numeric user id, handle, Bio, Bio content hash/version, profile URL, and observed time.

A citation or model answer cannot silently promote `unverified` to `present_exact`. Handle alone cannot establish
account identity. Bio text and organization mentions remain evidence proposals; they do not confirm employment.

### Raw evidence and retention

The current Stage 1 excerpt contract must not be widened in place. Any full-body experiment needs a separate private
raw-evidence lane with:

- source/call/citation binding and raw plus normalized SHA-256;
- owner-only storage, no repository fixture with real profiles, and no product adapter;
- an explicit short TTL, deletion state, deletion receipt, and fail-closed purge validation;
- a minimized durable artifact that survives raw deletion without claiming fields it no longer proves.

Unknown retention state, an unverified deletion, or a full body in the normal collection artifact is `no_go`.

## Stage 2A offline fixture slice

Before any Stage 2 live request, deterministic `.invalid` fixtures must cover:

- exact, bounded, absent, and unverified Post/profile fields;
- a handle rename with stable numeric account id;
- the same Post matched by multiple query families without duplicate identity;
- handle-only and conflicting-author quarantine;
- missing/mismatched citations and cross-account evidence;
- duplicate and near-duplicate tasks, calls, and Batch result ids;
- budget/deadline overrun, partial/cancelled/late results, and missing terminal rows;
- profile/Bio unavailable, malformed nested provider values, and retention deletion failure;
- zero tasks, NaN/Infinity, fake provenance, omitted selected packets, and forged aggregate KPIs.

All metrics must be recomputed from task, observation, packet, gold, adjudication, incident, and retention rows. The
evaluator must reject a valid-looking decision whose underlying rows do not reproduce it.

## Stage 2B bounded canary

The first owner-reviewed canary remains intentionally small:

- one lab: OpenAI;
- one frozen time window;
- four high-authority discovery tasks:
  `official_lab_output`, `first_party_technical_posts`, `official_lab_interactions`, and
  `paper_conference_linkage`;
- after stable account discovery, at most one separate profile task covering at most five accounts;
- at most five tasks total, two model/tool turns per task, and 100 retained observations;
- no retry, provider fallback, generic web fallback, canonical write, export, outreach, or automatic identity merge;
- owner-pinned cost and deadline caps; missing provider cost is `unreported`, never zero.

### Independent golden set

Build the golden set before looking at Grok output, using official lab people/research pages, official technical
reports/blogs, and papers. The first directional set contains 12-20 positives and 8-12 hard negatives, double-reviewed
with conflicts preserved. Hard negatives include former staff, non-pretraining roles, parody/aggregator accounts,
common-name conflicts, handle renames, mention-only accounts, and coauthors without current-affiliation evidence.

Provider discoveries may be added to a later challenger set, never retroactively to the frozen evaluation set.

## Separate acceptance verdicts

### A. Post field capability

- at least ten source-bound Posts;
- `10/10` exact agreement for every field claimed `present_exact`;
- `100%` binding across task, call, citation, Post id, canonical URL, and author account;
- zero cross-account, duplicate-object, full-body/TTL, or model-prose substitution violations.

If full text is not exact, the verdict is `bounded_excerpt_only`. That does not invalidate a separately successful
native-search verdict.

### B. Profile/Bio capability

- at least five profiles;
- `5/5` exact agreement for numeric user id, handle, Bio value/hash, and observation binding when claimed present;
- no name-based merge and no model-generated Bio repair.

Failure makes the profile lane `no_go`; it cannot be filled from Post prose or inferred from language.

### C. Search feasibility

Let `S` be all selected packets, `A` the unique manually accepted evidence-complete packets, `G` the frozen golden
population, `Gx` the independently verified public-X subset, and `T` completed normalized discovery tasks.

| Dimension | First canary gate |
| --- | --- |
| Terminal and coverage | `5/5` tasks have terminal, coverage, and call-accounting rows; every selected packet stays in the denominator |
| Evidence binding | `100%` of retained observations bind task/call/citation/object id/URL |
| Stable identity | `100%` of accepted packets have numeric platform user id |
| Evidence completeness | `>=90%` of selected packets have independent current-affiliation and pretraining-relevance evidence |
| Precision | `|A| / |S| >= 80%` |
| Conditional recall | `|A intersect Gx| / |Gx| >= 50%` |
| Minimum yield | reviewed unique `>=5`, accepted unique `>=5`, and `|A| / T >= 1.0` |
| Dedupe | false merge `=0`; exact duplicate task/call `=0`; near-duplicate task rate `<=10%` |
| Guardrails | fallback, cross-account evidence, overrun, unbound claim, product write, and retention violation all `=0` |
| Cost | every call has usage; known total stays within request cap; unknown remains `unreported` |
| Latency | finite per-task/run latency plus P50, P95, and time-to-first-five; first run establishes a baseline |
| Novelty | at least two of four discovery families each add one accepted id; stop after two consecutive zero-marginal families |

Precision and conditional recall always include Wilson 95% intervals. Small-sample point estimates are feasibility
evidence, not production claims. End-to-end recall `|A intersect G| / |G|` is reported alongside X-account availability
`|Gx| / |G|`; excluding people without a verified X account must not inflate the headline result.

### D. Batch transport

Only after the field/search canary, submit five uniquely keyed requests through the future supported API path. GO
requires:

- exactly one terminal result for each of five custom/task ids;
- no missing, duplicate-matched, duplicate-billed, retried, or orphan result;
- `100%` reconciliation of request, call, status, usage, cost, result, and pagination;
- cancellation/deadline produces explicit terminal rows, and late results cannot become success;
- Batch success makes no coverage or exhaustiveness claim.

The provider's maximum Batch file size or request count is a platform ceiling, not a product default, a per-query Post
limit, or proof of recall.

## Iteration and scale gate

Champion/challenger runs change exactly one versioned variable: query-family text/order, field request, source cap,
selection threshold, or evidence rule. Frozen lab/window, golden holdout, model/transport, budgets, adjudication rubric,
and no-fallback rule stay fixed.

A challenger replaces the champion only under the existing evaluation contract's bounded trade-offs and zero hard
guardrails. Overlapping small-sample intervals require another frozen window.

Method-level scale requires three runs, at least two labs, at least 50 reviewed selected packets, precision `>=90%`
with Wilson lower bound `>=80%`, blind conditional recall `>=70%`, and no pooled metric hiding a failing lab or
relevance segment. Efficiency cannot regress more than 10% unless another efficiency metric improves at least 15%.

## Empirical questions that remain open

Stage 1 and later canaries must measure rather than assume:

1. whether this installed OAuth CLI actually completes hosted native `x_search` with a stable receipt shape;
2. whether returned Posts include numeric author ids and exact authored timestamps;
3. whether text is complete, truncated, normalized, or only recoverable through citations;
4. which reply/quote/thread/media/engagement/profile fields are available and source-bound;
5. whether profile search binds numeric user id, handle, and Bio in the same evidence record;
6. whether handle/date filters exclude out-of-scope hard negatives without leakage;
7. exact attempted versus successful tool calls, usage, turns, latency, and cost;
8. Batch tool support, result pagination, cancellation, expiration, cost, and late-result behavior;
9. query recall and ordering stability across model versions and frozen windows;
10. the difference between many submitted requests and many unique, evidence-qualified Posts/accounts.

## Owner decisions still required

Before Stage 2 live work, the owner must separately approve:

- the four contract/envelope versions and their independent review scope;
- a supported xAI API credential path; Grok OAuth must not be exported or reverse-engineered into one;
- exact model, transport, cost/deadline/turn/source/observation budgets and kill switch;
- the private raw-evidence fields, TTL, purge owner, and deletion evidence;
- the golden-set reviewers, split, adjudication rubric, and private storage;
- the first live field/search canary; then, separately, the five-request Batch canary;
- any later lab expansion, retry policy, product adapter, canonical materialization, or outreach use.

Until those decisions and reviews exist, Stage 2 remains fixture/design work only.
