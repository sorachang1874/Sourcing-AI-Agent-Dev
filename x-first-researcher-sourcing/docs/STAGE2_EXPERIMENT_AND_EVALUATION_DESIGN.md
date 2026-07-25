# Stage 2 experiment and evaluation design

> Status: design plus exploration evidence and one bounded Stage 2A offline field-capability remediation candidate
> after the pinned `2700d10` `NO-GO`, not a promotion approval or replacement review `GO`. Seven later Grok CLI
> sessions proved native-X
> keyword, semantic, user, and thread search plus adaptive recall expansion, but did not prove replayable Post bodies
> or complete profile fields. No
> Stage 2 provider adapter, reviewed live profile probe, Batch runner, durable task ledger, search-quality evaluator,
> supported API credential path, or canonical product writer exists yet.

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

### Observed exploration checkpoint

The first 2026-07-14 session completed eight hosted-X calls; a later seven-wave campaign mechanically observed `702`
native-X calls and retained `99` rows / `98` unique handles. New unique yield was
`29/12/6/20/25/5/1`, showing the value of strategy diversity and a low-yield final frontier. “Can this installed
client/account perform adaptive native-X researcher discovery?” is no longer open for those runs. Exhaustiveness
remains open: the replay stop owner returns `insufficient_proof / continue_expansion` because legacy request context
and query-family call attribution are incomplete.

The next gate begins at field hydration: after handle merge, `95/98` leads contain model-mediated Bio text and `48/98`
contain a model-mediated numeric platform ID, but neither is a source-bound snapshot. Retained Post bodies are
likewise model-mediated and not replayable. The replay copy is owner-only, but the original Grok session permissions
still fail the owner-only retention requirement. These gaps prevent promotion or batching but do not negate the
search result.

## Stage 2A artifact owners

The bounded field-capability slice implements four independent offline envelopes. Their outer shapes are closed by
JSON Schema; deep replay, digest, terminal, retention, and authority rules are executable. This does not implement a
provider adapter, live runner, search-quality experiment, or Batch scheduler. See
`STAGE2_FIELD_CAPABILITY_CONTRACT.md`.

A separately supplied `x.stage2.external_selection.fixture.v1` manifest owns the unique five-row fixture selection.
It is not a fifth result envelope and does not self-prove search output: the request binds its id, version, digest,
selected count, and selected-row digest and must cover the same lead/candidate/account denominator exactly once. The
comparison covers each row's opaque lead ref, full candidate SHA-256, lookup handle, reported numeric-id value, and
reported-id status; matching only a subset is invalid.

The experiment request also owns a closed scenario manifest before collection construction. Each task is bound to one
closed `scenario_id` and exact terminal, error/quarantine, field-state, and source-count semantics. The request stores
both the complete scenario-manifest SHA-256 and expectation-semantics SHA-256. The capability expectation must copy
those exact semantics, bind the complete request SHA-256, and reproduce both digests. Collection output cannot mutate
or choose its own expectation.

| Envelope | Owner / source of truth | Required contents | Forbidden authority |
| --- | --- | --- | --- |
| `x.stage2.experiment.request.v1` | Experiment owner | frozen lab/window, registry digest, complete source-manifest denominator, request-frozen scenario semantics/digests, per-task native-X-only tool policy, technical parser/execution ceilings, retention and zero-authority policy | No provider result, live execution authority, or caller-reported KPI |
| `x.stage2.collection.v1` | Collection/task-ledger owner | source-derived terminal task rows and field states, offline call/source receipts, exact profiles, bounded Posts, mechanically derived quarantine/incidents, retention | `assertions=[]`, `canonical_writes=[]`, `outreach_actions=[]`; no inferred exhaustive coverage |
| `x.stage2.capability_expectation.v1` | Offline fixture-scenario owner | request-bound terminal/error/quarantine/field/source expectations frozen before collection construction | Collection output cannot create or relabel expectations; no accuracy-gold or human-adjudication claim |
| `x.stage2.evaluation.v1` | Deterministic evaluator | recomputed terminal/field counts, fixture-expectation conformance, guardrails, decision and reason codes | No search-quality KPI, interval, segment claim, caller aggregate, provider call, identity merge, or product write |

The source-neutral patterns to reuse from `sourcing-ai-agent` are semantic, not runtime dependencies:

- asset -> evidence -> assertion separation;
- stable external identity distinct from execution and shard facts;
- deterministic queue item identity and provider lineage;
- explicit coverage proofs rather than mode-string inference;
- row-level quality recomputation and incident/retention ledgers.

Integration remains a versioned artifact adapter. This sibling must not import `sourcing_agent` runtime modules.

## Deterministic task identity

A normalized task is the retry, dedupe, progress, cost, and evaluation grain. Stage 2A now hashes each task-local
lookup contract together with `x.stage2.task_scope.v1`: target lab and frozen window, field-registry version/digest,
normalization-contract version, and external selection-manifest version/id/digest. Tool policy, requested fields,
lookup identity, candidate digest, reported-id diagnostic, fixture scenario, receipt requirement, and zero authority
remain task-local
identity inputs. Every receipt/source/profile/Post identity descends from that task id. Changing the lab, window, or
any interpretation digest (including technical-limit, retention-policy, and request-authority digests) therefore
cannot reuse old task or child ids.

Retry attempt, Batch id, worker id, queue position, latency, and execution timestamp are facts, not identity inputs.
One logical task keeps one idempotency key across retries. A future live query task must extend the scope with its
query/prompt/model/budget versions rather than reusing this offline task version.

## Field-capability contract

Every requested field has one explicit state:

- `present_exact` — source-bound value was independently verified byte-for-byte or identifier-for-identifier;
- `present_bounded` — a declared excerpt or normalized projection is available, but the full source value is not;
- `absent` — the reviewed provider response explicitly lacked the field;
- `unverified` — the transport or receipt cannot establish presence or absence.

The implemented v1 Post registry includes stable Post id, canonical URL, numeric author id, handle, authored time,
bounded excerpt, and thread relation. The implemented profile registry includes numeric user id, handle, profile URL,
Bio, Bio content hash/version, and observed time. Full text, media, engagement, and language observations require a
future registry version rather than being inferred from this narrower slice.

A citation or model answer cannot silently promote `unverified` to `present_exact`. Handle alone cannot establish
account identity. Bio text and organization mentions remain evidence proposals; they do not confirm employment.

Task-row field states are derived from retained raw records and their normalized consumers rather than accepted as
caller summaries. A replayable full profile/Post payload must be consumed by normalization or its derived
quarantine/incident; a metadata-only trace is non-replayable and leaves fields `unverified`. A present Post can never
be reported `absent`. With no Post payload, Post fields are `absent` only when a replayable profile response explicitly
records their absence; otherwise they remain `unverified`. A valid Post without a profile uses the typed
`completed_post_only/profile_source_unavailable_post_retained` terminal: Post fields remain source-bound while all
profile fields stay `unverified`, and no profile/person identity is synthesized.

Receipt, source, task, profile and Post identities close bidirectionally. Source observation timestamps stay inside
their receipt window, a profile Bio timestamp equals its source observation, and a Post cannot be authored after it
was observed. Numeric canonical Post ids are unique collection-wide. Quarantine/incident reason codes are derived
from actual retained conflicts, so a caller cannot swap one valid code for another and merely rehash ids.

Raw profile, Post, and metadata records are exact closed shapes before field-state derivation, even when their task
later quarantines. All URLs are canonical reserved `.invalid` URLs; extra credential-like/private text or live URLs
reject. Provider provenance is not a free path string: source transport/result-contract/result-type/ordinal are
closed, receipt-bound, request-tool-bound, and unique within each receipt result slot.

### Raw evidence and retention

The current Stage 1 excerpt contract must not be widened in place. Any full-body experiment needs a separate private
raw-evidence lane with:

- source/call/citation binding and raw plus normalized SHA-256;
- owner-only storage, no repository fixture with real profiles, and no product adapter;
- an explicit short TTL, deletion state, deletion receipt, and fail-closed purge validation;
- a minimized durable artifact that survives raw deletion without claiming fields it no longer proves.

Unknown retention state, an unverified deletion, or a full body in the normal collection artifact is `no_go`.
For the offline simulation, `created_at` and `delete_after` must bound every retained receipt start/completion, source
observation, profile Bio observation, and Post authored timestamp. The evaluator derives the retention-violation
guardrail from that same function; it is not a hardcoded zero.

## Stage 2A offline fixture slice

The first bounded field-capability slice is author-complete offline. Its deterministic five-task fixture covers an
exact profile/same-account bounded Post, conflicting numeric ids, handle rename, metadata-only tool trace, unbound
Bio, a source-bound Post with no profile, cross-account Post, missing terminal rows, retention drift, and zero-authority
violations. The exact state result is
`5 terminal = 1 completed + 1 completed_post_only + 2 quarantined + 1 failed`. A metadata-only trace cannot promote any field above
`unverified`.

The broader Stage 2 program still requires the following separate fixtures before search-quality or Batch promotion:

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

The implemented evaluator recomputes terminal counts, all 14 field-state denominators, capability-expectation
conformance, and hard guardrails from the bound request/collection/expectation rows. It rejects a valid-looking
decision whose underlying rows do not reproduce it. It intentionally emits no precision/recall KPI, confidence
interval, population segment result, or human-adjudication result. Its decision is mechanically
`offline_fixture_expectation_conformant` only for zero mismatches and
`offline_fixture_expectation_mismatch` otherwise.

Validation performs a bounded byte/depth/node scan before JSON Schema traversal. Deep JSON Schemas plus explicit
runtime checks keep booleans and integers type-strict rather than accepting Python equality aliases. `collection_id` hashes every
material collection field except itself, including rows, receipts, raw and normalized evidence, quarantine,
incidents, retention, empty authority-write arrays, and authority; rehashing only a convenient subset cannot hide a
mutation.

Source-conflict precedence is total: zero profile payloads with no Post fail as unavailable; Post-only payloads retain
their source-bound Post under the typed Post-only outcome; multiple replayable profile payloads
quarantine as `multiple_profile_sources`, malformed/multiple numeric ids take precedence over collection-wide
id/handle conflict, then lookup-handle rename and cross-account Post evidence follow. This prevents present payloads
from being erased as unavailable and gives combined multi-id/cross-handle evidence one legal terminal state.

The collection-wide account reducer is bidirectional: one id under multiple handles and one handle under multiple ids
both quarantine every affected task. Same-time reverse ownership is always a conflict. Different-time reassignment
also remains quarantined until a future schema supplies explicit handle-history intervals.

## Stage 2B bounded canary — future design only

Everything from this heading through the iteration/scale gate is an unimplemented search-quality and transport
design. None of these golden-set, KPI, interval, segment, or Batch claims are outputs of the Stage 2A fixture
evaluator.

The first owner-reviewed canary remains intentionally small:

- one lab: OpenAI;
- one frozen time window;
- four high-authority discovery tasks:
  `official_lab_output`, `first_party_technical_posts`, `official_lab_interactions`, and
  `paper_conference_linkage`;
- after stable account discovery, at most one separate profile task covering at most five accounts for this first
  live safety canary only;
- at most five tasks total, two model/tool turns per task, and 100 retained observations;
- no retry, provider fallback, generic web fallback, canonical write, export, outreach, or automatic identity merge;
- owner-pinned cost and deadline caps; missing provider cost is `unreported`, never zero.

The five-account limit is not a generalized hydration-queue cap or a business success target. The offline v1 request
uses a `10,000`-task technical ceiling solely for parser/execution protection; later volume and convergence remain
owner-controlled and evidence-driven.

### Independent golden set

Build the golden set before looking at Grok output, using official lab people/research pages, official technical
reports/blogs, and papers. Build two linked gold views: a narrow current-target-lab/current-pretraining precision set
and an experience-recall set spanning all four current/historical lab × current/historical pretraining combinations.
The first directional set contains 12-20 supported rows and 8-12 hard negatives, double-reviewed with conflicts
preserved. Former staff with supported pretraining experience are positives in the experience view, not automatic hard
negatives. Unsupported affiliation/experience, parody/aggregator accounts, false identity joins and unbound claims are
hard negatives; ambiguous rows remain explicit hydration cases.

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

Let `Scc`/`Acc` be the selected/accepted current-current precision tranche, `Eexp` the evidence-complete four-segment
experience population, `Gcc`/`Gexp` the corresponding frozen golden views, their `_x` subsets the independently
verified public-X rows, and `T` completed normalized discovery tasks.

| Dimension | First canary gate |
| --- | --- |
| Terminal and coverage | `5/5` tasks have terminal, coverage, and call-accounting rows; every selected packet stays in the denominator |
| Evidence binding | `100%` of retained observations bind task/call/citation/object id/URL |
| Stable identity | `100%` of accepted packets have numeric platform user id |
| Evidence completeness | `>=90%` of precision-selected packets have independent lab-affiliation and pretraining-experience evidence |
| Precision | `|Acc| / |Scc| >= 80%` |
| Conditional recall | report both `|Acc intersect Gcc_x| / |Gcc_x|` and `|Eexp intersect Gexp_x| / |Gexp_x|`; first canary target `>=50%` for each |
| Segment retention | all four supported current/historical combinations remain visible; `needs_evidence` cannot disappear |
| Minimum yield | reviewed unique `>=5`, experience-accepted unique `>=5`, and `|Eexp| / T >= 1.0` |
| Dedupe | false merge `=0`; exact duplicate task/call `=0`; near-duplicate task rate `<=10%` |
| Guardrails | fallback, cross-account evidence, overrun, unbound claim, product write, and retention violation all `=0` |
| Cost | every call has usage; known total stays within request cap; unknown remains `unreported` |
| Latency | finite per-task/run latency plus P50, P95, and time-to-first-five; first run establishes a baseline |
| Novelty | at least two of four discovery families each add one accepted id; stop after two consecutive zero-marginal families |

Precision and conditional recall always include Wilson 95% intervals. Small-sample point estimates are feasibility
evidence, not production claims. Both end-to-end recall views are reported beside their X-account availability;
excluding people without a verified X account must not inflate either headline result.

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

1. whether the observed CLI call/event shape remains stable across a second frozen window and client version;
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

Before Stage 2 promotion or volume expansion, the owner must separately approve:

- the four contract/envelope versions and their independent review scope;
- a supported xAI API credential path; Grok OAuth must not be exported or reverse-engineered into one;
- exact model, transport, cost/deadline/turn/source/observation budgets and kill switch;
- the private raw-evidence fields, TTL, purge owner, and deletion evidence;
- the golden-set reviewers, split, adjudication rubric, and private storage;
- the reviewed five-lead field-hydration canary; then, separately, the five-request Batch canary;
- any later lab expansion, retry policy, product adapter, canonical materialization, or outreach use.

Until those decisions and reviews exist, Stage 2 remains design/diagnostic work only; the completed exploration is not
a Stage 2 `GO`.
