# X-first workflow evaluation contract

> Status: Design methodology only. The first executable draft was withdrawn after adversarial review found that
> caller-reported counters could pass with zero tasks and inflate recall. Live Stage 2 inputs do not yet exist;
> thresholds below are proposed feasibility gates, not executable policy, production claims, or permission to scale.

## Decision this framework supports

The evaluator answers whether an X-first researcher-discovery workflow is accurate, coverage-positive, operationally
efficient, and safe enough for another bounded owner-reviewed experiment. It does not decide canonical identity,
employment, promotion, outreach, or product integration.

The evaluation grain is fixed as `lab + query family + frozen time window + handle/source/turn cap`. Splitting one task into smaller
requests cannot improve yield. Every selected packet stays in the precision denominator until adjudicated; unresolved
or incomplete packets cannot disappear from metrics.

## Three primary KPIs

Let:

- `S` be every packet selected as review-ready;
- `A` be the unique packets manually confirmed to have current lab affiliation, `PRETRAIN_CORE|PRETRAIN_ADJACENT`
  relevance, complete evidence, and a stable X platform user id;
- `G` be every independently labeled in-scope subject in the frozen golden set;
- `Gx` be subjects in `G` with an independently verified public X account;
- `T` be completed normalized tasks.

| KPI | Formula | Why it changes a decision |
| --- | --- | --- |
| Evidence-qualified precision | `|A| / |S|` | Measures reviewer-facing quality without hiding unresolved packets |
| End-to-end reachable recall | `|A intersect G| / |G|` | Measures actual population coverage instead of excluding people without X |
| Qualified reachable yield | `100 * |A| / T` | Measures useful output at a stable operational grain |

Recall is decomposed into `X account availability = |Gx|/|G|` and
`conditional search recall = |A intersect Gx|/|Gx|`. This separates platform reachability from query quality.
The future executable evaluator must report Wilson 95% intervals for precision and conditional recall; small samples cannot be
presented as precise production estimates.

## Drivers and diagnostics

- X-native observation validity and stable-account-id coverage;
- evidence-complete selected-packet rate;
- query-family marginal unique accepted ids per normalized task;
- quarantine rate and one primary failure owner per packet;
- exact duplicate task rate and normalized near-duplicate rate;
- time to first five accepted accounts, P50/P95 task latency, accepted accounts per minute;
- review minutes per accepted account;
- reported provider cost per accepted account; missing cost remains `unreported`, never zero;
- double-review agreement for golden labels and packet adjudication.

Failure ownership uses the closed families `capability`, `query_miss`, `pagination`, `provenance`, `identity`,
`affiliation`, `relevance`, `dedupe`, `review`, and `retention`. Optimization must address the owning failure rather
than masking it in a downstream score.

## Hard guardrails

Any nonzero violation makes the run `no_go` regardless of KPI values:

- protected-trait or proxy query/ranking/label use;
- false identity merge, cross-account evidence, or handle-only automatic merge;
- generic web, search-engine, Apify, or other provider fallback;
- unverified provenance/request hash/stable object id/canonical URL;
- canonical PersonAsset/evidence/assertion, CRM, projection, export, outreach, or other product write;
- budget/deadline overrun or retry that creates a duplicate external call;
- full-body retention or unverified TTL deletion;
- incomplete terminal artifact or coverage ledger;
- exact duplicate normalized task; near-duplicate rate above 10%.

Discovery and ranking population is only lab, current professional affiliation, and pretraining relevance. Name,
language, region, school, community, biography, post text, mentions, or graph position cannot infer or proxy ethnicity,
nationality, race, citizenship, religion, gender, or another protected identity. The stricter sibling rule controls even
where a source project's general query guardrail permits region/language as an explicit outreach skill.

## Golden-set contract

The real golden set must be built independently from official lab people/research pages, official technical reports,
papers, and official blogs before reviewing Grok output. Building it from the provider's discoveries would leak the
test and inflate recall. Real rows remain in ignored private runtime storage; the repository stores only schema,
synthetic fixtures, hashes, counts and split metadata.

Required row shape:

```text
gold_subject_id                 opaque
lab_id
as_of
affiliation_label               current_confirmed|former|unknown|out
relevance_label                 PRETRAIN_CORE|PRETRAIN_ADJACENT|OUT_OF_SCOPE|UNKNOWN
x_presence                      verified_public|no_verified_account|unknown
platform_user_id                only when independently verified
handle_history
affiliation_evidence_refs
relevance_evidence_refs
x_identity_evidence_refs
label_status                    double_reviewed|conflict
split                           development|blind_holdout
```

Hard negatives include former employees, non-pretraining roles, parody/aggregator accounts, common-name conflicts,
handle renames, mention-only accounts, list/graph-only accounts, and coauthors without current-affiliation evidence.
The first directional lab set should contain 12-20 positives and 8-12 hard negatives. Method-level scale claims need
at least 30 blind positives, 50 reviewed selected packets, two labs, and three frozen windows.

## Proposed feasibility and scale gates

The first Stage 2 feasibility policy should require:

- at least five reviewed and five accepted unique accounts;
- evidence-qualified precision >=80%;
- conditional search recall >=50%;
- evidence completeness >=90%;
- all hard guardrails zero.

Cost, review time, latency, and query-family marginal yield establish baselines on the first run rather than using an
invented absolute efficiency target.

Multi-lab method validation additionally requires three runs, two labs, at least 50 reviewed selected packets,
precision >=90% with Wilson lower bound >=80%, blind conditional recall >=70%, and no pooled metric hiding a failing
lab or relevance segment. Relative to the champion, efficiency cannot regress more than 10% unless another efficiency
metric improves at least 15%.

## Iteration protocol

Use champion/challenger runs and change exactly one versioned variable per iteration: query-family text/order, page
cap, packet-selection threshold, or evidence rule. Lab, golden holdout, time window, Grok model/access mode, budgets,
adjudication rubric, and no-fallback rule stay fixed.

A challenger may replace the champion only when guardrails remain zero and one condition holds:

- precision decreases by at most 2 percentage points and recall improves by at least 5 points;
- precision decreases by at most 2 points and cost or review minutes per accepted account improves by at least 15%;
- precision improves by at least 5 points while recall and efficiency each regress by at most 10%.

Overlapping small-sample intervals require a second frozen window. Stop a query/prompt version when Stage 1 lacks
native provenance/stable identity, the high-authority tranche yields no accepted packet, two consecutive families add
no accepted id, or marginal cost per new accepted account exceeds twice the champion.

## Executable evaluator

There is intentionally no executable evaluator in the current slice. Restoring one requires all of these inputs to be
machine-verifiable rather than caller-reported aggregates:

1. at least one explicit normalized task row with lab, query-family version, frozen time window, handle/source/turn caps,
   idempotency key, terminal state, latency, actual tool calls, and duplicate grouping;
2. an independently built golden-set manifest with two distinct reviewers, conflict state, development/blind split,
   source hashes, and stable numeric X ids only when independently verified;
3. accepted packet rows with valid `pp_x_<ULID>` ids, stable account ids, evidence refs, affiliation/relevance labels,
   adjudication status and one primary failure owner;
4. recall computed mechanically from accepted stable account ids intersected with golden stable account ids, never a
   caller-supplied `recovered_ids` list;
5. run-level incident/coverage/TTL ledgers bound by hashes, including selected packets that failed before packet
   materialization;
6. finite-number validation, exact schema/runtime parity, immutable policy digest, and a result validator that
   recomputes every KPI and decision;
7. zero-task, NaN/Infinity, fake provenance, missing crosswalk, duplicate task, policy-override, malformed ULID,
   incomplete review and guardrail-omission mutations that all fail closed.

The future evaluator consumes only opaque task/subject/packet ids and adjudication labels. It neither performs provider
calls nor writes product state. Its implementation and schemas require their own independent review before any live
Stage 2 decision uses them.
