# X-first workflow evaluation contract

> Status: Design methodology plus executable offline exploration and recall-campaign evaluators. Seven live
> exploration waves supply author evidence only; thresholds below remain proposed feasibility gates, not production
> claims or permission to promote.

## Decision this framework supports

The evaluator answers whether an X-first researcher-discovery workflow is accurate, coverage-positive, operationally
efficient, and safe enough for another bounded owner-reviewed experiment. It does not decide canonical identity,
employment, promotion, outreach, or product integration.

The evaluation grain is fixed as `lab + strategy/query family + frozen time window + request/prompt/model receipt`.
Operational deadline and emergency resource ceilings remain explicit, but candidate/observation/call totals are not
business success caps. Splitting one task into smaller requests cannot improve yield. Every selected packet stays in
the precision denominator until adjudicated; unresolved or incomplete packets cannot disappear from metrics.

## Precision delivery and experience-recall views

Let:

- `Scc` be every packet selected for the business-facing current-lab/current-pretraining precision tranche;
- `Acc` be the unique evidence-complete packets confirmed `target_lab=current` and `pretraining=current`, with a
  stable X platform user id;
- `Lexp` be every model-mediated lead assigned to one of the four supported current/historical temporal combinations;
- `Eexp` be the unique evidence-complete packets in any of the four supported current/historical lab × current/
  historical pretraining combinations;
- `Gcc` and `Gexp` be independently labeled golden populations for those two views;
- `Gcc_x` and `Gexp_x` be their independently verified public-X subsets;
- `T` be completed normalized tasks.

| KPI | Formula | Why it changes a decision |
| --- | --- | --- |
| Current/current evidence-qualified precision | `|Acc| / |Scc|` | Measures the narrow business delivery without mixing in experience-only segments |
| Current/current end-to-end recall | `|Acc intersect Gcc| / |Gcc|` | Measures coverage of the high-precision target |
| Experience end-to-end recall | `|Eexp intersect Gexp| / |Gexp|` | Prevents current/historical, historical/current, and historical/historical researchers from disappearing |
| Qualified experience yield | `100 * |Eexp| / T` | Measures useful discovery at a stable operational grain |

`Lexp` is a Recall-oriented discovery queue, not an accepted packet set. A lead can therefore belong to a configured
temporal segment while still requiring stable-account, Bio, or high-authority evidence hydration. Likewise,
`precision_current_current` is a value segment name, not permission to deliver that row: a packet enters `Scc`/`Acc`
only after the precision completeness rule passes. This separation prevents both common errors: dropping valuable
historical experience too early and presenting incomplete current/current leads as Precision output.

Each view decomposes recall into X-account availability and conditional search recall. The future executable evaluator
must report Wilson 95% intervals for precision and both conditional-recall views; small samples cannot be presented as
precise production estimates. Every selected or hydration-needed packet remains visible in a denominator or segment.

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

### Decision KPI hierarchy

Keep the operating review focused on three primary KPIs:

| Primary KPI | Decision | Required source of truth |
| --- | --- | --- |
| Evidence-qualified experience conditional recall | whether another discovery strategy materially improves coverage | independent `Gexp_x` plus source-bound `Eexp`; model leads cannot enter numerator |
| Current/current evidence-qualified precision | whether the narrow tranche is safe to present for business review | human-adjudicated `Acc/Scc` with stable account identity |
| Marginal qualified unique yield per completed native-X call | whether to continue discovery or move capacity to hydration | new source-bound accepted accounts divided by raw-session-replayed completed calls, segmented by distinct strategy |

Each primary KPI has actionable drivers:

- recall: strategy/query-family coverage, X-account availability, and newly hydrated evidence-complete accounts;
- precision: stable-ID coverage, independent lab/pretraining evidence coverage, and adjudication disagreement;
- efficiency: raw new-unique/call, qualified new-unique/call, latency, provider cost, and review minutes per accepted
  account.

Guardrails remain zero-tolerance for false merges, protected-identity inference, provider fallback, unbound evidence,
product/outreach writes, and deadline/retention violations. Raw lead count is a capacity diagnostic, not a success KPI:
it is easy to inflate by weakening evidence.

### Metric ownership and denominator trust

| Layer | May own | Must not own |
| --- | --- | --- |
| Grok/model self-report | diagnostic observations, claimed calls/queries, proposed labels | mechanical calls, unique population, precision, recall, source completeness |
| Sanitized JSON parser | candidate/evidence row counts, casefold handle novelty, missing-field rates | provider terminal state, source truth, accepted identity/employment |
| Raw-session replay | completed native-X call lifecycle, tool distribution, CLI terminal and hashes | provider Post-body replayability or provider terminal usage when absent |
| Hydration + human adjudication | stable account, source-bound evidence completeness, accepted segment | independent golden-population denominator |
| Frozen gold owner | `Gcc/Gexp` and public-X subsets | provider-discovered relabeling after the experiment |

The first fixed targets remain provisional feasibility gates: `>=80%` current/current precision and `>=50%`
conditional recall in each gold view. No universal discovery-stop threshold is set from one lab. Each experiment
versions a non-enforcing call-normalized plateau advisory and requires at least two materially different recent
strategies plus an explicit coverage audit. A candidate-count threshold can never trigger stopping.

## Hard guardrails

Any nonzero violation makes the run `no_go` regardless of KPI values:

- protected-identity inference or protected-identity proxy query/ranking/label use;
- false identity merge, cross-account evidence, or handle-only automatic merge;
- generic web, search-engine, Apify, or other provider fallback;
- unverified provenance/request hash/stable object id/canonical URL;
- canonical PersonAsset/evidence/assertion, CRM, projection, export, outreach, or other product write;
- budget/deadline overrun or retry that creates a duplicate external call;
- full-body retention or unverified TTL deletion;
- incomplete terminal artifact or coverage ledger;
- exact duplicate normalized task; near-duplicate rate above 10%.

Discovery and ranking population is only target-lab affiliation (current or historical), professional role/function,
and pretraining experience/relevance (current or historical). Name, language, region, school, community, biography,
post text, mentions, or graph position cannot infer or proxy ethnicity, nationality, race, citizenship, religion,
gender, or another protected identity. The stricter sibling rule controls even where a source project's general query
guardrail permits region/language as an explicit outreach skill.

This does not ban the distinct professional-experience proxy contract. After the base population is established, a
separately versioned and governed verification-queue policy may consume `strong_proxy|weak_proxy` China/Asia
professional-experience proposals with their source refs and unverified status. It cannot alter the base population,
be relabelled as physical-region evidence or identity, or independently decide final eligibility/ranking/outreach.

## 2026-07-14 exploration baseline

The Grok CLI diagnostic recomputed one real session from raw hashes and call events:

- `8/8` completed native-X calls reconciled;
- eight retained model-mediated leads from 68 model-reported observations;
- model-mediated Bio presence `62.5%`, model-mediated platform-user-ID presence `12.5%`;
- model-mediated high-authority support coverage `25%`, third-party-only leads `25%`;
- provider Post-body replayability `0%`;
- raw Grok session permissions were not owner-only.

After explicitly migrating the legacy model result into the two-axis contract, the diagnostic mechanically reported:

- `precision_current_current=2` lead segments;
- `recall_current_historical=1`;
- `recall_historical_current=0`;
- `recall_historical_historical=1`;
- `needs_evidence=4`;
- four of eight leads in the experience-Recall pool, but zero evidence-qualified Precision packets because the
  current/current leads still lacked required stable platform IDs.

The legacy result remains immutable and hash-bound; the normalized result and migration receipt are separate private
artifacts. These counts demonstrate the intended denominator preservation, not accuracy or recall against a golden
population.

This is capability and first-field-gap evidence only. It supplies neither a golden set nor role/function labels,
adjudicated selected packets, conditional recall, evidence-qualified precision, cost, or a scale-ready task ledger.

### Adaptive recall campaign

A later seven-wave campaign deliberately removed candidate/observation/call success ceilings and diversified search
strategy. It produced `99` rows / `98` unique handles from `702` mechanically observed native-X calls. New unique
yield by wave was `29/12/6/20/25/5/1`; the rebound in waves 4–5 proves that one declining wave is not a valid stop
signal. The final current-team and residual-coverage strategies fell to `0.048` and `0.014` new unique handles per
raw call, which justified an operator pause to move capacity to hydration. It is not a formal exhaustion decision:
raw-session replay returns `insufficient_proof / continue_expansion` because complete non-system user context,
legacy prior-exclusion membership, versioned strategy definitions, and precommitted query-family attribution were not
machine-bound before execution. The private v5 merged replay binds all seven emitted assistant terminal JSON objects,
system prompts, exact terminal byte/chunk/update locations, and terminal-start-after-tools ordering while keeping every
candidate field `model_mediated_unverified`. It also binds every evidence association to the enclosing candidate,
revalidates persisted URL-author/status-id pairs, preserves native-X thread evidence, rejects generic-web provenance
and validates real UTC calendar instants; the replayed artifact is hash-stable at
`4f27d046c8618e424c1f24b7dcf4978b71799284d03e75d77054dc4ab6a0d19a`.

After handle merge, model labels contain `13 current/current`, `5 current/historical`, `1 historical/current`,
`18 historical/historical`, `1` historical/conflicting row, and `60` rows with at least one ambiguous/unsupported
dimension. These are recall leads, not accepted packets. Exact evidence and stable account identity remain unverified. See
`docs/live-evidence/2026-07-14-openai-pretrain-recall-campaign.md`.

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
target_lab_affiliation_state    current|historical|ambiguous|unsupported
pretraining_experience_state    current|historical|ambiguous|unsupported
candidate_value_segment         precision_current_current|recall_current_historical|
                                recall_historical_current|recall_historical_historical|needs_evidence
x_presence                      verified_public|no_verified_account|unknown
platform_user_id                only when independently verified
handle_history
affiliation_evidence_refs
relevance_evidence_refs
x_identity_evidence_refs
label_status                    double_reviewed|conflict
split                           development|blind_holdout
```

Former employees are not automatically negative. Historical target-lab affiliation with supported current or historical
pretraining experience belongs to the experience-recall population. Hard negatives instead include unsupported target-
lab affiliation or unsupported pretraining experience, parody/aggregator accounts, common-name conflicts, false
identity joins, and rows whose claimed experience lacks source support. Ambiguous rows remain in `needs_evidence`.
The first directional lab set should contain 12-20 positives and 8-12 hard negatives. Method-level scale claims need
at least 30 blind positives, 50 reviewed selected packets, two labs, and three frozen windows.

## Proposed feasibility and scale gates

The first Stage 2 feasibility policy should require:

- at least five reviewed and five accepted unique accounts;
- current/current evidence-qualified precision >=80%;
- current/current conditional search recall >=50%;
- experience-recall results reported separately for all four current/historical combinations;
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

`grok_cli_exploration.py` is an executable exploration diagnostic, not this Stage 2 KPI evaluator. It can hash-verify
a raw session, reconcile tool calls/queries, validate model-mediated lead structure, compute field gaps, and emit
non-executable hydration diagnostics. Its persisted diagnostic and hydration task are closed by
`contracts/x.grok_cli.exploration.evaluation.v1.schema.json` and
`contracts/x.grok_cli.candidate_hydration.task.v1.schema.json`; runtime validation additionally recomputes task keys,
parent/child experiment bindings, temporal segment mappings, counts, rates, gaps, proof state, feasibility, blockers,
identity quarantines, and authority before the CLI can write the private artifact. The public persisted-output
validator is source-required:
it accepts the sanitized result and tool receipt, requires the raw session directory whenever the artifact claims
session verification, re-runs the evaluator against the immutable public keyed-commitment descriptor plus the private
receipt's exact ordered call preimages, and requires canonical output equality. A detached artifact cannot validate
itself.

Query-policy v2 generates a fresh random 256-bit HMAC key and independent 256-bit nonce for one private run. Only the
owner-only `0600` tool receipt retains `key_hex`, `nonce_hex`, raw run identifiers, and query arguments. The intended-
public descriptor/registry, evaluation, and hydration tasks retain only `key_id`, `nonce_id`, a deterministic opaque
`commitment_issuance_id`, the domain-separated HMAC commitments, and descriptor/registry/issuance-row hashes. Every
immutable version-named registry snapshot carries an append-only issuance lineage. Runtime rejects reuse of either a
key id or nonce id by any other run, even if the lab or policy version differs; a new lab therefore requires fresh key
and nonce material. The key and nonce live exactly as long as the private receipt: they are
needed for offline source replay, are never copied into tracked files or terminal summaries, and are deleted with that
receipt under the run's private retention/purge policy. A replay after deletion is intentionally impossible. Key/nonce
reuse across runs is forbidden. `scripts/migrate_grok_cli_query_commitments_v2.py` is the bounded one-time migration,
replay, and explicit source-purge entrypoint. Its supplied private root must be a non-symlink owner-UID `0700`
directory; private ancestors and regular files are descriptor checked with no-follow semantics, `0600`, and single-link
ownership. Prepare/evaluate/purge share one nonblocking filesystem lock. Each atomic file write fsyncs the file and
containing directory. A durable private receipt binds canonical source-to-target hashes, issuance ids, retention/delete
owner, state, and idempotency; partial, stale, linked, or mismatched destinations fail closed. Only a byte-identical
validated rerun is reported as idempotent. Purge first persists an intent, deletes the three legacy sources with
directory fsync, then writes a deletion tombstone so an interrupted delete can resume without treating partial state as
success.

The former v1 public descriptor used unsalted low-entropy hashes. Those v1 config/schema files are removed from HEAD
and disabled as evaluator defaults. Their historical Git objects may still reveal dictionary-verifiable labels; v2
rotation prevents correlation to the new commitments but cannot retroactively erase repository history. Deletion
condition: after every retained evaluation has a source-replayed v2 replacement and its private v1 receipt reaches its
retention deadline, delete the old private receipt/full-policy pair; do not delete the v2 receipt before its last
required replay.

There is no candidate-count, observation-count, hydration-task-count, or total-call business cap. Runtime work is
bounded only by a 64 MiB canonical-input ceiling, one million JSON nodes, depth 64, one absolute 30-second monotonic
evaluation deadline passed through registry validation, hydration construction, task validation, final shape validation,
and source replay, plus provider-specific per-call argument bounds. Nested helpers cannot restart that budget. Every rate
publishes its exact denominator. These technical
ceilings fail closed and are not stop rules for discovery.

Each hydration task is `planned` and binds the parent candidate SHA-256, both temporal states, derived segment, and
identity-counting status. Every hydration-needed candidate receives a task; ordering does not truncate the list.
Equal-priority mixed temporal segments are ordered round-robin, and confidence orders candidates only within one
segment. Restoring a promotion-grade evaluator still requires all of these inputs to be
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

## Configured candidate-value owner

The exploration diagnostic keeps its candidate-value semantics in the reviewed
`configs/candidate_value_segment_policy.v1.json` policy, not in lab-specific algorithm branches. That policy owns the
state closed set, four complete temporal combinations, segment priority, Recall eligibility, Precision completeness,
and hydration triggers. Its executable schema requires four unique closed rows, while runtime validation proves the
exact state-pair/segment bijection. Query-policy registries are immutable version-named snapshots under
`configs/grok_cli_exploration_query_policy_registries/`; each public snapshot binds an approved experiment through an
opaque run commitment, append-only issuance row, descriptor hash, legacy full-policy commitment, and exact ordered call commitments without
publishing query operands. These are domain-separated HMAC values under the private run key and nonce, not unsalted
operand hashes. Adding another lab creates a new reviewed descriptor snapshot instead of editing the snapshot named by an
existing evaluation, so historical artifacts remain replayable. It must not require adding a new candidate field or
changing segment code.

Candidate inclusion/exclusion and caveats are closed reason-code fields. Free-form text is permitted only as bounded
source evidence. Descriptor and registry rows bind `protected_category_boundary_version`; the current reviewed value is
`base-discovery-protected-category-boundary-v1`. That versioned, code-governed protected-category/value boundary scans each exact query/supporting span;
possible protected targeting or claims such as American, Indian, Muslim, 华人, or multilingual equivalents cannot
support a base lab/pretraining axis. This is a data-driven phrase registry, not an expanding identity regex. China/
Asia professional-experience proxy interpretation remains a separate governed Bio-semantic lane and cannot modify
base discovery, exclusion, Recall, Precision, or ordering decisions here. If the same non-null reported numeric X ID
appears under multiple handles, every involved row is quarantined, excluded from unique/Recall/Precision counts, given
`reported_platform_user_id_conflict` hydration, and retained in raw candidate-row denominators for auditability.

Confidence is only a within-segment ordering tie-break. It cannot move a lead into another segment, manufacture a
current/historical state, or remove an evidence gap. The two mixed Recall segments intentionally have equal priority;
historical/historical is retained at the next configured priority rather than treated as a negative.
