# Generalized research orchestration and portable product boundary

> Current authority: provider-free control plane and synthetic fixture. This contract does not authorize Grok/Luna
> calls, canonical person writes, automatic cross-source identity merges, outreach, or product promotion.

## Engineering goal

The generalized lane makes a candidate's own public X Posts and Replies the default evidence surface for both:

- exploratory analysis, such as discovering plausible research directions without forcing a predefined label; and
- verification analysis, such as judging whether evidence supports a configured target direction.

The target direction is a runtime taxonomy, not a pre-training field. The same request shape can represent
pre-training, post-training, mid-training, evaluation, benchmark, infrastructure, data, coding, mathematics,
multimodal work, or a separately versioned domain taxonomy such as health, finance, and design.

The existing source-neutral pre-training lane remains a replay-compatible specialization. Its frozen artifacts and
field names are not renamed. New callers use this generalized boundary; a future compatibility adapter may project a
generic result into the legacy pre-training view without rewriting the source artifact.

## Default evidence channels

The channel registry is policy-owned and request-overridable.

| Channel | Default | Intended use | Authority |
| --- | --- | --- | --- |
| `candidate_authored_surface` | on | Candidate's own Post and Reply; exploration and verification | primary subject evidence |
| `project_direct_credit` | off | Fine-grained team, lead, author, and project-topology questions | named seed requiring candidate follow-up |
| `official_source` | off | Official lab/project anchors and named seeds | anchor, not proof of a person's work by itself |
| `conversation_graph` | off | Reply, Quote, and thread expansion | low-authority graph seed only |

An optional channel is activated only by an explicit request override. Enabling it does not upgrade its evidence
authority and does not allow candidate scoring from the seed alone.

## Two query classes for authored Post and Reply

Semantic recall and mechanical coverage are deliberately separate:

1. A semantic recall query may use the configured target aliases to find high-value evidence.
2. A coverage query proves only that a single account's Post or Reply surface was attempted.

For each resolved handle, the plan emits these OR-free coverage queries:

```text
from:<handle> -filter:replies
from:<handle> filter:replies
```

The first is mechanically classified as `authored_post`; the second as `authored_reply`. An `OR`, multiple handles,
or mixed reply filters makes a query ineligible for the coverage denominator even if it remains useful for recall.
The result records attempts independently from observations, so a zero-yield search can still be represented without
pretending an observation existed.

Every authored-surface, semantic-recall, and optional-channel call is an ordinal page in an explicit continuation
chain. Page 1 has `input_continuation_ref=null`; a successor must consume the exact prior `continuation_ref`. Failed
calls do not advance the frontier, so a retry consumes the same input continuation. A chain is complete only when its
last successful page says `exhausted`. A result with an available continuation is persistable as `partial` by marking
the subject `research_in_progress`; changing that artifact to `complete` fails validation. An in-progress subject is
only legal while every one of its accounts still holds a live frontier: at least one authored-surface or
semantic-recall chain whose tip is an unconsumed `continuation_available` page or a retryable `failed` call. An
account whose chains are all exhausted, or that has no bound attempts at all, cannot be parked as
`research_in_progress`; an unstarted planned chain is remaining work but not a bound frontier.

For `authored_surface_only`, exploratory, verification, and hybrid questions all consume the broad Post and Reply
corpus and emit no alias-search tasks. This is the default fit for selected-person research: retrieve the person's own
surface once, then use the configured taxonomy to explore or judge it. `adaptive_marginal_gain` and
`exhaustive_alias_matrix` additionally expand catalog aliases into separate quoted, handle-scoped Post and Reply
queries; those calls are recorded as `semantic_recall_attempts`. One account is retrieved once for all questions, so
the planner does not duplicate the broad Post/Reply collection per question. Exploratory questions without predefined
labels may propose new topics for later taxonomy review. Their dimension row remains `ambiguous` with no configured
matched labels; the discovered topics live only in `exploratory_findings`.

Adaptive verification has two distinct terminal paths. A positive `target_match_proven` stop is allowed as soon as
the configured `any` or `all` operator is proven by bound target evidence and all pages of the attempted queries are
exhausted. A negative low-marginal-gain stop must first cover every configured target-label x Post/Reply pair, exhaust
every attempted query chain, and carry an inline, recomputable scope-frontier audit. The audit's SHA-256 is the only
allowed `scope_frontier_audit_ref`; an arbitrary path or prose reference is not accepted.

## Fresh Research Scope Catalog

The old free-form `project_family` string is replaced at this boundary by `x.research_scope.catalog.v1`.
It supports typed nodes for:

- organization, team, and initiative;
- model and research program;
- product and application;
- capability; and
- industry.

Nodes have aliases, parent relations, status, source references, `last_verified_at`, and `refresh_after`. A campaign
can either select explicit nodes or request `complete_under_roots` for required node kinds. The latter fails closed
unless the catalog has exactly one fresh `complete` coverage assertion for every requested root/kind pair and at least
one matching node.

`complete` is bounded completeness for the named root, node kinds, evidence set, and freshness window. It is never a
claim that every model, product, application, or industry in the world is known. Before a real campaign starts, the
catalog owner must refresh stale roots and record the coverage evidence; the planner will not silently use a stale
Project Family.

## Generic analysis contract

Each question provides:

- `analysis_mode`: `exploratory | verification | hybrid`;
- human-readable question text;
- a runtime `dimension_id`; and
- zero or more target labels from that dimension.

Exploratory mode may have no target labels. Its discovered topics are emitted as `exploratory_findings` and do not
silently mutate the request taxonomy. Verification and hybrid modes require at least one target label and produce:

```text
target_core | target_adjacent | ambiguous | out_of_scope
```

These replace pre-training-specific `pretraining_core` metrics. The default precision subset is `target_core`; the
recall/follow-up pool can retain `target_core`, `target_adjacent`, and `ambiguous` according to downstream policy.

Affiliation time and target-activity time remain independent:

```text
affiliation_state: current | historical | ambiguous | unsupported
target_activity_state: current | historical | ambiguous | unsupported
```

This preserves valuable combinations such as historical target-lab affiliation plus historical target-direction
experience, or current target-lab affiliation plus only historical target-direction evidence.

## Cross-source portable ingress

`x.portable.research_campaign.request.v1` accepts four seed kinds:

```text
x_account | linkedin_profile | professional_profile | name_only
```

Each seed carries an opaque source reference, source-record SHA-256, optional bounded professional facts, and zero or
more X handle proposals. It also carries an explicit `fixture_synthetic | source_bound |
human_supplied_unverified | model_mediated_unverified` source status. An X-account seed must assert its handle. A LinkedIn/professional seed may only propose a
handle. A name-only seed cannot contain a handle and enters the handle-resolution queue.

Host namespaces are checked explicitly: an X URL cannot be placed in a LinkedIn profile field, and a LinkedIn URL
cannot be treated as an X account. Equal names never merge identities. Multiple proposed accounts remain possible.

The result artifact keeps these layers separate:

```text
source subject -> reversible link proposal -> X external account -> Post/Reply observations
```

Every input subject receives exactly one outcome. `research_in_progress` preserves a resumable account and its
attempt/observation frontier without claiming terminal analysis; the other subject states are terminal. Post/Reply
evidence belongs first to an X external account, not to a canonical person. The main product's adjudication owner must
accept a link proposal before it may materialize that evidence on a person record.

The negative resolution path is typed as first-class attempts and outcomes. A non-X seed may end
`no_verified_account` only when a bound `handle_resolution_outcome` proves an exhausted chain of real `no_match`
attempts (`search_exhausted_no_match`), and may end `failed` only when the outcome binds at least one `failed`
attempt with its error receipt (`execution_failed`). Every resolution attempt carries its own inline,
content-addressed attempt receipt mirroring query hash, execution state, truncation, and continuation frontier, so an
unexecuted or dropped resolution cannot be reported as a terminal negative. `handle_resolution_required` remains the
honest queued state: it claims no attempts and keeps the campaign `partial`.

An `x_account` seed asserts its handle at the source, so its subject may only be `analyzed` or
`research_in_progress`: the queued/negative resolution states are meaningless for it, and a terminal x-seed failure
carries no typed execution proof in this version. A failed x-seed fetch is persisted as `research_in_progress` with a
retryable failed frontier; a terminal x-seed execution outcome may be introduced by a future contract version.

A cross-source link proposal must cite account-side `handle_resolution_evidence`, even when the input already proposed
a handle. Each evidence row binds the seed and X account to an X-host profile URL, observation time, query text/hash,
observed value/content hash, and an inline content-addressed retrieval receipt. Name-only same-name matches can remain
auditable `ambiguous` proposals, but cannot become an automatic merge or analyzed subject in the same pre-resolution
plan.

Selected people from `sourcing-ai-agent` use the product-owned
`sourcing.x_first.subject_selection.v1` artifact. The X-First adapter validates the vendored schema byte digest,
selection/member hashes, row hashes, and exact subject denominator before mapping each subject one-to-one into a
portable seed. It emits `x.portable.selected_subject.request_binding.v1`, which binds the selection, portable request,
both schema digests, and every subject-to-seed row. This sidecar preserves the product selection revision without
adding product-internal fields to the generic campaign request.

## Portable result and trust preservation

`x.portable.research_campaign.result.v1` binds the complete request, plan, and catalog hashes. It contains:

- subject outcomes, including a resumable `research_in_progress` state;
- X external accounts and handle-history proposals;
- query-, content-, and receipt-bound handle-resolution evidence;
- reversible cross-source link proposals;
- optional-channel evidence, attempts, terminal outcomes, and discovery origins that require authored-surface follow-up;
- mechanically attributable Post/Reply attempts;
- separately bound semantic-recall attempts;
- account-bound Post/Reply observations;
- independent affiliation and target-dimension results;
- exploratory topic proposals;
- an optional experience-verification queue; and
- denominator-bound generic coverage metrics and limitations.

Evidence status is preserved as `fixture_synthetic`, `source_bound`, `human_supplied_unverified`, or
`model_mediated_unverified`. A dimension result cannot claim `source_bound` when any supporting observation has a
weaker status. Fixture attempts are explicitly synthetic and cannot be relabeled as unverified or presented as
Grok/X coverage. Optional evidence has its own URL, timestamp, excerpt hash, receipt and task binding; a `no_result`
outcome requires a fully exhausted chain of only `no_result` attempts and cannot conceal a failed call. An optional
channel with bound evidence, or any chain whose tip is an unconsumed continuation or a retryable failed call, is
persistable as a `research_in_progress` outcome instead of being forced into a terminal lie; it keeps the campaign
`partial` and never satisfies the `complete` or `failed` derivations.

`complete`, `partial`, and `failed` are derived states rather than producer prose. `complete` requires every input
subject to be analyzed, exhausted Post and Reply chains for every analyzed account, a valid question/account semantic
terminal path, successful terminal optional-channel outcomes, and a terminal experience row for every analyzed
account when that queue is enabled. An authored-surface question uses the explicit
`broad_authored_surface_completed` stop reason; it does not pretend that a missing alias matrix was the stopping
cause.

## Optional China/Asia experience verification

This queue is off by default and activates only when the request explicitly asks for the configured
`china_asia_professional_educational_experience` dimension. A colloquial incoming phrase such as “华人” is normalized
at the product/request layer to this professional/educational-experience question; it is not stored as an ethnicity,
nationality, or identity label.

The queue runs after the base population exists and cannot rewrite base discovery, ranking, or eligibility. It may
evaluate explicit public professional/educational evidence and professional ecosystem activity under its separate
semantic policy. It does not infer protected identity, and the result records `identity_inference_performed=false`.

## Generic quality metrics and stopping

The policy registry defines metrics in terms of the configured target direction:

| Metric | Numerator | Denominator |
| --- | --- | --- |
| authored both-surface coverage | planned accounts with exhausted Post and Reply chains | planned candidate-authored tasks |
| source-bound evidence rate | source-bound Post/Reply observations | all retained Post/Reply observations |
| target-direction core rate | `target_core` terminal verification results | terminal target-direction verification results |
| target-direction active rate | `target_core + target_adjacent` results | terminal target-direction verification results |
| evidence-backed temporal-state rate | supported temporal results with evidence | temporal results evaluated |
| marginal unique target accounts per call | unique `target_core` X accounts | all authored, semantic, and optional native calls, including failed calls |
| cross-source handle-resolution rate | non-X seeds ending with a reviewable X account proposal | non-X seeds requiring resolution |

There is no fixed business candidate, observation, answer-length, or call cap in this contract. A future live
execution policy should stop on sustained low marginal target yield plus a scope-frontier audit, with thresholds owned
by that campaign. Technical deadlines, byte limits, and provider limits remain safety ceilings rather than business
completion criteria.

`target_direction_core_rate` and `target_direction_active_rate` are owned per question/account result row, so multiple
configured verification questions do not collapse into one account denominator. The marginal-call metric is emitted
as `0/0` unless every included attempt is receipt-bound. `coverage_source_status` is likewise derived from all authored,
semantic, and optional attempts; mixed fixture provenance is rejected rather than downgraded or upgraded. Typed
handle-resolution attempts are validated under their own family with inline attempt receipts and are not folded into
the coverage-provenance summary or the marginal-call denominator in this version; the resolution family may join those
denominators in a future policy revision.

## Product integration topology

```text
sourcing-ai-agent canonical snapshot
  -> sourcing-ai-agent-owned allowlisted export adapter
  -> sourcing.x_first.subject_selection.v1 JSON artifact
  -> X-First selected-subject adapter
  -> x.portable.research_campaign.request.v1 + request binding
  -> X-First validator / planner / reviewed live runner
  -> x.portable.research_campaign.result.v1 JSON artifact
  -> sourcing-ai-agent-owned read-only import preview
  -> cross-source identity review queue
  -> canonical materialization only after product-owner adjudication
```

Neither project imports the other's Python runtime. Each side owns its adapter and pins an identical schema digest in
CI. The X-First result is an evidence-source artifact, not a product writer.

## Owner and source-of-truth matrix

| Concern | Owner/source of truth | Fail-closed condition |
| --- | --- | --- |
| Channel defaults and generic relevance/KPIs | `research_orchestration_policy.v1.json` | unknown channel/state/metric |
| Project Family nodes and freshness | `x.research_scope.catalog.v1` producer | stale/unknown node or missing bounded-complete assertion |
| Campaign intent and cross-source seeds | portable request producer | bad hash, source host confusion, ambiguous field shape |
| Deterministic task resolution | `research_orchestration.py` + portable plan schema | plan differs from recomputation |
| External-account observations and semantic outcomes | portable result producer plus receipts | bad bindings, missing terminal outcome, trust upgrade |
| Handle-resolution evidence and typed negative outcomes | content-addressed retrieval/attempt receipts plus downstream identity owner | wrong X host/handle, query/content/hash drift, missing account-side evidence, or a terminal `no_verified_account`/`failed` without an exhausted `no_match` chain or a bound `failed` attempt |
| Optional-channel execution and discovered seeds | optional task outcomes + discovery origins | missing outcome, unbound origin, or skipped required follow-up |
| Source subject to X account link | downstream identity adjudication owner | automatic merge or missing human review |
| Canonical person/evidence materialization | `sourcing-ai-agent` product owners | direct X-First write/import |
| Grok/Luna execution, cost, retention, and recovery | future reviewed live-runner policy | no matching review/grant/receipt |

## Current validation boundary

The checked campaign fixture includes one asserted X account, one LinkedIn-origin proposed handle, and one name-only
seed whose terminal `no_verified_account` is proven by an exhausted two-page `no_match` resolution chain with inline
attempt receipts. The LinkedIn-origin proposal cites a synthetic but fully content-addressed account-side resolution
receipt. A separate selected-subject regression builds a source-bound LinkedIn seed and a source-bound name-only
seed from the product-owned selection artifact, uses verification with `authored_surface_only`, and verifies the exact
request binding. All checked campaign attempts and observations remain `fixture_synthetic`; no provider or product call
occurs. The orchestration registry covers both selected-subject schemas in addition to policy, catalog, request, plan,
and result schemas, plus the package manifest and semantic validation receipt schemas; the checked selected-subject
package fixture is rebuilt and compared byte-for-byte on every preflight so validator, schema, or fixture drift fails
fast instead of surfacing in a later live review. Accepted contract residuals and their expiry conditions live in
`docs/RESIDUAL_LEDGER.md`; the product-lane sync payload for this batch lives in
`docs/PRODUCT_INTEGRATION_SYNC.md`.

Run:

```bash
PYTHONPATH=src python3 -m x_first.research_orchestration
PYTHONPATH=src python3 -m unittest tests.test_research_orchestration tests.test_selected_subject_adapter tests.test_portable_campaign_package -v
```
