# Track D D3c2h0 — Evidence cross-contract ratification

> Status: **decision lock only; no schema manifest, SQL, migration, descriptor, repository, runtime writer, transport,
> provider/model call, or served Agent path.** This batch resolves only the cross-contract contradictions that must be
> fixed before D3c2h1 may ratify exact `verification_intent`, transport-receipt, and late-quarantine manifests.

## 1. Outcome and bounded impact

D3b correctly separated command authorization, transport evidence, cost exposure, and late-result quarantine, but its
receipt/quarantine sketches predate the exact five-field scope prefix ratified by D3c2g and contain three contradictions:

1. Migration-C keys and occurrence/idempotency examples are scoped only by `scope_digest` instead of the complete
   strict-D3 physical prefix;
2. response/failure receipts omit the post-claim `command_attempt`, while the exposure and ActivityAttempt already bind
   that identity;
3. response-only quarantine includes an unowned `workflow_run_id` and permits a no-call cost terminal even though a
   valid response receipt proves that a physical call occurred;
4. the evidence-only post-network sketch starts at exposure yet optionally inserts quarantine, even though an
   exposure-first UoW cannot authoritatively classify current versus stale command/business state.

D3c2h0 ratifies the corrected relationships, owner boundaries, mode applicability, and post-network transaction order.
It deliberately does **not** choose ordered columns, column totals, SQL types/nullability/defaults, constraint names or
counts, indexes/FKs, exact repository class/module names for the still-future evidence owners, CAS signatures,
idempotency encoding bytes, or retention deadlines. Those belong to D3c2h1 after the D0f durable-envelope prerequisite.

The following block is the machine-readable cross-contract authority for this batch:

```text
CROSS_CONTRACT_RATIFICATION_V1
scope_prefix = runtime_namespace | provider_mode | workspace_id | scope_digest | coordination_plan_review_id
receipt_attempt_source = dispatch_exposures.command_attempt
receipt_attempt_equality = workflow_activity_attempts.command_attempt | ClaimReceipt.identity.attempt | ClaimIdentity.attempt | workflow_commands.attempt
quarantine_forbidden_identity = workflow_run_id
quarantine_cost_transition = pending_reconciliation -> reconciled_confirmed | reconciled_uncertain
quarantine_forbidden_cost_state = reconciled_no_call
durable_envelope_prerequisite = D0f
canonical_envelope_schema_owner = ModelInvocationEnvelopeV1
model_transport_kind = model_tool_v1
eligible_provider_modes = live | simulate | scripted
replay_behavior = fail_closed_zero_write
provider_search_behavior = owner_ratified_transport_variant_required
quarantine_classifier_authority = stored_current_state_under_d3_dispatch_v2_global_lock_prefix
response_classification_order = d3_dispatch_v2 -> operation_root -> optional_plan_review_gate -> participating_commands_sorted -> intent_predecessor -> activity_run_attempt -> dispatch_exposure_lock -> transport_response_receipt -> optional_response_quarantine -> dispatch_exposure_terminalization
exposure_first_evidence_order = dispatch_exposure_lock -> transport_evidence_receipt -> dispatch_exposure_terminalization
exposure_first_quarantine_permission = forbidden
classification_retry_contract = deferred_to_D3c2h1
schema_manifest_status = deferred_to_D3c2h1
```

No declaration above claims that the future rows or owners are implemented.

## 2. Supersession ledger

These are explicit fixed-forward decisions. They supersede only the named D3b sketches; they do not rewrite historical
review evidence or silently close any implementation obligation.

| ID | superseded sketch | ratified replacement | reason |
|---|---|---|---|
| H0-1 | receipt/quarantine keys and occurrence/idempotency examples whose tenant scope is only `scope_digest` | every strict-D3 evidence identity, unique/FK relation, idempotency scope, exact-replay comparison, and CAS uses the full five-field PFX | `scope_digest` is derived identity, not a substitute for physical namespace, mode, workspace, and positive review lineage |
| H0-2 | response/failure receipt field sketches without `command_attempt` | both receipt families exact-copy the exposure's immutable post-claim `command_attempt`, and exact replay compares it | generation and epoch do not replace retry-attempt identity |
| H0-3 | late quarantine carrying `workflow_run_id` | `workflow_run_id` is forbidden on quarantine; no post-network path may synthesize or look it up | neither the exposure nor receipt owns that identity, and post-network evidence may not return to workflow/command rows |
| H0-4 | response-only quarantine cost axis admitting `reconciled_no_call` | the only cost transitions are `pending_reconciliation -> reconciled_confirmed` or `pending_reconciliation -> reconciled_uncertain` | a valid response receipt is positive call evidence; no-call has no response receipt and no quarantine row |
| H0-5 | receipt sketches containing a durable envelope ref without an issuer | D0f must ratify and implement the sole durable envelope reference owner before receipt manifests or runtime | placeholders and a second envelope schema are forbidden |
| H0-6 | post-network prose that does not close the final exposure mutation order | every path uses the closed exposure tail: lock exposure, get-or-create the applicable receipt, optionally insert response quarantine only when H0-13 authorizes it, then terminalize the same exposure | the closed tail removes partial evidence and exposure/receipt disagreement |
| H0-7 | transport applicability inferred from generic D3 prose | initial v1 is `model_tool_v1` only; Harvest/provider-search needs a separately owner-ratified transport variant | model-only pins cannot be blanked or fabricated for a different transport |
| H0-8 | treating zero cost as absence of physical evidence | strict-D3 simulate/scripted keep zero-money exposure and normal response-receipt reachability | mode isolation and deterministic E2E need the same durable identity chain as live |
| H0-9 | treating global-D3 replay grammar as current model-envelope support | replay is zero-write fail-closed in this v1 | current `ModelInvocationEnvelopeV1` admits only live/simulate/scripted and D3c2g has no replay pricing/schema path |
| H0-10 | implementing immutable rows through the current generic descriptor | exact Decimal/TIMESTAMPTZ codecs and specialized insert-once/exact-replay/CAS primitives are prerequisites | the current generic descriptor lacks those codecs and defaults to replace-all upsert |
| H0-11 | inferring `verification_intent` or terminal workflow writes from post-network evidence | the company-identity verification owner remains the only intent writer, and terminal workflow apply is a later current-fence UoW consuming committed evidence | evidence persistence is not domain authorization |
| H0-12 | deriving an exact schema from D3b narrative lists | all exact intent/receipt/quarantine manifests remain D3c2h1 work | D3c2h0 is a cross-contract decision, not schema design by prose |
| H0-13 | D3b's exposure-first evidence-only sketch optionally inserting quarantine | only a response-classification UoW that first takes `d3-dispatch-v2` and the complete global owner-row lock prefix may derive current/stale from stored state and insert optional quarantine; a pure exposure-first UoW has zero quarantine permission | caller flags, stale ClaimReceipt, or an exposure alone cannot prove current/stale; D3c2h1 must ratify pending-classification retry/idempotency |

## 3. Owner boundaries

| object / composition role | single owner | physical source of truth | allowed use | forbidden use | next bounded owner work |
|---|---|---|---|---|---|
| canonical envelope shape and digest | current `model_tool_runtime.py::ModelInvocationEnvelopeV1` | the existing immutable in-memory canonical record and digest | validate one terminal model response without changing the schema | a second envelope class/schema, caller digest trust, placeholder refs, or receipt-owned envelope issuance | D0f reuses this exact schema and does not redefine it |
| durable envelope record and owner-issued ref | future D0f durable result-slot/evidence owner, exactly one | future immutable PFX-bound persistence identity plus exact canonical envelope digest | issue/persist one real ref, enforce exact replay/collision and retention, and support strict-D3 live/simulate/scripted | receipt/quarantine/cost owners minting refs; fake URIs/hashes; unscoped or cross-mode reuse | D0f ratifies physical owner, reference grammar, issuance UoW, retention, and presence rules |
| dispatch exposure | D3c2g `CostLedgerRepository` through `store.repos.cost_ledger` | committed `dispatch_exposures` row | pre-network authorization, post-network lock, and exposure terminalization through typed owner calls | receipt/quarantine writers issuing or directly mutating exposure SQL | later cost-ledger implementation after storage prerequisites |
| response and attempt-failure receipts | future transport-evidence repository, exactly one | future immutable receipt rows | exact-copy a committed exposure/PFX/attempt and owner-issued D0f envelope evidence where applicable | command/domain authorization, envelope issuance, exposure SQL, quarantine SQL, or cross-mode replay | D3c2h1 ratifies exact owner name, manifests, relations, and insert/exact-replay CAS |
| late response quarantine | future shared quarantine repository, exactly one | future immutable response identity plus orthogonal cost/retention axes | non-authorizable response-only audit, typed cost CAS, and typed retention CAS | attempt-failure rows, no-call, reducer/domain/public/model consumption, identity rebind, or exposure SQL | D3c2h1 ratifies exact manifest, identity relations, CAS, and retention contract |
| verification intent | company-identity verification owner | future `verification_intent` row | exact-copy the source ActivityAttempt/command identity and later consume committed terminal evidence under the current business fence | post-network transaction writing intent/domain state; generic workflow control writing the row | D3c2h1 ratifies its exact manifest and source/terminal tuple CAS |
| post-network transaction coordinator | future composition seam only; it owns order and authoritative response classification, not another table | response-classification UoW under `d3-dispatch-v2` plus complete global owner-row lock prefix, or a quarantine-ineligible exposure-first evidence UoW | classification path derives current/stale from locked stored state before the exposure tail; evidence-only path persists receipt/cost without quarantine | caller/stale-ClaimReceipt classification, direct cross-owner SQL, holding a transaction over network, or returning to earlier rows after exposure | D3c2h1 names both composition APIs plus pending-classification retry/idempotency after all physical owners are ratified |

The owner split is intentional: the transaction coordinator can enforce one atomic order without becoming a second SQL
writer. Each repository retains exclusive mutation authority for its own rows and participates through the same PG
transaction. Only the classification-capable coordinator may pass a transaction-local, non-serializable stored-state
classification to the quarantine owner. A generic Store facade, reducer, callback, transport helper, or read path may
not bypass those typed owners or mint a classification capability.

## 4. Full PFX and post-claim attempt identity

`PFX` means exactly this ordered physical prefix and is only documentation shorthand:

```text
(runtime_namespace, provider_mode, workspace_id, scope_digest, coordination_plan_review_id)
```

All five values must be present, canonical, and exact-equal across the strict-D3 operation, command, ActivityAttempt,
exposure, response/failure receipt, quarantine, verification intent, and D0f durable envelope reference relation.
`coordination_plan_review_id` is a positive BIGINT lineage. Every future primary/unique/FK relation, idempotency scope,
occurrence identity, exact-replay equality, and mutating predicate for these evidence surfaces begins with or otherwise
physically compares the complete PFX. A scope-digest-only key is not tenant/mode authorization, even when digest input
was originally derived from the same tuple.

D3c2h1 must therefore replace the old scope-digest-only receipt and quarantine key sketches with full-PFX relations.
This batch does not choose the final length-delimited encoder, identifier text, or SQL key shape; it only makes omission
of any PFX component invalid.

Both future receipt families also carry an immutable `command_attempt` copied from the committed exposure, never from a
late read of mutable command state. The equality chain is:

```text
transport receipt command_attempt
= dispatch_exposures.command_attempt
= workflow_activity_attempts.command_attempt
= ClaimReceipt.identity.attempt
= ClaimIdentity.attempt
= workflow_commands.attempt at the winning Stage-A claim
```

Receipt insert/exact replay compares the complete chain already frozen into its parent rows. Stale evidence remains
auditable because the receipt uses the exposure copy; it does not need a current command token or permission to walk
back to command state. D3c2e's event decision remains unchanged: WorkflowEvent does not add an event-side
`source_command_attempt` alias and continues to use the linked ActivityAttempt.

## 5. Quarantine reachability and post-network transaction order

Late quarantine is reachable only from a valid `TransportResponseReceipt`. An attempt-failure receipt is cost/audit
evidence only and never creates quarantine. Proven no-call has neither response receipt nor attempt-failure receipt and
never creates quarantine. Consequently:

```text
cost_state: pending_reconciliation -> reconciled_confirmed | reconciled_uncertain
retention_state: retained -> purged_tombstone
```

The axes remain orthogonal and monotonic. Cost reconciliation cannot extend retention; retention purge cannot erase
immutable identity/digests required for audit or conservative accounting. `reconciled_no_call` is explicitly forbidden
for quarantine. No-call remains a cost-ledger exposure terminal backed by no-call proof, with no receipt/quarantine row.

`workflow_run_id` is also forbidden on quarantine. The post-network row may only copy identity already owned by the
exposure/receipt relation selected by D3c2h1; it may not synthesize a workflow identity or query workflow rows to fill a
nullable/placeholder value.

There are exactly two post-network compositions.

**A. Pure exposure-first evidence-only UoW.** This path may persist authenticated response or attempt-failure evidence
and terminalize its exposure, but it has zero quarantine authority:

```text
dispatch_exposure_lock
-> applicable_transport_evidence_receipt_insert_or_exact_replay
-> dispatch_exposure_terminalization_or_exact_replay
-> commit
```

It does not lock OperationRun, plan/review/gate, workflow command, intent, or Activity rows and therefore cannot classify
current versus stale. A caller flag, callback label, stale `ClaimReceipt`, serialized capability, exposure field, or
receipt field cannot upgrade it. For a response that still needs classification, D3c2h1 must ratify durable pending
classification ownership, retry/recovery, and idempotency so a committed receipt cannot be stranded or silently treated
as current/stale. Until that contract exists, no exposure-first response path may insert quarantine.

**B. Response-classification UoW.** Any transaction that may insert quarantine first acquires the common coordination
lock and complete global owner-row prefix, then derives current/stale only from locked stored state:

```text
d3_dispatch_v2
-> operation_root
-> optional_plan_review_gate
-> participating_commands_sorted
-> intent_predecessor
-> activity_run_attempt
-> dispatch_exposure_lock
-> transport_response_receipt_insert_or_exact_replay
-> optional_response_only_quarantine_insert_or_exact_replay
-> dispatch_exposure_terminalization_or_exact_replay
-> commit
```

The global prefix is read-only classification: it may lock and exact-compare the operation, plan/review/gate, every
participating command, intent/predecessor, and ActivityRun/Attempt, but writes none of them. It compares the exposure's
immutable PFX/claim/attempt/epoch/business pins to those authoritative rows. Only a transaction-local, server-derived
stale classification permits the optional quarantine call; current evidence omits quarantine and may later be consumed
by the normal terminal workflow UoW. Missing rows, lock-budget exhaustion, or an unprovable classification fail closed
without quarantine and enter the D3c2h1 pending-classification contract, never caller fallback.

After either path enters `dispatch_exposure_lock`, it cannot return to any earlier aggregate. The first and final
exposure operations are typed `CostLedgerRepository` calls on the same locked row; receipt and optional quarantine use
their own typed repositories and the same transaction. Any collision, PFX/attempt mismatch, invalid transition, or final
terminalization failure rolls back every mutation in that UoW. Neither path locks `cost_reservations`; settlement remains
the separate D3c2g parent-first flow. Neither path writes a workflow event, EntityDelta, artifact publication, child
command, intent, or domain state, and neither authorizes send/retry/apply. No PG transaction spans DNS, connect, request
bytes, response streaming, provider polling, or any network I/O.

## 6. Mode and transport applicability

The initial evidence chain is closed to `transport_kind=model_tool_v1` and exactly three provider modes:

| mode / transport | durable exposure | valid-response receipt reachable | money semantics | v1 decision |
|---|---|---|---|---|
| `live` + `model_tool_v1` | required before send | yes, after D0f owner-issued envelope persistence | positive reservation/worst case under D3c2g | eligible only after all later gates and implementations |
| `simulate` + `model_tool_v1` | required in strict D3 | yes, through the same receipt identity chain | every reservation/exposure/accounting amount exactly zero | eligible for future scripted/simulate verification after D0f/D3c2h implementation |
| `scripted` + `model_tool_v1` | required in strict D3 | yes, through the same receipt identity chain | every reservation/exposure/accounting amount exactly zero | eligible for future deterministic E2E after D0f/D3c2h implementation |
| `replay` | no v1 row | no v1 receipt or quarantine | no v1 pricing/schema path | fail closed with zero writes until a D0 envelope/schema and owner decision add support |
| Harvest/provider-search | cannot reuse the model variant | not through `model_tool_v1` | must have its own ratified pricing/accounting relation | blocked pending a separately owner-ratified transport variant; no identifier is guessed here |

Zero cost is not zero evidence: strict-D3 simulate/scripted valid responses retain the same PFX, exposure,
post-claim-attempt, D0f envelope ref/digest, receipt, and optional quarantine relationships as live. Conversely, a replay
fixture cannot borrow a scripted row or receipt, and no non-live evidence may exact-replay into live.

HarvestAPI/provider-search transports do not truthfully own model-only fields such as requested/effective model identity
or model-safe schema revision. They may not write blanks, fake model values, or pretend to be `model_tool_v1`. Before a
Thinking Machines Lab live canary can traverse this Track-D evidence path, a later owner decision must ratify that
transport's variant, request/call/result identity, pricing, terminal provenance, receipt relationship, and mode isolation.

## 7. D0f and storage prerequisites

Current `ModelInvocationEnvelopeV1` is the canonical immutable **in-memory** schema. It intentionally has no durable
`model_invocation_envelope_ref` issuer. D3c2h0 names **D0f** as a hard predecessor; it does not implement or pre-empt it.
D0f must ratify and implement all of the following before D3c2h1 can finalize receipt envelope-ref fields:

- exactly one durable result-slot/evidence owner and no second envelope schema;
- an owner-issued reference grammar bound to the complete PFX and canonical envelope digest;
- one issuance/persistence UoW, insert-once exact replay/collision behavior, and tenant/mode isolation;
- retention/tombstone and ref/digest lookup rules, including historical spec retention;
- strict live/simulate/scripted presence rules and explicit absence outside the admitted durable path;
- no placeholder ref/hash, raw wire payload, secret, credential, or caller/model-authored provenance.

Receipt or quarantine design may cite a D0f owner-issued ref only after those rules exist. It may not invent a URI, hash,
row id, storage path, or wrapper to make a future field non-null. `ModelInvocationEnvelopeV1` remains the sole canonical
shape/digest owner; D0f owns durable issuance and persistence, not a competing shape.

Two generic storage gaps are also implementation prerequisites:

1. `control_plane_repository.Kind` has no exact Decimal/`NUMERIC(38,12)` or timezone-aware `TIMESTAMPTZ` codec family;
2. `TableDescriptor.upsert_sql()` defaults to replace-all conflict update and cannot express immutable insert-once,
   exact-replay collision checks, or split-axis monotonic CAS.

Later implementation must add the exact codecs and specialized owner primitives before any cost/evidence migration. It
must not encode money as float/text, accept caller timestamps, use generic replace-all upsert for immutable evidence, or
perform read-merge-write authorization in Python.

## 8. D3c2h1 schema-manifest boundary

D3c2h1, not this batch, owns the next exact decision surface. It must start from this cross-contract lock plus D0f and
ratify:

- ordered manifests and totals for `verification_intent`, both receipt families, and late quarantine;
- exact SQL types, nullability/defaults, local checks, keys, indexes, full-PFX FKs, and rollback behavior;
- exact repository/module names, typed insert/exact-replay/CAS methods, collision taxonomy, and transaction-composition
  API;
- durable pending-response-classification owner/state, retry/recovery, exact replay, and idempotency after an
  exposure-first receipt commit;
- full-PFX occurrence/idempotency encoders and retention/tombstone deadlines;
- response/failure/no-call race acceptance plus live/simulate/scripted cross-mode mutation tests;
- the separately owner-ratified Harvest/provider-search transport variant before any provider-search migration or live
  use.

Until D0f and D3c2h1 are reviewed and implemented, `verification_intent`, transport receipts, and quarantine remain
physically absent and strict-D3 transport activation remains closed. This document authorizes no migration from a
narrative field list.

## 9. Mechanism × ten-invariant matrix

| mechanism | 1 owner | 2 tenant | 3 fence | 4 lifecycle | 5 late/partial | 6 cost | 7 physical identity | 8 provenance | 9 consistency | 10 mode isolation |
|---|---|---|---|---|---|---|---|---|---|---|
| full-PFX evidence lineage | each row retains its domain owner; §3 | all five PFX fields mandatory; §4 | every replay/CAS compares PFX; §4 | immutable identity never rebinds; §4 | late evidence uses its original PFX; §4/§5 | PFX ties evidence to one exposure; §4 | scope-only sketches superseded; §2/§4 | server-owned parent copies only; §4 | one prefix across all surfaces; §4 | provider mode is physical identity; §4/§6 |
| receipt attempt identity | transport-evidence owner only; §3 | attempt copy remains within exact PFX; §4 | post-claim attempt plus generation/epoch; §4 | immutable on insert/replay; §4 | stale receipt uses exposure copy; §4 | binds one paid/zero-cost physical call; §4/§6 | exact-copy chain has one ActivityAttempt source; §4 | caller/model cannot claim attempt; §4 | event-side alias remains forbidden; §4 | cross-mode replay fails PFX equality; §4 |
| D0f durable envelope ref | one future D0f persistence owner; §3/§7 | PFX-bound ref grammar required; §7 | issuance and exact replay are owner CAS; §7 | retention/tombstone required; §7 | preserves non-authorizable terminal evidence; §7 | exact exposure ref remains D3c2g-owned; §3/§7 | canonical envelope digest plus owner ref; §7 | no placeholder or caller provenance; §7 | existing schema remains single canonical shape; §3/§7 | live/simulate/scripted explicit, replay absent; §6/§7 |
| post-network composition | coordinator owns two orders, repositories own rows; §3/§5 | one PFX across every participant; §4/§5 | classification prefix or quarantine-ineligible exposure-first tail; §5 | atomic insert/replay/terminalization per UoW; §5 | only classification path may quarantine; §5 | exposure terminalizes before later settlement; §5 | exposure/receipt/quarantine identities stay linked; §5 | typed owners only; §3/§5 | after exposure no return to earlier aggregates; §5 | same-mode equality required; §4/§6 |
| response classification authority | coordinator derives from locked stored rows only; §3/§5 | `d3-dispatch-v2` and all owner rows share PFX; §4/§5 | full global prefix precedes exposure; §5 | current/stale classification is transaction-local; pending retry deferred; §5/§8 | late response can be classified after receipt replay; §5 | classification cannot change settlement amounts; §5 | exposure claim/attempt/epoch/business pins compare to current rows; §5 | caller flag, stale ClaimReceipt, and serialized capability forbidden; §5 | prefix is read-only and optional quarantine is atomic; §5 | provider mode exact-compared before classification; §4/§5 |
| quarantine axes | shared quarantine repository only; §3 | immutable PFX relation; §4/§5 | insert-once plus disjoint CAS; §5 | cost and retention monotonic; §5 | valid late response only; §5 | confirmed/uncertain only, never no-call; §5 | no unowned workflow-run identity; §5 | response receipt is sole ingress; §5 | orthogonal axes, no mixed disposition; §5 | non-live cannot replay into live; §4/§6 |
| model transport and modes | transport variant owner plus typed repositories; §3/§6 | transport identity carries PFX; §4/§6 | variant applicability before row creation; §6 | replay remains absent in v1; §6 | simulate/scripted keep real evidence chain; §6 | live positive, simulate/scripted zero; §6 | model and provider-search variants cannot alias; §6 | model-only pins never fabricated; §6 | Harvest variant explicitly deferred; §6/§8 | three eligible modes, replay zero-write; §6 |
| storage prerequisites | future codec and specialized owner primitives; §7 | codecs cannot erase PFX values; §7 | CAS stays in SQL owner primitive; §7 | insert-once and split axes required; §7 | partial/collision rolls back; §5/§7 | exact Decimal, never float/text; §7 | DB timestamps and immutable refs; §7 | no generic replacement of evidence; §7 | D3c2h1 owns exact manifests; §8 | typed provider mode never fallback; §6/§7 |

Every one of the 80 cells is populated. No cell claims physical implementation. No new OB-ID is created; this batch
fixed-forwards contradictions within the existing Plan §6 / D3b / D3c2g obligation boundary.

## 10. Executable oracle and current physical baseline

`tests/test_d3c2h0_evidence_cross_contract_ratification.py` mechanically checks:

- the exact canonical declaration block and H0-1..H0-13 supersession inventory;
- complete PFX propagation, response/failure receipt `command_attempt`, quarantine identity/state exclusions, and the
  two post-network orders, including global-prefix classification and exposure-first zero-quarantine authority;
- owner separation, D0f-before-D3c2h1 ordering, model-only v1 applicability, non-live zero-cost reachability, replay
  zero-write behavior, and Harvest/provider-search deferral;
- the complete eight-mechanism × ten-invariant matrix;
- decision-time source absence of future receipt/quarantine owners and durable envelope refs;
- the decision-time generic Kind/replace-all incompatibility and D0c/D3b/D3c2g evidence behind each supersession.

The absence checks are a decision-only ratchet, not a permanent ban. A later implementation batch must intentionally
advance the oracle together with reviewed SQL/runtime evidence; it may not make this batch look implemented retroactively.

## 11. Explicit non-closure, validation, and next order

D3c2h0 closes no runtime, migration, rollout, formal-review, provider, live, W6, manual, or product gate. `R-019`,
`R-023`, `R-027`, `R-029`, action-root durable scope, OB-10.1/10.2/10.4, full Migration A, Migration B-D,
terminal-provenance registry implementation, strict writers, served Agent population, and every live/product gate remain
open. D3c2g's OB-2.2/OB-10.3 status remains `decision_locked_not_implemented`; this batch does not advance it.

The required order is:

1. D0f ratifies and implements the sole durable envelope reference owner without changing the canonical
   `ModelInvocationEnvelopeV1` schema;
2. D3c2h1 ratifies exact intent/receipt/quarantine manifests and the provider-search variant decision where applicable;
3. storage prerequisites and reviewed dormant migrations land in bounded batches;
4. repositories, atomic/race acceptance, strict writers, fake/scripted E2E, and only then separately gated live paths.

Local non-live validation for this decision batch:

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src .venv/bin/python -m pytest -q \
  tests/test_d3c2h0_evidence_cross_contract_ratification.py
.venv/bin/ruff check tests/test_d3c2h0_evidence_cross_contract_ratification.py
.venv/bin/ruff format --check tests/test_d3c2h0_evidence_cross_contract_ratification.py
git diff --check -- \
  docs/TRACK_D_D3C2H0_EVIDENCE_CROSS_CONTRACT_RATIFICATION.md \
  tests/test_d3c2h0_evidence_cross_contract_ratification.py
```

Author validation is local evidence only. It is not an independent or formal `GO`.

### D0f successor observation (2026-07-15)

The bounded D0f implementation candidate now supplies the sole PG-only durable `ModelInvocationEnvelopeV1` ref owner,
an exact `TIMESTAMPTZ` codec, immutable insert/exact-replay storage, and the fixed retained-to-tombstone CAS. This
intentionally advances the D0f predecessor described above without retroactively changing D3c2h0's decision-time
evidence. D3c2h1 receipt/quarantine manifests, Decimal cost codecs/tables, strict runtime writers, and every live/product
gate remain outside D0f.
