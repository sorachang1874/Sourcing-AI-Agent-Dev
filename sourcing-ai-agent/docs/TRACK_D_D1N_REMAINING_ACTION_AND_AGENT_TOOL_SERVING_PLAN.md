# Track D D1n Remaining Action and Agent Tool Serving Plan

> Status: Current bounded implementation plan. This document fixes the top-down migration order for the remaining
> five API-submittable actions and the first shadow Agent tool. It is not implementation evidence, a review verdict,
> production serving approval, or live-provider authorization.

Date: 2026-07-17

## 1. Outcome and boundaries

D1n closes the implementation-side `5/15` schema-less action numerator before scripted or product E2E is allowed to
act as a discovery mechanism. Contract, owner, target, result, and tool-serving behavior are specified and tested
first; fake/scripted and browser E2E then confirm the assembled path.

The batch has three independently measurable outcomes:

1. all 15 API-submittable actions have a revisioned closed request schema, an owner-minted physical target, a
   registered dispatch adapter, and exact replay/approve/retry pins;
2. a shared revisioned model-safe result registry, serializer harness, and mechanical served predicate exist;
3. `search_projection` becomes the first **shadow-ready** tool and passes no-provider simulate dispatch; the minimum
   local acquisition tool suite then adds `plan_acquisition`, `start_acquisition_run`, and a read-only Operation status
   tool before the Thinking Machines Lab canary. Production served population remains zero until all release and review
   gates are satisfied.

D1n does not authorize live providers, expose raw command results to a model, close R-019/R-028 by assertion, or mark
R-029 closed merely because the in-code schema-less numerator reaches zero.

### 1.1 Twenty-four-hour executable vertical slice

The local canary is not blocked on making all 15 actions product-served at once. The ordered executable slice is:

```text
plan_acquisition -> user confirms role/employment multi-select -> start_acquisition_run
-> inspect_operation -> search_projection
```

- `plan_acquisition` and `start_acquisition_run` share one canonical population selector: nonempty
  `role_buckets[]` from the existing role-bucket vocabulary and nonempty `employment_statuses[]` from
  `{current, former}`. Empty arrays are rejected rather than interpreted as a hidden default.
- `start_acquisition_run` advances from request v1 to a revisioned v2 that carries those explicit selectors plus the
  reviewed thematic facets/keywords. It may bind an immutable preview id/revision; it must not re-infer a different
  population from free text after the user confirms the preview.
- `inspect_operation` is a read-only tool projection over the existing Operation/Command/Activity APIs, not a sixteenth
  API-submittable action. It exposes bounded status, progress, next controls, and result readiness without repairing or
  mutating workflow state.
- `search_projection` reads the canonical published result with the same selected role/employment facets. It is the
  first serializer pilot because it has no provider cost, then becomes the terminal read leg of the acquisition slice.

The slice can become locally served only after its exact commits have valid pinned review artifacts and fake/scripted
Agent E2E passes. Reviews run asynchronously and do not block implementation of the next independent node. Hosted or
general production serving remains a later gate.

## 2. Frozen population

The production registry denominator remains 15. D1m leaves 10 schema-defined actions and these five migrations:

| Action | Current registry owner | D1n owner decision | Request/target shape | Dispatch/result shape |
| --- | --- | --- | --- | --- |
| `fetch_profile_sample` | `profile_scheduler` | retain; physical command owner is `linkedin_profile_activity_owner` | closed bounded profile-sample request; binder mints source Activity/Delta and acquisition lineage | one `linkedin.profile_fetch.activity.run`; typed sample summary result |
| `continue_acquisition_run` | `acquisition_run_writer` | retain as the action-level owner; each variant keeps its registered command owner | closed discriminated request keyed by one of seven allowed command types; no opaque `command_payload` | seven exact variants; one normalized progress/result envelope |
| `plan_acquisition` | `planner` | retain as a commandless pure-preview owner | closed user-intent + company + bounded facet request; binder mints requester/workspace scope | one PG UoW terminal preview; no provider, command, plan-review, or acquisition-run side effect |
| `promote_person_assertion` | `person_assertion_writer` | retain generic evidence-to-assertion semantics | reviewed evidence id + immutable evidence revision + assertion type/value; binder mints person/evidence target | dedicated promotion UoW through `PersonAssertionWriter`; R-028 applies |
| `external_intake` | `intake_service` at registry boundary; `excel_intake_owner` executes v1 | v1 is Excel-only and model-assisted | server-minted staged artifact ref + filename/company/options; raw path/base64 forbidden from the Agent request | standard Operation-linked Excel command; typed intake summary result; budget required |

These decisions deliberately preserve the user-facing action count. Splitting `continue_acquisition_run` into seven
new public action names or projecting direct upload internals into `external_intake` would change the denominator and
is outside D1n.

## 3. Shared foundation before per-action integration

### F1. Result contract registry

Add one immutable registry parallel to `ActionRequestSpec`. Every served candidate declares:

- `result_schema_version` and canonical digest;
- serializer and validator owner ids;
- exact success, deferred/stale, and terminal-error variants;
- deterministic item and byte limits;
- an exact field whitelist and artifact-reference policy.

The serializer consumes owner output and produces the only payload eligible for `ToolResultMessage`. It must never
copy an arbitrary `workflow_commands.result` dictionary. Unknown fields, non-JSON values, raw filesystem paths, and
oversize output fail closed before the action can be considered served.

### F2. Simulate serializer harness

Provide one no-network harness that:

1. validates the request with the pinned request schema;
2. invokes the registered adapter in an isolated `simulate` namespace;
3. follows the resulting Operation/action/command to a terminal or explicitly deferred state;
4. invokes the registered result serializer and validator;
5. records the exact request/result versions and digests exercised.

A manifest-only check is insufficient. `simulate_preflight_passed=true` is derived only from this complete execution.

### F3. Agent tool registry and served predicate

Expose a shadow registry derived from canonical owners, not a second manually maintained list. An action is
`shadow_ready` only when all five conditions are true:

1. a non-empty revisioned request schema exists;
2. a registered dispatch adapter exists;
3. every allowed command is Agent-callable, non-legacy, and satisfies its Activity policy;
4. a revisioned result schema, validator, and serializer owner exist;
5. the simulate harness passed using that serializer.

`served` additionally requires the release/review gates. Until those gates pass, the public tool population remains
zero even if one or more actions are shadow-ready. Tool schema version/digest pins must survive turn creation,
AgentAction persistence, approve/retry, terminal result, and journal replay.

### F4. Scoped state UoWs

The two generic command actions touch the R-019 tripwire. D1n must not add another sequence of independently committed
command-plan, action, run, and event writes. Provide bounded repository UoWs for:

- command creation plus action/run/event transition; and
- command terminal result plus action/run/event synchronization.

`promote_person_assertion` separately requires the R-028 promotion/assertion/event/index-plan UoW. These are scoped
closures for the touched paths, not a claim that all historical R-019/R-028 callers are migrated.

## 4. Per-action contract locks

### M1. `fetch_profile_sample`

Canonical input contains only the bounded sample request and owner selectors. The binder resolves the current
Activity/EntityDelta/acquisition lineage and mints the physical target. Caller-provided aliases currently accepted
from `input`, `target_ref`, or nested `command_payload` are either canonicalized once or rejected on presence; no
truthy precedence remains.

The action plans exactly `linkedin.profile_fetch.activity.run`. Its model-safe result reports sample counts, bounded
profile summaries, cache/fetch disposition, command/activity status, and artifact references. It excludes raw provider
payloads and registry filesystem paths.

### M2. `continue_acquisition_run`

Use one closed discriminated schema with seven variants:

- `linkedin.discovery_query.run`;
- `linkedin.profile_fetch.activity.run`;
- `linkedin.profile_fetch.provider.fetch`;
- `linkedin.profile_terminal.admit`;
- `projection.profile_admission.apply`;
- `projection.person_search_index.build`;
- `collection.authoritative.merge`.

Each variant declares its own required target revision/generation, canonical input fields, owner binder, and exact
command payload builder. Shared profile/projection helpers may reduce duplication, but no caller-controlled opaque
payload is retained. `linkedin.profile_refill.submit_batch` is not in this action's allowlist and stays rejected.

The normalized result envelope reports the selected variant, physical command/activity identity, status/progress,
bounded effect summary, and next allowed controls. Variant owners retain their existing domain effects.

### M3. `plan_acquisition`

Implement the registry's existing description literally: a commandless pure preview with no provider or workflow
side effects. Do not alias the existing side-effecting `plan_workflow` path.

The preview owner normalizes target company, the existing canonical role buckets (`research`, `engineering`,
`product_management`, and any other allowed role bucket), exact `current|former` employment multi-select, thematic
facets/keywords, source preferences, and bounded coverage intent. It then persists action/run/event plus the preview
result in one PG UoW. The result is suitable for user review and can later seed `start_acquisition_run`, but it does not
create plan review, criteria, commands, or an acquisition run. The reviewed preview id/revision freezes these choices;
the start action must reject a conflicting inline selector rather than choose a precedence rule.

### M4. `promote_person_assertion`

Keep the public action generic rather than silently redefining it as CRM Public Web promotion. The binder reloads the
reviewed evidence and immutable revision, resolves the person, and mints the assertion target. The writer validates
the assertion type/value against that evidence and commits promotion row, PersonAssertion, audit event, and required
index plan through the scoped R-028 UoW.

The model-safe result contains assertion id/type/value, evidence reference/revision, promotion status, and index-plan
status. It contains no unrestricted CRM notes or evidence body.

### M5. `external_intake`

V1 is explicitly Excel intake. Upload remains a separate authenticated transport step that returns a server-minted,
workspace-bound staged artifact reference. Agent input may select that reference and bounded intake options; it may
not submit a local path or base64 file body.

Replace synthetic workflow/operation identities with the current AgentAction/OperationRun lineage, and make terminal
command completion use standard Operation synchronization. Because normalization may invoke a model,
`budget_required=true`. The result reports row counts, accepted/rejected counts, bounded diagnostics, produced asset
references, and terminal status.

## 5. Parallel implementation DAG

```text
A0 contract lock (this document)
 |
 +-- F1 result registry + serializer core
 +-- F2 simulate harness
 +-- F3 shadow tool registry/predicate
 +-- F4 scoped command/terminal UoWs
 +-- M1 fetch-profile schema/binder/owner tests
 +-- M2 continue-run seven-variant schemas/binders/owner tests
 +-- M3 plan-preview owner/UoW/tests
 +-- M4 assertion-promotion owner/UoW/tests
 +-- M5 staged Excel intake/Operation integration/tests
        |
        v
B0 single-owner registry/API integration (15/15 schema-defined)
 |
B1 search_projection shadow result serializer + simulate preflight
 |
B2 plan/start/status/search local acquisition tool suite + simulate preflight
 |
B3 fake/scripted Agent loop and product E2E
 |
B4 pinned reviews + R-029 observation/constraint deployment
 |
B5 local served activation, then bounded live canary
```

Parallel workers own new leaf modules and focused tests. `operation_runtime.py`, `orchestrator.py`, public API routing,
shared migrations, and registry aggregation have one integration owner and are merged serially. This keeps parallel
work from racing on the two current shared hotspots.

Each independently committed batch gets a pinned non-author review request. Review latency blocks only that scope's
served/live/signoff gate; unrelated foundation or action batches continue.

## 6. Contract-first test matrix

Before fake/scripted E2E, require:

- exact 15-action enumeration and zero schema-less request pins;
- per-action owner, target binder, adapter, commands, approval/budget, result owner, and served-predicate matrix;
- presence-sensitive alias conflict/rejection tests, including empty values and `input` plus `input_payload` dual supply;
- foreign/missing owner parity with zero writes, plus same-owner and open-mode positives;
- stale target revision/generation, exact replay/conflict, approve/retry pin preservation;
- budget/approval/cancel/retry/late-terminal behavior;
- R-019 and R-028 fault injection at each write boundary;
- result serializer rejection for unknown/private fields, raw paths, oversize values, and nondeterministic output;
- simulate/scripted/live namespace contamination negatives;
- served predicate mutation tests proving removal of any one required condition yields `served=false`.

Fake/scripted then validates the assembled loop:

```text
model tool call -> validate/bind -> submit -> approve/budget -> dispatch
-> poll/retry/cancel -> terminal owner result -> model-safe serializer -> ToolResultMessage
```

Only after that passes should browser E2E validate tool discovery, role/employment multi-select UX, progress/control,
and final result rendering.

## 7. First shadow tool, TML canary, and lab scaling boundary

`search_projection` is the first shadow pilot because it is schema-defined, owner-bound, commandless, read-only, and
has no provider cost. Its first result serializer returns a bounded candidate summary/facets/readiness projection;
it does not expose raw CRM overlay fields and summarizes or references results beyond the byte/item limit.

The first live manifest is a bounded Thinking Machines Lab run. The manifest contains data, not company-specific code:

- canonical company selector and aliases resolved through the existing company registry;
- `role_buckets` and `employment_statuses` selected by the user;
- thematic facets for pre-training work;
- explicit sample/result/provider-call/cost/time limits; and
- the exact reviewed tool/request/result schema digests.

The canary first uses `research + engineering` and `current + former` only if that is the user's confirmed selection;
the implementation must also prove the other legal multi-select combinations in fake/scripted tests. Scaling to OpenAI,
Anthropic, Google DeepMind, xAI, Meta, and later labs changes only the manifest/company identity data and budgets. A
new lab must not require a new action, provider branch, role enum, or served predicate.

R-029 remains open after the code numerator reaches zero until scope-matched reviews, a new observation epoch with one
release window of durable zero compatibility hits, and separate validation of the installed `NOT VALID` constraints
complete. Production tool serving also remains gated by the applicable projection/hosted owner reviews. No live
provider canary begins from this plan alone.
