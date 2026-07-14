# Track D D3c1a — Workflow-command public-projection fixed-forward

> Status: implementation candidate; non-live only. This batch fixed-forwards the five direct findings produced by the
> invalid D3c1 pinned-review artifact. That artifact could not causally bind the effective reviewer settings, exact
> prompt, final output, and completed turn, so it is neither a formal `NO-GO` nor a formal `GO`. Author evidence on the
> final stable candidate tree is complete; a fresh pinned non-author review remains pending. Later precommit adversarial author audits
> also found and fixed the bounded cross-layer gaps recorded in §3. Those gaps are part of the D3c1a candidate, but they
> are not retroactively counted among the invalid artifact's five findings and are not independent-review evidence.

## 1. Outcome and bounded impact

D3c1a extends the D3c1 command projection seal to the complete public Activity spine, closes normalized private-field
aliases, prevents generic carriers from publishing caller-supplied execution summaries, and makes the two future
`BIGINT` diagnostics lossless within the JavaScript number wire contract. Precommit adversarial author audits further
closed structured Activity artifact refs, cancel/retry/resume Activity carriers, Activity carriers nested recursively
inside command results, cross-language mathematical-integer canonicalization, typed value-family parity, recursive
command closure inside direct Activity evidence, hazardous-object-key rejection, and partially constructed
orchestrator compatibility. Later passes closed the complete Operation action/event/run and registry mirror families,
typed trusted execution summaries, normalized response-envelope aliases, and demo `raw`/provenance views. The batch
also replaces path-filtered review checks with explicit route, owner, alias-dataflow, mutation, schema, adapter, and
demo gates.

The canonical backend owners remain deliberately split:

- `CommandKernel` owns the checked-in command and Activity allowlists, recursive public-mirror sanitizer, safe-integer
  validation, operation-sync projection, and generic carrier projection;
- `SourcingOrchestrator` owns Activity-derived public fields and the final attachment of an execution summary computed
  from trusted ActivityRun, ActivityAttempt, and EntityDelta evidence;
- the frontend schema, TypeScript types, public adapter, and demo are mirrors of those backend-owned public shapes, not
  independent field owners.

This batch changes no storage schema, repository write path, claim/CAS path, provider/model behavior, or Agent tool
population.

## 2. Five direct findings and fixed-forward closure

| Direct finding | Fixed-forward implementation | Mechanical gate |
| --- | --- | --- |
| ActivityRun, ActivityAttempt, and EntityDelta endpoints bypassed the capability seal | Three checked-in backend allowlists and matching closed frontend shapes now project every list/detail record through the shared recursive sanitizer | exact backend/schema/type/adapter/demo field equality; six Activity route bindings; nested malicious fixtures; source-record non-mutation |
| Normalized private aliases were not fail-closed | One normalization rule plus canonical snake and underscore-free compact root-prefix matching covers every private root | shared root-set equality across Python and both TypeScript mirrors; acronym/camel/hyphen/space/repeated-underscore/compact alias matrix |
| Generic carrier data could forge Activity-owned `execution_summary` | Generic carriers always discard supplied summaries; only the trusted Activity-derived owner attaches or reattaches a freshly computed summary after final generic projection | forged command/action/event/run metadata fixtures; trusted list/detail/status/control summaries remain present and Activity-derived |
| `claim_generation` and `control_epoch` accepted incompatible or lossy numbers | Both diagnostics are optional mathematical integers in `[0, 9007199254740991]`; invalid values are omitted and accepted integral representations are canonicalized | Python/JSON/TypeScript safe-integer matrix and exact boundary parity |
| The preflight did not exhaustively prove route and bypass closure | Route discovery runs before expected-manifest comparison, owner reachability is explicit, and the raw-return gate follows aliases and branch dataflow | exact method-route manifests, seven operation-sync owner calls, unsafe alias/dict/call/branch counterexamples, frontend executable projection |

These are implementation responses to direct advisory evidence. They do not themselves establish an independent-review
verdict.

## 3. Additional precommit adversarial author-audit fixes

After implementing the five direct findings, adversarial passes in the author session exposed additional bounded
cross-layer gaps. They are fixed in the same D3c1a candidate because leaving them open would make the advertised public
seal internally inconsistent:

1. **Structured Activity artifact refs.** ActivityRun, ActivityAttempt, and EntityDelta rows can carry mixed JSON
   artifact references, including structured object references. Their public `artifact_refs` contract is therefore a
   recursively sanitized JSON-value array in backend, schema, TypeScript, adapter, and demo. Nested private aliases and
   non-JSON/non-finite values are removed while legitimate strings, numbers, booleans, nulls, objects, and arrays remain.
   The existing workflow-command `artifact_refs: string[]` contract is unchanged.
2. **Control-response Activity carriers and derived fields.** The public cancel/retry/resume wrappers now run all four
   served singular keys (`workflow_activity`, `workflow_activity_run`, `workflow_activity_attempt`, and
   `workflow_entity_delta`) plus the three served plural keys (`workflow_activity_runs`,
   `workflow_activity_attempts`, and `workflow_entity_deltas`) through the same canonical record helpers. That preserves
   owner-derived `activity_type`/`owner`, the 10-field `control_target`, `module_state_mutated=false`, and the read-only
   `mutation_contract`; a response spread cannot bypass the closed record shape. Camel/hyphen/space aliases are
   removed at the final control boundary unless the raw key is one of those exact seven served canonical keys.
   Generic command/action/event carriers strip those owner-derived fields instead of trusting source JSON; only direct
   Activity endpoints, trusted summaries, and the exact canonical control carriers reattach current-evidence values.
3. **Recursive command-result carrier closure.** The generic carrier walker recognizes eight canonical snake carrier keys
   plus their normalized camel/hyphen/space variants
   at every dictionary/list depth: `workflow_activity`, `workflow_activity_run`, `workflow_activity_attempt`,
   `workflow_entity_delta`, `workflow_activities`, `workflow_activity_runs`, `workflow_activity_attempts`, and
   `workflow_entity_deltas`. This includes `workflow_command.result.workflow_activity_run`. Nested Activity records
   receive their exact projector instead of only private-alias redaction, so unknown future columns cannot survive
   through a command result. The frontend mirrors use the same recursive carrier rule.
4. **Mathematical safe-integer canonicalization.** JSON and JavaScript do not distinguish an integer-valued `1.0` from
   `1`, and JavaScript can preserve a negative-zero representation. The cross-language rule therefore accepts bounded
   mathematical integers and emits one canonical integer representation: `1.0 -> 1` and `-0.0`/`-0 -> 0`; booleans,
   fractions, negatives, strings, non-finite values, and overflow remain omitted.
5. **Lazy `CommandKernel` compatibility.** Normal orchestrator construction still installs one store-bound
   `CommandKernel` eagerly. A private lazy property now materializes that same per-instance owner for legacy and
   characterization paths that partially construct `SourcingOrchestrator` and attach only `store`. This is a
   compatibility guard, not a second owner, singleton, runtime fallback, or authorization path.
6. **Recursive workflow-command carrier closure.** Canonical and normalized singular/plural workflow-command carriers
   nested inside any descriptor JSON field now re-enter the same 42-field command projector at every depth. Operation
   sync and each direct ActivityRun/ActivityAttempt/EntityDelta projector use the combined Activity-plus-command
   walker. Normalized caller-supplied `execution_summary` values are removed recursively; only a direct trusted
   command-record root may retain the owner-derived summary before generic carrier projection removes it again.
7. **Hazardous-key and malformed-input frontend handling.** Python, the adapter, and the demo reject `__proto__`,
   `prototype`, and `constructor` at every arbitrary JSON depth rather than forwarding enumerable hazardous keys.
   Known command/Activity carriers use plain-object guards, malformed scalar/array records are omitted, and a malformed
   nested control target or command list member cannot abort the whole response.
8. **Independent contract oracles.** The preflight now derives the Activity base fields from the three repository
   descriptors, adds literal derived-field sets, fixes exact `23/27/26/10` counts, and independently locks the eight
   recursive Activity keys, three recursive command carrier families, and seven served control-response keys. A
   simultaneous production-tuple/frontend edit can no longer self-certify a changed public shape.
9. **Typed backend value-family closure.** The Python command, operation-sync, ActivityRun, ActivityAttempt,
   EntityDelta, and control-target projectors now enforce the same string, finite-number, string-array, JSON-array,
   JSON-object, boolean, and safe-integer families as schema and TypeScript. A known key with a malformed value is
   omitted instead of producing schema-invalid public JSON, including when nested in a command result. The four nested
   control-target policy/display/state objects and operation-run/event objects apply the same known-field typing while
   preserving schema-permitted safe extension fields.
10. **Observation bypass closure.** `_workflow_command_observation(...)` now compacts only the output of the canonical
    command projector. Its public intake/workflow callers therefore cannot use compact observations to bypass private
    capability redaction, string-array filtering, object typing, or the route gate that classifies it as a projector.
11. **Operation-family recursive and typed closure.** Backend, adapter, and demo action/event/run projectors now apply
    the same recursive capability/hazardous-key seal to the complete open record before strict-projecting known
    strings, booleans, finite numbers, arrays, objects, control state, and status summary. Malformed list members and
    singular records are omitted without aborting a response. Demo Action/Run/Event `raw` views, provenance envelopes,
    and action-decision envelopes contain only the sanitized mirror. Python operation helpers apply the same typed
    projectors before records cross the service boundary.
12. **Trusted-summary typing and dual-source ownership.** The Activity-owned root `execution_summary` is strict-typed
    in Python, adapter, and demo, including four numeric count maps and exact latest Activity/Attempt/Delta projectors.
    Response mappers use a raw-plus-sanitized dual-source rule: open extensions and generic aliases come from the
    sanitized source, while only exact canonical backend-owned command/status fields are reprojected from the raw
    source so a legitimate owner-derived summary survives. Generic Operation/command carriers still strip it.
13. **Normalized response-envelope closure.** Command list/detail plus all six Activity list/detail wrappers sanitize
    the complete envelope before spreading open extensions. Exact canonical command and Activity fields re-enter their
    trusted projectors; camel/hyphen/space aliases remain generic carriers and cannot retain a summary, unknown column,
    control target, mutation contract, module-state flag, or derived ActivityAttempt/EntityDelta owner.
14. **Registry-family closure.** Workflow-command policy/state/display/activity-spine contracts, command registries,
    Operation action registries, and their nested command/control-summary objects now use the same recursive seal and
    strict known-field types. Malformed registry members are filtered, optional collections remain sparse, and a raw
    spread cannot expose private aliases, hazardous keys, or loosely coerced count/boolean evidence.

These were precommit adversarial **author-audit** findings. They are neither additional findings attributed to the
invalid D3c1 review artifact nor a formal `GO`/`NO-GO`. Their final author validation is recorded in §9.

## 4. Closed Activity-spine public shapes

`CommandKernel` now owns three literal public-field tuples:

- ActivityRun: **23 fields** = 20 repository descriptor fields plus `control_target`, `module_state_mutated`, and
  `mutation_contract`;
- ActivityAttempt: **27 fields** = 22 repository descriptor fields plus derived `activity_type`, derived `owner`,
  `control_target`, `module_state_mutated`, and `mutation_contract`;
- EntityDelta: **26 fields** = 21 repository descriptor fields plus derived `activity_type`, derived `owner`,
  `control_target`, `module_state_mutated`, and `mutation_contract`.

`WorkflowActivityControlTarget` is a separate closed **10-field** object:
`target_type`, `command_id`, `command_type`, `owner`, `command_status`, `display_contract`, `control_policy`,
`control_state`, `activity_spine_policy`, and `fallback_status`.

The six Activity list/detail variants route through the corresponding orchestrator record helper and then the canonical
kernel projector. Unknown fields, private aliases, unsupported objects, and non-finite numbers cannot survive merely
because a repository row gains a key. Projection is sparse and does not mutate its source. ActivityAttempt
`attempt_number` is additionally restricted to the same non-negative JavaScript-safe integer interval; booleans,
fractions, negative values, strings, and larger integers are omitted.

The frontend schema sets `additionalProperties=false` on all four shapes. The three TypeScript record interfaces no
longer extend `JsonObject`; the public adapter does not spread source records; and the demo derives its `raw` view from
the same explicit wire allowlists. Activity `artifact_refs` preserve structured mixed JSON values after recursive
sanitization; workflow-command `artifact_refs` remains the existing string array. Nested JSON values reuse the
recursive sanitizer in both TypeScript layers.

## 5. Exact, compact, and root-prefix private aliases

The canonical normalizer trims the key, maps runs of hyphens or whitespace to `_`, splits acronym and lower/camel
boundaries, collapses repeated underscores, trims edge underscores, and lowercases the result. The predicate then
compares both that canonical snake form and its underscore-free compact form against every member of
`WORKFLOW_COMMAND_PRIVATE_PUBLIC_MIRROR_FIELDS` treated as a root:

- exact canonical and compact matches are private;
- `canonical_root_*` is private;
- any compact value beginning with the compact root is private.

This closes forms such as `CLAIMToken`, `claimSecretPreview`, `claimReceiptEnvelope`, `leaseTokenDigest`, and compact
scoped-bootstrap carriers without introducing an over-broad `claim_*` rule. Public diagnostics and business data such
as `claim_generation`, `control_epoch`, `artifact_digest`, business digests, `lease_owner`, payload/result content, and
artifact refs remain visible. The sanitizer is a secrecy boundary only; it never authorizes a runtime effect.

Python, the public adapter, and the demo carry mechanically compared root literals and the same normalization and
snake/compact-prefix algorithm. Recursive dictionaries and arrays use this predicate at every depth.

## 6. Recursive carrier closure and trusted execution-summary ownership

The generic public carrier recursively projects the eight canonical Activity carrier keys and their normalized aliases wherever they
occur, rather than only at the top-level Activity endpoints. This includes command `payload`/`result`, operation
metadata, events, control responses, command lists, and trusted-summary samples. A nested `workflow_activity_run`,
`workflow_activity_attempt`, or `workflow_entity_delta` is therefore constrained to its 23/27/26-field shape and its
10-field control target before the enclosing carrier can be returned.

Each direct Activity projector then applies the workflow-command walker to its already typed record. Command carriers
and normalized execution-summary keys embedded in Activity metadata, attempt output, or entity payload therefore
receive the same closure as command/operation carriers instead of relying on a frontend mirror to repair backend JSON.

The same walker projects canonical and normalized `workflow_command`, `latest_workflow_command`, and
`workflow_commands` carriers recursively inside command descriptor JSON, operation sync, events, and metadata. Every
nested command re-enters the closed command projector, malformed list members are omitted, and caller-provided nested
execution summaries remain stripped.

The cancel/retry/resume public wrappers apply an orchestrator-owned final pass to the four served singular and three
served plural Activity carrier keys so derived control fields come from current command/Activity evidence. The frontend
control response schema, types, and adapter explicitly remap those same seven served keys instead of trusting a generic
response spread. `workflow_activities` remains a recursively recognized generic plural carrier key; it is not an eighth
served cancel/retry/resume response field. Normalized aliases such as `workflowActivityRun` and
`workflow-activities` are rejected at that final response boundary.

Generic nested Activity carriers intentionally omit `control_target`, `module_state_mutated`, and `mutation_contract`;
generic ActivityAttempt/EntityDelta carriers also omit their derived `activity_type` and `owner`. These values are not
accepted from action input, event payload, command result, or normalized aliases. Trusted orchestrator-owned direct
Activity records and exact canonical control-response carriers reattach them only after current command/Activity
evidence has been read and typed.

`CommandKernel._workflow_command_public_carrier_api_record(...)` now removes every source-provided
`execution_summary`, including values nested under `workflow_command`, `latest_workflow_command`, command lists,
actions, events, operation metadata, or another carrier. The generic command projector cannot restore it.

`SourcingOrchestrator._workflow_command_api_record_with_execution_summary(...)` first completes generic projection,
then attaches a sanitized summary computed by the Activity-spine owner. Operation status and control projections use a
narrow trusted reattachment helper after the generic carrier has stripped untrusted summaries. The reattached value
must come from the already computed trusted operation record; caller metadata is never a fallback source.

This ordering preserves legitimate command detail/status/control provenance while preventing a carrier from forging
Activity counts, latest effects, attempts, or deltas.

## 7. Safe-integer wire contract

`claim_generation` and `control_epoch` remain optional, non-authoritative diagnostics. Their canonical public wire
representation is a JSON integer from zero through `Number.MAX_SAFE_INTEGER` (`9007199254740991`), inclusive:

- Python accepts `int` or finite integral `float` values, explicitly excludes `bool`, and emits a canonical `int` in
  the bounded interval;
- JSON Schema uses `type=integer`, `minimum=0`, and `maximum=9007199254740991`;
- the adapter and demo require `Number.isSafeInteger(value) && value >= 0` and canonicalize either JavaScript zero
  representation to `0`.

Thus `1.0` is emitted as `1` and `-0.0`/`-0` as `0`. Invalid, fractional, negative, boolean, string, non-finite, or
precision-losing values are omitted rather than rounded or loosely coerced.
This fixed-forward does not change the existing `attempt` or `max_attempts` number contract, and the diagnostics do not
become claim authority, replay identity, or a durable fence.

## 8. Route, dataflow, mutation, and frontend gates

The primary public projection manifest contains **23 method-route variants**: the existing **17** workflow-command and
operation/action variants plus **6** Activity list/detail variants. The job materialization compact-command binding is
kept in a separate compact manifest; it is not folded into the 23-variant primary denominator and must still prove that
compaction receives a canonically projected command.

The route gate first inventories registered API bindings, then compares the discovered projector-reachable set with
the manifest. It cannot hide a newly added carrier route by filtering through expected paths before comparison. Each
binding also names the transport handler, orchestrator owner, and required reachable projector.

The operation-sync gate fixes the seven existing owner calls across `CommandKernel`, `AcquisitionCommandOwner`, and
`SourcingOrchestrator`. Its dataflow analysis rejects raw singular/plural command returns through direct variables,
assignment aliases, `dict(...)`, arbitrary calls, conditional branches, and merged control flow; centrally projected
returns remain valid. Activity and command fixtures also prove sparse projection, recursive redaction, non-finite
omission, preservation of legitimate business values, and no mutation of the input records.

Frontend gates mechanically compare all 23/27/26/10 backend fields against JSON Schema, TypeScript interfaces, public
adapter mapper keys, and demo wire allowlists. Bundled adapter/demo fixtures exercise unknown fields, nested exact and
alias capability keys, compact/root-prefix forms, structured Activity artifact refs, non-finite values, mathematical
safe-integer canonicalization (`1.0 -> 1`, `-0 -> 0`), ActivityAttempt bounds, all eight canonical recursive command-result
Activity carrier keys, nested canonical/normalized command carriers with forged summaries, all seven served
cancel/retry/resume carrier keys with derived control fields plus noncanonical alias rejection, direct Activity records
containing nested commands/summaries, wire-level hazardous keys with downstream `Object.assign`, malformed typed
command/operation-sync/Activity/control-target values, malformed command members in four normal adapter entries, and
closed demo `raw` records. The matrix also proves compact observations cannot bypass the canonical projector and that
generic Activity carriers lose forged owner-derived fields while trusted direct/control records retain current-evidence
values. The compatibility gate exercises a partially constructed orchestrator without introducing a second kernel
owner.

## 9. Acceptance evidence and review status

Author evidence on the final stable candidate tree:

- D3c1a projection contract: **13 passed**;
- D3a + D3b + D3c1a plus three exact durable-runtime adjacency nodes: **56 passed**
  (`6 + 34 + 13 + 3`);
- full operation runtime: **129 passed**;
- pre-Agent Activity/frontend contract adjacency: **4 passed**;
- Activity HTTP runtime plus route parity: **2 passed**;
- adjacent D3c2a migration/PG foundation: **9 passed, 20 subtests passed**;
- standalone frontend contract/adapter TypeScript compile: exit **0**;
- frontend production build: **81 modules transformed**;
- lint: repo **58 files** plus the three D3 test files and the updated pre-Agent adjacency oracle formatted and checked;
  all checks passed;
- mypy ceiling: unchanged at **81 errors in 4 files**;
- `git diff --check`: clean.

The repository-wide markdown-status check remains **1 passed / 1 failed** on both this tree and a clean `HEAD`
archive because the same twenty historical documents lack its required banner; this baseline-identical failure is not
counted as a D3c1a pass and is not changed by this batch.

The earlier D3c1 review artifact remains invalid formal evidence even though its five direct findings motivated this
fixed-forward. The additional §3 changes came from precommit adversarial author audits and must not be described as
that artifact's findings or as independent review. A fresh pinned non-author review must bind the final D3c1a commit
and remains **pending**. Until a valid scope-matched artifact records a verdict, D3c1a has neither formal `GO` nor
formal `NO-GO`; live/W6/manual validation, promotion, and milestone signoff remain fail closed for this scope.

## 10. Explicit non-closure

D3c1a closes only the public-projection findings. It does not activate D3c2a's dormant migration foundation, complete
full Migration A, or implement scoped review-session issuance, bootstrap or strict-D3 manifests,
ClaimAuthority/ClaimReceipt mint and verification, Stage A/B, generation/token/epoch CAS, heartbeat occurrence,
business fencing, terminal provenance, late-result quarantine, dispatch coordination, action-root durable scope, or
any Plan §6/OB-ID obligation.

R-019 remains open. Served Agent tool population remains zero. No fake, scripted, or live provider/model path is
authorized by this batch, and no provider-costing validation or product handoff may infer authorization from a closed
public mirror or a safe diagnostic value.
