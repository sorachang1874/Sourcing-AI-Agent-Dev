# Track D D3c1a — Workflow-command public-projection fixed-forward

> Status: current fixed-forward candidate; non-live only. This batch first fixed-forwarded the five direct findings from
> the original invalid D3c1 pinned-review artifact. The later D3c1a artifact
> `runtime/reviews/20260714T215839Z_Track_D_D3c1a_workflow-command_public_projection_fixed-forward.md` is also
> **invalid/advisory only**: `reviewer_exit_code=0`, but `causal_binding.final_response_item_exact=false`, so its
> substantive `NO-GO` text is neither a formal `NO-GO` nor a formal `GO`. Its findings #1, #2, and #4-#11 are
> fixed-forwarded below; #3 is the accepted `R-019` residual and remains open. A second pinned attempt,
> `runtime/reviews/20260714T231243Z_Track_D_D3c1a_public_projection_advisory_fixed-forward.md`, was likewise rejected
> only because Desktop appended a terminal memory-citation annotation to the rollout `response_item`; it is invalid,
> so its printed `NO-GO` is advisory only. Its seven actionable findings are fixed-forwarded in §3.2; `R-019` remains
> open. Section 9 distinguishes the original
> `4cfd1916da8bd98483d1ecfdba1f66639b122da9` evidence from the current follow-up evidence. Fresh validation is
> recorded. Commit `491999040d163ef9c49e707fb830cdce319b7c2a` subsequently received two fresh pinned
> medium-effort non-author advisory reviews; their findings and current fixed-forward are recorded in §3.3. A new
> pinned re-review for the current follow-up remains pending, and no medium-effort result is a formal highest-effort
> verdict.

## 1. Outcome and bounded impact

D3c1a extends the D3c1 command projection seal to the complete public Activity spine, closes normalized private-field
aliases, prevents generic carriers from publishing caller-supplied execution summaries, and makes the two future
`BIGINT` diagnostics lossless within the JavaScript number wire contract. Precommit adversarial author audits further
closed structured Activity artifact refs, cancel/retry/resume Activity carriers, Activity carriers nested recursively
inside command results, cross-language mathematical-integer canonicalization, typed value-family parity, recursive
command closure inside direct Activity evidence, and hazardous-object-key rejection. Later passes closed the complete Operation action/event/run and registry mirror families,
typed trusted execution summaries, normalized response-envelope aliases, and demo `raw`/provenance views. The batch
also replaces path-filtered review checks with explicit route, owner, alias-dataflow, mutation, schema, adapter, and
demo gates. The `20260714T215839Z` advisory follow-up adds the exact-built-in bounded copier, one-budget traversal in
each backend/frontend boundary, owner-separated current-evidence provenance, strict envelope/outcome and
numeric/policy families, and the constructor-only `CommandKernel` lifecycle described in §3.1. The later
`20260714T231243Z` advisory is closed by one canonical frontend outcome/budget manifest, action-specific client
outcome validation, once-captured canonical envelope members, one shared traversal across every wrapper child, a
bounded/cycle-safe demo projector, an exact-empty operation-sync source bit, and bounded batch Activity/Command
provenance reads described in §3.2.

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

## 3. Invalid D3c1a advisory and author-audit closure

### 3.1 `20260714T215839Z` invalid-artifact advisory fixed-forward

The runner appended `INVALID_REVIEW_ARTIFACT` because the persisted rollout response and extracted raw output were not
exactly equal (`final_response_item_exact=false`). The substantive findings are useful adversarial input, but their
printed final `NO-GO` is not formal evidence. Its advisory inventory was `P0/P1/P2/P3=0/3/7/1`: #3 is the accepted
`R-019` residual, while #1/#2/#4-#11 are ten new fixed-forward inputs. The current follow-up maps them as follows:

| Advisory finding | Current fixed-forward mechanism |
| --- | --- |
| #1 hostile non-string/stateful keys | `_sanitize_workflow_command_public_mirror(...)` accepts only exact built-in JSON scalar/container types and exact built-in string keys before any normalization; subclasses and arbitrary objects are omitted without invoking caller hooks. Final control envelopes perform every canonical lookup against that copied source, so colliding hostile keys cannot trigger or impersonate `status`. |
| #2 repeated frontend reprojection | The public adapter carries one `WorkflowPublicProjectionTraversal` through command, Activity, policy, summary, and carrier mapping; depth/node/collection budgets, active-container detection, memoization, and closed-container markers ensure a mapped subtree is not recursively projected again. |
| #4 cycles, hostile containers, and unbounded iteration | The exact-built-in copier and one non-reentrant backend command/Activity carrier traversal share explicit depth, total-node, per-collection, and active-container budgets. Alternating command-to-Activity chains are linear, and cycles, malformed values, or over-budget carrier members are omitted locally without leaving phantom empty carriers. |
| #5 loose workflow/Operation envelopes | Backend envelope helpers first copy the source into the exact-built-in trust domain, then reproject every canonical command, operation-sync, action, run, parent-run, event, policy/state/display, boolean, and status member through its typed owner. Malformed status uses a non-overridable fail-closed reason; the exact `operation_sync={}` replay sentinel remains distinct from malformed non-objects. Adapter/demo wrappers reject malformed canonical response members instead of repairing them from aliases. |
| #6 source-fill Activity provenance | Source-supplied derived fields are removed first. Exact current ActivityRun evidence independently owns descriptor `activity_type`/`owner`, including when the linked command is missing; an exact linked current WorkflowCommand independently owns `control_target`. Planned downstream Activity owners need not equal the source-command owner, while missing/mismatched evidence omits only the fields whose owner cannot be proven. |
| #7 arbitrary nonempty response status | Contract constants define endpoint/action-specific success or applied-outcome sets; adapter and demo require those exact outcomes for action decisions, provenance, Operation controls, and command cancel/retry/resume, so conflict/not-found/approval/unknown values cannot be treated as applied work. |
| #8 loose count maps | JSON Schema now owns `$defs.NumberRecord`, TypeScript owns `NumberRecord = Record<string, number>`, and the command status plus four execution-summary count maps use that shared numeric-only family end to end. |
| #9 policy-field inventory drift | `WorkflowCommandControlPolicy.to_record()` and the three producer-owned field-family sets in `durable_runtime.py` are canonical; backend projection, schema, TypeScript, adapter, demo, and mechanical tests mirror those string/string-array/boolean families rather than accepting producer fields as untyped extensions. |
| #10 lazy kernel repair path | `SourcingOrchestrator.__init__` is the only production construction path for `CommandKernel`; there is no lazy getter or partial-object repair. Characterization fixtures that bypass `__init__` must inject `_command_kernel` explicitly, and missing injection fails visibly. |
| #11 stale review wording | Track D docs distinguish recorded author evidence from review evidence and name `20260714T215839Z` as invalid/advisory; they do not promote its substantive text to formal `NO-GO` or `GO`. |

Finding #3 remains `R-019`; this projection follow-up does not implement its generation/token/CAS or fixed-UoW runtime
requirements. The mechanisms above are present in the current fixed-forward tree; fresh author and scope-local
advisory evidence is recorded in §9. A scope-matched pinned formal verdict remains pending.

### 3.2 `20260714T231243Z` invalid-artifact advisory fixed-forward

The second runner attempt completed with `reviewer_exit_code=0`, but the persisted final `response_item` carried a
Desktop-owned terminal `<oai-mem-citation>` suffix that was absent from the exact raw reviewer output, rollout
`agent_message`, and `task_complete.last_agent_message`. The old verifier therefore set
`final_response_item_exact=false` and rejected the artifact. Its substantive counts were
`P0/P1/P2/P3=0/4/4/1`, including accepted residual `R-019`; neither those counts nor the printed `NO-GO` are a formal
verdict. The actionable evidence is fixed-forwarded as follows:

| Advisory finding | Current fixed-forward mechanism |
| --- | --- |
| action methods accepted another action's successful outcome | `frontend_api_runtime_contract.ts` is the sole outcome-policy owner. All eleven client methods select their exact action/decision outcome set; adapter and demo tests exercise the complete **11 client methods × 9 action outcomes** and **9 demo actions × 9 outcomes** matrices. |
| response wrappers reset collection/node budgets and reread canonical members | Every wrapper captures each own canonical member once, excludes it from the open envelope, and maps it with the same traversal used by the envelope. Exact/over-limit, total-node, depth, cycle, hostile array/Proxy/getter, and capture-once fixtures run against the adapter. |
| the demo projector was unbounded and cycle-unsafe | The demo imports the same canonical limits and owns an equivalent depth/node/collection, active-container, memo, and closed-container traversal. The same adversarial budget/cycle fixtures run through the actual demo bundle. |
| the demo duplicated endpoint outcome policy | Outcome values, action maps, and projection budgets are imported from one runtime manifest and mechanically compared; the demo no longer declares a second table. |
| Activity provenance introduced hosted-PG N+1 reads | Public pages use one bounded request-ordered ActivityRun batch read and one WorkflowCommand batch read for at most 500 raw input identifiers, with zero point reads. Duplicates inside that bounded input are deduplicated before SQL, Activity list reuses its selected rows, empty input issues zero SQL, and missing rows stay absent. Real-PG exact-500 and adapter query-spy coverage proves one `select_many` per required owner. Compatibility point reads exist only when an incomplete test double omits the batch method. |
| malformed operation sync could impersonate `{}` replay | The backend records whether the original exact built-in `operation_sync` member was truly an empty dict. A nonempty value that projects empty is omitted; only the genuine empty source preserves the no-sync replay sentinel. Hostile-key and partially valid fixtures prove the distinction. |
| canonical Activity provenance was reread from mutable input | Canonical Activity values are captured once and projected only from that snapshot under the shared traversal; getter/Proxy fixtures prove no second read. |
| validation wording was stale | This document, the original D3c1 baseline, Plan, TODO, ledger, and index now distinguish recorded author evidence, invalid/advisory artifacts, and the still-pending highest-effort pinned review. |

The stable-tree precommit audit then found and closed four second-order integration gaps without broadening the batch:

1. the adapter now preserves `operation_sync={}` only when the captured source member was itself the exact empty plain
   object; a nonempty member that projects to empty is omitted, while a partially valid member keeps only its valid
   fields;
2. every memoized DAG alias occurrence is charged by the serialized projected node population, so repeating one shared
   large subtree cannot amplify the adapter or demo output beyond the shared total-node budget;
3. Activity-derived provenance now requires exact nonempty `workspace_id` and `workflow_run_id` lineage, exact optional
   Operation/acquisition lineage when supplied, and exact ActivityRun-to-WorkflowCommand run lineage before either a
   control target is queried or attached; foreign/missing evidence stays absent;
4. demo outcomes are compared as exact strings, so whitespace-wrapped enum values remain fail closed exactly as they
   do in the adapter and JSON contract.

These are author-audit findings, not findings retroactively attributed to either invalid artifact. The final dirty-tree
non-author audit reports `P0/P1/P2/P3=0/0/0/0`, but it is advisory and does not replace a pinned formal review.

The review runner transport incompatibility was first addressed by
`4dac471ec18197259dc65a8e3d8759a369bbefe0`; its pinned advisory found two P2 and two P3 edge cases. Commits
`7010fa49ecee0d1f69c28bad4bb762853d459426` and
`eea26e3a71245b91a19021ac13325de6a3b7b2f3` fix-forward suffix separation, unique final-response observation,
canonical Desktop source/thread gating, lowercase UUIDs, and transport-added prompt context. A fresh pinned non-author
review of exact range `4dac471..eea26e3` is advisory `GO`, `P0/P1/P2/P3=0/0/0/0`; the operator-owned effort was
`medium`, so this is not a formal gate artifact. The app-server final item, rollout `agent_message`, and
`task_complete` remain exact raw-output bindings. Runner evidence is separate from this product scope.

### 3.3 Valid pinned advisory findings against `4919990` and fixed-forward

Two independent read-only reviews pinned base `045267d...` and head
`491999040d163ef9c49e707fb830cdce319b7c2a`; neither read the mutable working tree. The backend review returned
medium-effort **ADVISORY NO-GO** with P0/P1/P2/P3=`0/0/2/1`; the frontend review returned medium-effort
**ADVISORY NO-GO** with P0/P1/P2/P3=`0/0/5/2`. They are legitimate scope-local findings, but medium effort is not the
operator-owned highest-effort formal gate.

| Pinned finding | Current fixed-forward |
| --- | --- |
| Backend evidence called a two-row PG probe a 500-ID proof | A real isolated-PG test now creates and projects exactly 500 ActivityRuns plus 500 linked WorkflowCommands and asserts exactly one 500-parameter `select_many` call per owner, zero point reads, and 500 complete projections. |
| Acquisition lineage and ActivityRun-to-WorkflowCommand operation lineage lacked independent mismatch controls | Separate positive-control/mismatch tests prove acquisition mismatch removes all Activity-derived provenance, while command-operation mismatch preserves independently owned Activity type/owner but omits only the foreign control target. |
| Documentation described 500 unique IDs although the guard bounds raw input before dedupe | The contract now says at most 500 raw input identifiers; duplicates inside that bounded population are deduplicated before SQL. |
| Projection budgets omitted key, string, occurrence-byte, and transport-body bounds | One runtime manifest adds UTF-8 key/string, serialized occurrence, and response-body limits. Adapter and demo charge keys/primitives plus memoized subtree node/byte weight, reject a trustworthy oversized `Content-Length` before body read, and reject the decoded body by UTF-8 size. Arbitrarily large valid decimal headers are compared without unsafe-number coercion. |
| Memoized aliases rescanned subtrees and could amplify output | Each memo entry stores projected node/byte weight; every alias consumes that weight in O(1), and the first failed admission permanently blocks later occurrences in the same monotonic traversal. |
| Demo DTO serialized both derived fields and a duplicate enumerable `raw` tree | Derived records retain direct `raw` access for current callers, but attach it as a non-enumerable immutable property; JSON serialization therefore emits one bounded tree. |
| `Object.entries` invoked accessors before filtering and width admission | Descriptor-first capture checks total own-key width, key byte size, privacy/hazard rules, enumerability, and data-descriptor shape before reading a value. Getter and hostile array-length traps execute zero times. |
| Budget exhaustion emitted phantom empty list records | Command/Activity/Operation list projectors omit projected records with no serializable field; node/byte exhaustion cannot append `{}`. |
| Runtime action outcome checks were exact but adapter return types remained broad | All eleven `SourcingAgentApiClient` action/control methods expose action-specific status literal unions; demo approve/reject return decision-specific result types. Positive and `@ts-expect-error` assignments compile in a checked TypeScript fixture. |
| Operation-sync null and own-key-hiding Proxy could collapse to the exact-empty replay sentinel | `null` is rejected, a hostile Proxy cannot establish exact emptiness, and only a structured-cloneable exact empty plain object preserves `{}`; malformed/nonempty projected-empty values are omitted. |

When `Content-Length` is absent or malformed, Fetch still exposes only `response.text()` in the current abstraction, so
the 4 MiB UTF-8 check occurs after buffering. A true read-before-allocation cap requires a separate bounded streaming
reader and remains an explicit transport residual; it does not weaken projection/output admission after decoding.

### 3.4 Earlier precommit adversarial author-audit fixes

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
   ActivityRun descriptor provenance and WorkflowCommand control-target provenance are verified as separate owner
   domains; a planned downstream Activity may legitimately differ from its source command.
3. **Recursive command-result carrier closure.** The generic carrier walker recognizes eight canonical snake carrier keys
   plus their normalized camel/hyphen/space variants
   at every dictionary/list depth: `workflow_activity`, `workflow_activity_run`, `workflow_activity_attempt`,
   `workflow_entity_delta`, `workflow_activities`, `workflow_activity_runs`, `workflow_activity_attempts`, and
   `workflow_entity_deltas`. This includes `workflow_command.result.workflow_activity_run`. Nested Activity records
   receive their exact projector instead of only private-alias redaction, so unknown future columns cannot survive
   through a command result. Backend and frontend each use one non-reentrant bounded carrier traversal; an already
   projected subtree is never sent through the other carrier walker again.
4. **Mathematical safe-integer canonicalization.** JSON and JavaScript do not distinguish an integer-valued `1.0` from
   `1`, and JavaScript can preserve a negative-zero representation. The cross-language rule therefore accepts bounded
   mathematical integers and emits one canonical integer representation: `1.0 -> 1` and `-0.0`/`-0 -> 0`; booleans,
   fractions, negatives, strings, non-finite values, and overflow remain omitted.
5. **Kernel lifecycle, later corrected by advisory #10.** The precommit candidate added partial-construction
   compatibility, but the `20260714T215839Z` advisory correctly identified it as a lazy production repair path. The
   current follow-up instead keeps one constructor-owned `CommandKernel`; characterization paths that bypass
   `SourcingOrchestrator.__init__` must inject `_command_kernel` explicitly. There is no lazy property, silent repair,
   second owner, singleton, runtime fallback, or authorization path.
6. **Recursive workflow-command carrier closure.** Canonical and normalized singular/plural workflow-command carriers
   nested inside any descriptor JSON field now re-enter the same 42-field command projector at every depth. Operation
   sync and each direct ActivityRun/ActivityAttempt/EntityDelta projector use the combined Activity-plus-command
   walker. Normalized caller-supplied `execution_summary` values are removed recursively; only a direct trusted
   command-record root may retain the owner-derived summary before generic carrier projection removes it again.
   Budget cutoff omits the offending singular/list member rather than publishing an empty canonical carrier.
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
values. The lifecycle gate proves constructor-only production ownership and requires partially constructed test
fixtures to inject `_command_kernel` explicitly; absence fails visibly rather than materializing a lazy replacement.

## 9. Acceptance evidence and review status

Author evidence recorded for pinned commit `4cfd1916da8bd98483d1ecfdba1f66639b122da9`, before the
`20260714T215839Z` #1/#2/#4-#11 follow-up:

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

Current follow-up stable-tree evidence, before creating the enclosing commit:

- D3c1a projection contract: **18 passed**;
- D3a + D3b + D3c1a plus three exact durable-runtime adjacency nodes: **63 passed**;
- full PG-backed operation runtime: **129 passed**;
- pre-Agent contract suite: **60 passed**;
- cancel/resume dispatch plus request-scope owner fencing adjacency: **51 passed**;
- frontend production build: **81 modules transformed**; the existing `>500 kB` chunk warning remains informational;
- public mapper inventory: **85 calls / 34 calling functions**, including the two final envelope reprojections;
- scoped Ruff check and format: clean; `git diff --check`: clean;
- mypy ceiling: unchanged at **81 errors in 4 files**;
- fresh dirty-tree non-author backend advisory: **CLEAN**, `P0/P1/P2/P3=0/0/0/0`.

Second invalid-artifact fixed-forward focused evidence, before creating its enclosing commit:

- full D3c1a projection contract: **18 passed**;
- D3c1 claim-fence/static contract: **38 passed**;
- exact backend sentinel/batch/provenance matrix: **6 passed, 3 subtests passed**;
- real-PG batch provenance proof: **2 passed**, including exactly one ActivityRun and one WorkflowCommand
  `select_many` for the two required owners and zero point reads;
- storage surface guardrails: **60 passed**;
- full pre-Agent contract suite: **60 passed**;
- standalone adapter/contract/runtime-manifest TypeScript compile: exit **0**;
- frontend production build: **82 modules transformed**; the existing `>500 kB` warning remains informational;
- scoped Ruff check/format and `git diff --check`: clean;
- mypy ceiling: unchanged at **81 errors in 4 files**;
- stable-tree non-author audit: advisory **GO/CLEAN**, `P0/P1/P2/P3=0/0/0/0`.

Current `4919990` pinned-review fixed-forward evidence, before creating its enclosing commit:

- combined D3 claim-fence + D3c1a projection + pre-Agent contract lane: **119 passed**;
- D3c1a projection contract alone: **19 passed**, including the focused TypeScript literal-status compile gate;
- isolated real-PG evidence matrix: **6 passed + 500 subtests**, including exact 500-row/two-owner projection,
  request-ordered dedupe/bounds, acquisition mismatch, command-operation mismatch, and lineage controls;
- 501-identifier storage guard: **1 passed**;
- frontend production build: **82 modules transformed**, `547.08 kB` / gzip `163.06 kB`; the existing `>500 kB`
  chunk warning remains informational;
- scoped Ruff check/format and `git diff --check`: clean;
- mypy ceiling: unchanged at **81 errors in 4 files**.

The advisory also exercised hostile hash-collision keys with zero equality-hook calls, alternating carrier depth 40 in
9 command projections, and a binary depth-10 carrier tree in 99 projections; budget cutoffs omitted the member rather
than emitting an empty canonical carrier. This is author/advisory evidence, not a formal pinned verdict.

The earlier D3c1 review artifact remains invalid formal evidence even though its five direct findings motivated the
first D3c1a candidate. The later pinned D3c1a artifacts `20260714T215839Z_*` and `20260714T231243Z_*` also remain
invalid: both processes completed, but `causal_binding.final_response_item_exact=false`. The first artifact's
substantive #1/#2/#4-#11 findings are mapped to §3.1; the second artifact's actionable findings are mapped to §3.2;
accepted residual `R-019` remains open. No invalid artifact is formal `NO-GO` or `GO`. Fresh targeted validation is
recorded above. The two valid medium-effort reviews of `4919990` are advisory `NO-GO` inputs and are fixed-forwarded
in §3.3; a fresh pinned re-review must bind the enclosing commit. A formal review still requires the operator-owned
highest supported reasoning effort. Until a valid formal scope-matched artifact exists, live/W6/manual validation,
promotion, and milestone signoff remain fail closed for this scope.

## 10. Explicit non-closure

D3c1a closes only the public-projection findings. It does not activate D3c2a's dormant migration foundation, complete
full Migration A, or implement scoped review-session issuance, bootstrap or strict-D3 manifests,
ClaimAuthority/ClaimReceipt mint and verification, Stage A/B, generation/token/epoch CAS, heartbeat occurrence,
business fencing, terminal provenance, late-result quarantine, dispatch coordination, action-root durable scope, or
any Plan §6/OB-ID obligation.

R-019 remains open. Served Agent tool population remains zero. No fake, scripted, or live provider/model path is
authorized by this batch, and no provider-costing validation or product handoff may infer authorization from a closed
public mirror or a safe diagnostic value.
