# Track D D1g — Operation API exact-owner closure

> Status: Fixed-forward author implementation candidate (2026-07-16). This bounded non-live batch remediates R-031 for
> authenticated Operation reads and controls without expanding the action request-schema or served-tool population.
> A review attempt against the earlier candidate terminated without a valid verdict, but supplied three reproducible
> findings: shared-workflow command leakage, malformed foreign-workspace event leakage, and mutable planned-command
> references. Pinned `646e596` advisory then returned `NO-GO 0/0/1/1`: valid planned replay bypassed persisted request
> validation and approval, while post-D1h schema-count wording drifted. Pinned `c7d2e24` then returned
> `NO-GO 0/0/1/0` for a planned approval branch that wrote action/run/event before returning its queued command.
> Commit `ebe7ed0` closes that boundary and has a fresh pinned non-author scope-local **ADVISORY GO 0/0/0/0**;
> neither author evidence nor that advisory is a formal verdict. Formal highest-effort review remains required before
> hosted/live multi-user Operation exposure or product/milestone signoff. No provider, model, or live
> environment is used by this batch.

## 1. Outcome and bounded scope

D1g closes the authenticated Operation IDOR mechanism recorded after D1f. The product-code change is limited to the
HTTP owner-context wiring in `api.py`, the Operation aggregate preflights and response composition in
`orchestrator.py`, the linked-action/event workspace predicates in `repositories/workflow_runtime.py`, and the
linked-operation command predicate in `storage.py`:

- authenticated action/run lists ignore caller workspace overrides and use the server-derived workspace;
- authenticated action/run detail, run provenance, and all existing Operation controls exact-match that workspace
  before any event, run, command, Activity, EntityDelta, or domain mutation;
- an authenticated OperationRun is valid only when both the run and its linked AgentAction exist in the exact same
  workspace;
- authenticated missing and foreign ids use the same generic not-found transport body for each resource kind;
- authenticated status/provenance command reads require each command's physical `operation_id` to resolve through an
  exact-workspace OperationRun and linked AgentAction before SQL `LIMIT` is applied; a caller-reused
  `workflow_run_id` cannot cross that boundary;
- authenticated action/run/timeline event reads exact-filter physical `operation_events.workspace_id` before SQL
  `LIMIT`, including malformed rows whose stream/action ids point at an owned aggregate;
- an authenticated planned CRM/export run exposes or reuses its referenced command only when that command resolves to
  the exact current OperationRun; blank, missing, foreign, and same-workspace-other-run references return generic
  not-found before compatibility observation or any domain/runtime write. A valid reference is captured once, then
  must pass persisted request validation plus adapter target/approval guards before replay is returned; it never
  records a schema-less compatibility write and is not read a second time from mutable `workflow_ref`;
- open-mode operator calls preserve their existing explicit-workspace behavior;
- `GET /api/operations/action-registry` remains a shared registry read rather than a workspace-owned aggregate read.

D1g did not define schemas or binders for the 12 schema-less actions at its checkpoint. D1h reduced that checkpoint to
11 and D1i reduced the current population to 10. D1g does not add an action to the served Agent registry, change D1f's
CRM target contract, add a provider/model path, add a workflow-command type, or migrate storage.

## 2. Canonical owner and caller provenance

The authorization owner is the persisted aggregate workspace, not the request payload and not `actor`:

| resource | canonical owner/source of truth | authenticated authorization rule |
| --- | --- | --- |
| AgentAction | `agent_actions.workspace_id` | exact-equal server-derived workspace |
| OperationRun | `operation_runs.workspace_id` plus linked `agent_actions.workspace_id` | both rows must exist and both exact-equal server-derived workspace |
| action registry | checked-in `ActionRegistry` | shared read; no per-workspace aggregate row |
| actor/source | authenticated request identity / existing open-mode payload | provenance only; never an authorization fallback |

Authenticated request state derives the workspace namespace. Caller-supplied `workspace_id` and `actor` cannot change
the authorization decision: list scope is overwritten, control actor is server-derived provenance, and every id-based
path receives a keyword-only `expected_workspace_id`. The user component does not become a second record owner; the
canonical production owner remains the workspace columns above.

## 3. Route matrix

| surface | authenticated behavior | open-mode compatibility |
| --- | --- | --- |
| `GET /api/operations/action-registry` | shared registry read | unchanged shared read |
| `GET /api/operations/actions` | server exact-workspace list | explicit operator workspace retained |
| `GET /api/operations/runs` | server exact-workspace list; linked foreign/missing action excluded | explicit operator workspace and legacy rows retained |
| action detail | exact AgentAction workspace preflight | existing id lookup retained |
| run detail/provenance | exact OperationRun plus linked AgentAction workspace preflight | existing id lookup retained |
| approve/reject | exact AgentAction workspace preflight | existing operator control retained |
| cancel/retry/resume | exact OperationRun plus linked AgentAction workspace preflight | existing operator control retained |
| dispatch | exact OperationRun plus linked AgentAction workspace preflight; lock-taking branch rechecks under the dispatch lock before the R-029 compatibility event | existing operator dispatch retained |
| nested status/provenance commands | command `operation_id` resolves through exact-workspace run+action at query time; shared workflow aliases cannot widen scope | existing shared-workflow aggregation retained |
| nested action/run/timeline events | physical event workspace exact-matches before query limit | existing unfiltered operator evidence view retained |
| planned CRM/export command reference | invalid ref fails before writes; valid-current ref is captured once, passes request/target/approval validation, then replays without compatibility observation | existing invalid-ref legacy behavior retained; valid replay still passes approval/schema guards |

The production method signatures make `expected_workspace_id` keyword-only. An empty value is the explicit open-mode
compatibility marker; authenticated routes always pass a non-empty server-derived value.

## 4. Fail-closed transport and zero-write boundary

For authenticated action paths, missing and foreign ids both return:

```json
{"status": "not_found", "reason": "operation_action_not_found"}
```

For authenticated run paths, missing, foreign, and owned-run/foreign-linked-action ids all return:

```json
{"status": "not_found", "reason": "operation_run_not_found"}
```

Those bodies are normalized at the HTTP boundary so underlying ids or record-existence details do not escape. The
orchestrator preflight occurs before the existing control writers. The PG regression snapshots action/run/events,
workflow event/current state/commands/outbox, ActivityRun/Attempt, EntityDelta, and CRM domain tables and proves no
change for denied calls. Dispatch branches that take the existing operation dispatch lock re-read the run and linked
action under that lock before schema-less compatibility observation, so an authorization failure cannot append the
R-029 event first.

List protection is server-scoped rather than post-filtered for actions. Authenticated run lists additionally require
the linked action to share the exact workspace, so a malformed cross-workspace link cannot become visible through an
otherwise owned run row. That linked-action predicate is one repository-level SQL `EXISTS`, not per-row lookups or an
application post-filter; this avoids N+1 reads and prevents `limit`/`offset` from being applied before authorization.

The same pre-limit rule now governs nested evidence. `list_workflow_commands(...,
linked_operation_workspace_id=...)` uses one SQL `EXISTS` joining the command's physical `operation_id` through
`operation_runs` and `agent_actions`. Event repository reads add physical workspace clauses before `LIMIT`. These are
authorization predicates, not response-time cleanup: a foreign row cannot consume the authorized result window.

For planned CRM/export dispatch, command-reference validation runs before request-schema compatibility observation.
In authenticated mode the command must exist, carry a nonblank physical operation id, resolve through the same exact
workspace, and name the current run. Failure returns the same run not-found shape with a full zero-write snapshot.
Open mode passes an empty expected workspace and retains legacy invalid-reference behavior. A valid planned replay in
either mode still passes the same persisted request validator and adapter approval/target guards. The captured replay
is passed into the adapter, avoiding a second mutable-reference read and preserving zero compatibility writes for a
valid replay. If an already-planned sensitive CRM action no longer has valid approval, the adapter returns the same
approval requirement from the persisted action/run without writing action, run, event, command, or CRM state; it does
not run the normal first-plan approval writer against a separately claimable command.

## 5. Compatibility and residual boundaries

- R-031 is remediated by `ebe7ed0` and has a fresh pinned non-author scope-local advisory GO, but remains formal-review
  pending. That formal gate blocks hosted/live multi-user Operation exposure and product/milestone
  signoff, not unrelated bounded non-live work.
- R-019 remains open. D1g adds authorization-only read preflights and a lock-internal recheck, but no state mutator,
  no `_connect_with_transaction_lock` caller, and no claim/terminal/UoW implementation. The production state-sync
  caller ratchet remains **26**. The existing event/action/run/command atomicity, generation/lease, and total lock-budget
  obligations are unchanged. Pinned `c7d2e24` advisory re-raised this boundary because the first fixed-forward let a
  captured-plan approval guard call the existing action/run/event writer; the current fixed-forward makes that exact
  replay branch read-only and expands its regression snapshot to all D1g tables.
- R-028 remains open and is not triggered by D1g. No CRM writer or effect boundary changes; D1f's target
  owner/version revalidation and the outstanding command/effect/terminal UoW remain exactly as documented.
- R-029 remained open at **12/15 schema-less** actions at the D1g checkpoint; D1h reduced it to **11/15** and the
  post-D1i current population is **10/15**. D1g
  protects existing Operation reads/controls; it does not claim that schema-less submission is served, reviewed, or
  migration-complete.
- Served Agent tool population remains zero. Fake/scripted or local open-mode testing does not authorize live/provider
  use.

## 6. Verification and review handoff

Run from `sourcing-ai-agent/` with the local PG fixture:

```bash
make local-pg-up
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src .venv/bin/python -m pytest -q \
  tests/test_api_request_scope.py \
  tests/test_d1g_operation_api_exact_owner.py
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src .venv/bin/python -m pytest -q \
  tests/test_operation_runtime.py::OperationRuntimeTest::test_operation_control_rejects_cross_workspace_links_and_event_collisions \
  tests/test_operation_runtime.py::OperationRuntimeTest::test_list_provenance_resume_and_retry_stay_inside_operation_runtime \
  tests/test_operation_runtime.py::OperationRuntimeTest::test_operation_http_api_submit_query_approve_cancel \
  tests/test_storage_surface_guardrails.py::test_operation_state_sync_residual_callers_are_ratcheted
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src .venv/bin/python -m pytest -q tests/test_operation_runtime.py
.venv/bin/ruff check \
  src/sourcing_agent/api.py \
  src/sourcing_agent/orchestrator.py \
  src/sourcing_agent/repositories/workflow_runtime.py \
  tests/test_api_request_scope.py \
  tests/test_d1g_operation_api_exact_owner.py
.venv/bin/ruff format --check \
  src/sourcing_agent/api.py \
  src/sourcing_agent/orchestrator.py \
  src/sourcing_agent/repositories/workflow_runtime.py \
  tests/test_api_request_scope.py \
  tests/test_d1g_operation_api_exact_owner.py
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src .venv/bin/python -m py_compile \
  src/sourcing_agent/api.py \
  src/sourcing_agent/orchestrator.py \
  src/sourcing_agent/repositories/workflow_runtime.py \
  tests/test_api_request_scope.py \
  tests/test_d1g_operation_api_exact_owner.py
make typecheck PYTHON_BIN=.venv/bin/python
git diff --check
```

Evidence confirmed so far on the current candidate tree:

- `tests/test_api_request_scope.py` plus the expanded D1g real-PG matrix: **29 passed + 107 subtests**;
- exact D1g matrix alone: **7 passed + 35 subtests**, including pre-limit shared-workflow command/malformed-event
  probes; blank/missing/foreign/same-workspace-other planned-reference zero-write; and authenticated/open approval,
  schema-invalid, and pin-drift planned positives. The latest approval replay assertion uses the full D1g table
  snapshot after pinned `c7d2e24` advisory=`NO-GO 0/0/1/0`;
- dispatch registry plus two exact characterization adjacency probes: **25 passed**; the stale probe seams exposed by
  `646e596` are updated without weakening the frozen selector contract;
- combined D1 request/schema/activation adjacency: **117 passed + 198 subtests**;
- four exact adjacent Operation nodes: **4 passed** — cross-workspace link/event-collision, list/provenance/resume/retry,
  open-mode HTTP Operation flow, and the R-019 state-sync caller ratchet;
- full `tests/test_operation_runtime.py`: **136 passed + 503 subtests**;
- `make lint`: **58 files**, green;
- global mypy: accepted baseline unchanged at **81 errors / 4 files**.

The full Operation runtime and final diff check are green. Commit `ebe7ed0` fresh pinned non-author review returned
scope-local **ADVISORY GO 0/0/0/0** after the `29+107` matrix, a dedicated full-table diagnostic, regression-sensitivity
check against its parent, lint/format/compile, and SQL predicate audit. It is not a formal `GO`.
