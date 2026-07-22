# Track D D1n S1b — `inspect_operation` physical-owner result adapter

> Status: Implementation/decision record (Track D increment). Review state and current authority are routed via docs/INDEX.md Tier 3; scheduled for module distribution (reorg R3+).

Date: 2026-07-17

Status: third fixed-forward author response active. The first bundled Ultra artifact remains reference-only
`NO-GO 0/4/6/0`; the correctly scoped fresh Ultra review of `4dddd0e..28c2b2e` is valid and returned
`NO-GO 0/6/1/0` plus the accepted R-019 residual. This response addresses all seven new findings with explicit
version pins, canonical policy/topology revalidation, actor/source evidence, pre-connection binding, bounded reason
text, and frozen historical replay fixtures. R-019 remains open. S1d is a separate successor batch. A fresh pinned
review of this response is still required. This is not a formal `GO`, does not populate the default Agent registry,
and does not authorize a provider or model call.

A subsequent bounded audit found that the first response named only the three generic control reason constants while
two production producer families also wrote `progress.reason`. The fixed-forward follow-up therefore installs one
exact eight-entry owner/phase/code registry for runtime controls, projection reselection, and CRM approval-required
transitions. Producers validate before their first effect and the repository independently validates every explicit
reason replacement; reason-omitting patches leave brownfield values untouched. The retained v1 fixture also carries
a frozen reason-bearing hash probe generated with the historical `18c7583` serializer blob.

Registry codes are globally unique, and the phase in each triple is the mint-time authorization context. Later
phase-only progress patches retain the last registered code as transition provenance without reminting it under the
new phase; current phase/control must never be inferred from that retained reason.

## Impact

S1b makes the V3 `inspect_operation` query executable against production-shaped PostgreSQL state and persistable in
the S1a exact result slot. The query result is no longer supplied as an unattached snapshot fixture. It is rebuilt
from one exact physical owner set:

```text
workspace + AgentAction + OperationRun
-> complete, contiguous Operation event stream
-> strict workflow_ref + exact WorkflowCommand causal identity
-> one unambiguous OperationCommandPlanned event + full payload digest
-> canonical control/display/progress/result-readiness projection
-> non-model physical-owner fingerprint + revision-bound serialized result
```

The event sequence is the monotonic revision anchor. The adapter locks the complete physical stream by stream id and
rejects any foreign workspace/action/run/family/schema row or non-contiguous sequence topology rather than hiding it
with a scoped event query. Its non-model fingerprint binds the strict four-field workflow ref, WorkflowCommand causal
identity, complete event identities, selected plan-event id/sequence, and complete plan payload digest. A result
prepared at revision N is accepted only if the same rows still rebuild the same terminal at acceptance; otherwise the
slot stays pending and no attempt or journal is written.

For a missing or foreign Action/Operation tuple, the same adapter returns the byte-identical public
`operation_not_found` result through a versioned, slot-generation-anchored masked-error owner. Acceptance rechecks the
scoped absence under the same locks and then uses the normal attempt -> slot CAS -> journal transaction. It does not
throw an owner-specific exception, reveal whether a foreign row exists, or leave a reserved slot permanently pending.

The first response fixed-forwarded readiness to v2. This response retains the exact v1 and v2 query/result/tool/
fixture manifests and pins an explicit v3 physical contract; the unversioned symbol is only a current facade and
persisted occurrences resolve by exact historical identity rather than list order or that facade. Frozen V1 and V2
JSON/SHA fixtures come from named historical Git objects, and the accepted V2 fixture replays through the current
public acceptance path without invoking the head prepare implementation. V3 removes operator-authored
`progress.reason` from the model-visible result, while its physical fingerprint binds the complete raw progress plus
an audit-event digest containing exact actor/source evidence. A single canonical projector rederives every
command-control policy field from the durable owner registry; a full-stream topology check permits exactly zero or
one command and requires the unique exact plan proof when referenced. The request is bound once before dependency
routing, schema access, connection acquisition, or owner/result effects and the same immutable binding is reused
under locks. OperationRun progress stores canonical machine codes for cancel/retry/resume; trimmed operator text is
accepted only up to 500 characters before any runtime-writer repository read or write and remains append-only event audit
evidence. Result readiness remains centralized: completed without a non-empty durable `result_ref` is
`pending/fail_closed`, never inferred `ready` or `not_applicable`. The request remains v1 because its shape and
semantics did not change.

## Exact implementation

- `agent_operation_query_postgres.py` owns the physical query adapter:
  - validates the exact isolated-canary `inspect_operation` tool/request/result/serializer pins;
  - takes the event-stream advisory fence, then locks scoped OperationRun, scoped AgentAction, only the exact
    WorkflowCommand referenced by `OperationRun.workflow_ref`, and the complete event stream before the result slot;
  - requires exact workspace/action/run ownership and ActionRegistry owner/type parity;
  - accepts only `{}` or an exact four-field, typed, unpadded `workflow_ref`; requires workflow run/id/type/owner parity
    across that ref, the exact command row, the linked Action contract, the durable command-owner registry, and one
    unambiguous same-command `OperationCommandPlanned` event;
  - binds command stage/group/parent/source/idempotency/schema causality plus typed artifact/count/downstream/payload/
    retry-policy digests and full event identities into the historical v2 or current v3 physical fingerprint;
  - deliberately excludes unrelated/downstream rows that merely share `operation_id`, so they cannot become phantom
    members of this query owner;
  - projects `operation_run_control_state`, durable command control policy, ActionRegistry display contract,
    Operation progress/result readiness, and bounded provenance;
  - exact-resolves the occurrence's retained result contract, reruns that version's validator/serializer, and binds
    the output to the latest event id/sequence plus snapshot digest; v3 additionally binds raw progress.
- `agent_projection_query.py` retains exact v1/v2 result contracts and exposes v3 as the explicit current alias. The
  v3 named serializer rechecks phase, canonical control-state owner output, disabled-reason values, readiness,
  policy, and event/command provenance rather than relying on schema shape alone. Its shared policy projector accepts
  only exact registered command/owner pairs and returns the complete canonical 10-field available or three-field
  commandless projection.
- `operation_runtime.py` owns the reusable canonical control-state projection validator and the immutable
  `OPERATION_PROGRESS_REASON_REGISTRY`. It binds the exact runtime-control, projection-reselection, and CRM-approval
  owner/phase/code triples; forged owners, phases, codes, and string subclasses fail closed. Cancel/retry/resume
  preserve operator text only in the corresponding append-only event payload, bounded to 500 characters before the
  first runtime-writer repository read. Repository writes validate only explicit new/replaced reason fields, so
  retained brownfield rows are not rewritten merely because another progress field changes.
- `agent_canary_registry.py` retains byte-identical inspect tool v1/v2 history and exposes an explicit v3 tool spec
  that pins the explicit v3 result spec. The unversioned symbol aliases that object only as a public/current facade.
  The registry remains four names/seven historical specs and does not change public/default population.
- `agent_tool_result_postgres.py` now has one shared pending-to-accepted state machine. Both the existing
  `plan_acquisition` adapter and S1b reuse its generation quarantine, late-winner quarantine, exact replay, fault
  rollback, slot CAS, and append-only journal behavior.
- `prepare_inspect_operation_tool_result` is a read-only owner step. It returns a typed terminal proposal, performs no
  result-slot or domain write, and does not bootstrap or mutate table schemas.
- `accept_inspect_operation_tool_result_uow` re-reads and reserializes the owner inside the acceptance transaction
  before the accepted attempt, slot CAS, or journal can be written. Its closed union accepts only the exact success
  owner or the exact `operation_not_found` masked-error owner; arbitrary `is_error` terminals remain rejected.
- repository and live-PG adapter entrypoints expose the same exact contract to the later local Agent loop.

Acceptance lock order is:

```text
Operation event-stream advisory
-> scoped OperationRun -> scoped AgentAction
-> exact workflow_ref command -> complete operation event stream
-> result slot
-> accepted attempt -> slot CAS -> journal
```

No control action, repair, provider/model call, projection mutation, Action/Operation transition, or public release
transition occurs.

## Verified behavior

- commandless completed Operation: exact success result, `not_applicable` command policy, ready result reference;
- command-backed running Operation: registered durable command policy and pending result readiness;
- completed Operation without a durable result reference remains `pending/fail_closed` in the physical PG path;
- forged readiness status, owner, fallback, and direct-serializer projections fail closed;
- command-owner drift fails closed before a terminal row is written;
- foreign workflow-run and mismatched planned-event links fail closed before terminal writes;
- an unreferenced row sharing `operation_id` remains outside the exact query owner and cannot change its provenance;
- prepare performs no write-schema bootstrap;
- prepare/accept exact replay including commit-before-ack recovery;
- Action, Operation, and Operation-event rows remain byte-equivalent across accepted query-result persistence;
- event revision drift between prepare and acceptance leaves the slot pending with zero attempt/journal writes;
- a foreign-workspace row in the same event stream, wrong event action/run/schema, or sequence gap/start fails closed
  with a pending slot and zero attempt/journal writes;
- duplicate or conflicting same-command plan proofs, in-place plan-payload mutation, coordinated workflow lineage drift,
  and command causal-identity drift fail closed even without a revision advance;
- empty/null/list/invalid/partial/numeric/padded/unknown/extra workflow refs and malformed plan identity fields fail
  closed; legal plan payload variant fields remain supported and digest-bound;
- missing and foreign owner tuples produce byte-identical model-visible `operation_not_found`, atomically persist
  `is_error=true`, replay after a lost acknowledgement, and leave Action/Operation/event domain rows unchanged;
- a masked error whose exact owner appears before acceptance is rejected, while forged success/error union shapes and
  all three pre-commit fault points leave zero partial result rows;
- a forged historical tool/request/result/serializer identity fails before owner read;
- exact retained v1 and v2 occurrences prepare, accept, and lost-ack replay with their own serializer/fingerprint;
- frozen V1/V2 fixture bytes and SHA digests are produced by named historical Git blobs, and the accepted V2 terminal
  replays without calling the current prepare implementation;
- a V1 legal machine-reason probe generated by the historical `18c7583` serializer remains byte/hash identical under
  the retained current v1 serializer;
- a synthetic future V4 plus reordered registry cannot reinterpret the retained V3 physical contract;
- mixed tool/result history and unknown digests fail before database connection with zero result writes;
- v3 omits Unicode/operator-authored reason text, but a raw-reason-only drift after prepare leaves the slot pending
  with zero attempt/journal writes;
- V3 event actor/source drift changes the audit fingerprint and invalid actor/source fails before result writes;
- forged or incomplete control-policy fields, impossible command/event cardinality, commandless plan phases/events,
  and truncated provenance fail closed against one canonical projector and the complete event stream;
- malformed or padded request identities fail before dependency routing, schema access, connection acquisition, or
  any owner/result effect; the immutable request binding is reused under the owner locks;
- cancel/retry/resume accept exactly 500 trimmed characters and reject 501 before a runtime-writer repository effect;
  the API returns an explicit invalid response while Action, OperationRun, child-run population, and event streams
  remain unchanged;
- impossible terminal control flags and forged disabled-reason values fail the shared canonical control-state check;
- S1a `plan_acquisition` concurrency/quarantine/replay behavior remains green through the shared-core refactor.

## Deliberate remaining boundary

S1 still needs two canary owner adapters:

- `start_acquisition_run`: confirmation/start UoW, exact WorkflowCommand and Activity winner;
- `filter_projection`: exact projection publication and Cohort membership result.

The filter adapter cannot safely reuse the existing integer-only target revision: canonical
`membership_revision` is an opaque equality token and must not be parsed, ordered, or hashed into an invented
integer. S1c fixed-forwards the storage/result aggregate with an explicit equality-only token while preserving the
numeric revision/generation carriers. That carrier is foundation only: the next filter batch must still load and lock
the exact projection publication/membership owner, copy its canonical token, reserialize inside the acceptance UoW,
and prove mismatch and foreign/missing zero writes.

Default/public serving remains `0`; scripted model-loop assembly and live provider/model work remain blocked on their
separate gates.

## Fresh pinned review scope

The first bundled Ultra artifact is reference-only because its comma-joined scope list bound no actual file bytes.
The later exact review artifact
`runtime/reviews/20260717T143711Z_Track-D-D1n-S1b-versioned-inspect-contract-closure.md` validly binds
`4dddd0e93634fcb4920f3f15a8c1984ca239a6f3..28c2b2eb6282462fa5e1a7003590473119e4a30e` and is a
`NO-GO 0/6/1/0`, not signoff. The current response base is
`f4f3e58ddce3342f8f7e2e0b88c30898f510fcb1`; the head is the eventual standalone response commit. Exact intended
scope:

- `docs/AGENT_OPERATION_CONTRACT.md`
- `docs/NEXT_TODO.md`
- `docs/PRE_AGENT_CONTRACT_REVIEW.md`
- `docs/TRACK_D_D1N_REMAINING_ACTION_AND_AGENT_TOOL_SERVING_PLAN.md`
- `docs/TRACK_D_D1N_S1B_INSPECT_OPERATION_RESULT_IMPLEMENTATION.md`
- `docs/TRACK_D_D1N_V3_PROJECTION_QUERY_IMPLEMENTATION.md`
- `src/sourcing_agent/agent_canary_registry.py`
- `src/sourcing_agent/agent_operation_query_postgres.py`
- `src/sourcing_agent/agent_projection_query.py`
- `src/sourcing_agent/agent_tool_result_postgres.py`
- `src/sourcing_agent/operation_runtime.py`
- `src/sourcing_agent/orchestrator.py`
- `tests/fixtures/d1n/inspect_operation_result_v1.json`
- `tests/fixtures/d1n/inspect_operation_result_v2_accepted.json`
- `tests/test_d1n_canary_agent_tool_population.py`
- `tests/test_d1n_inspect_operation_result_slot_uow.py`
- `tests/test_d1n_inspect_operation_historical_fixtures.py`
- `tests/test_d1n_projection_query_contracts.py`
- `tests/test_operation_runtime.py`
- `tests/test_pre_agent_contract_review.py`

That response was committed as `f8a72c75b299825d3c14584751a1586adb51dff7`. Two fresh canonical executions on
2026-07-17 produced only fail-closed invalid artifacts: the first stopped during transport initialization; the second
read an obsolete shared model-cache schema. A filtered-cache diagnostic then proved the dedicated reviewer can
initialize with `gpt-5.6-sol / ultra / priority`, but the server returned `usageLimitExceeded`. None of those attempts
is a GO/NO-GO review artifact. The S1b response review therefore remains pending, while the bounded registry/history
follow-up is a separate direct-child diff based at `f8a72c7` and also requires fresh pinned review after commit.

## Author validation evidence

Latest response run from the repository root with no live provider/model variables:

```text
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src SOURCING_REQUIRE_PG_STORE_TESTS=1 \
  .venv/bin/python -m pytest -q tests/test_d1n_inspect_operation_result_slot_uow.py
=> 38 passed + 43 subtests

PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src SOURCING_REQUIRE_PG_STORE_TESTS=1 \
  .venv/bin/python -m pytest -q --tb=short \
  tests/test_d1n_action_contract_activation.py \
  tests/test_d1n_action_contract_identity.py \
  tests/test_d1n_action_result_schema.py \
  tests/test_d1n_acquisition_plan_preview.py \
  tests/test_d1n_acquisition_plan_preview_uow.py \
  tests/test_d1n_start_acquisition_v2.py \
  tests/test_d1n_projection_query_contracts.py \
  tests/test_d1n_agent_tool_registry.py \
  tests/test_d1n_canary_agent_tool_population.py \
  tests/test_d1n_agent_tool_result_slot.py \
  tests/test_d1n_agent_tool_result_slot_uow.py \
  tests/test_d1n_inspect_operation_result_slot_uow.py \
  tests/test_d1n_inspect_operation_historical_fixtures.py \
  tests/test_migration_runner.py \
  tests/test_pre_agent_contract_review.py
=> 839 passed + 127 subtests; includes 945 canonical control-owner input combinations

PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src SOURCING_REQUIRE_PG_STORE_TESTS=1 \
  .venv/bin/python -m pytest -q \
  tests/test_operation_runtime.py::OperationRuntimeTest::test_operation_run_control_state_is_backend_owned \
  tests/test_operation_runtime.py::OperationRuntimeTest::test_cancel_operation_updates_operation_and_action_without_module_side_effects \
  tests/test_operation_runtime.py::OperationRuntimeTest::test_list_provenance_resume_and_retry_stay_inside_operation_runtime \
  tests/test_operation_runtime.py::OperationRuntimeTest::test_operation_control_blank_reasons_keep_canonical_progress_and_blank_audit_payload \
  tests/test_operation_runtime.py::OperationRuntimeTest::test_operation_controls_bound_operator_reason_before_any_runtime_effect
=> 5 passed + 3 subtests

PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src .venv/bin/python -m mypy \
  src/sourcing_agent/agent_canary_registry.py \
  src/sourcing_agent/agent_operation_query_postgres.py \
  src/sourcing_agent/agent_projection_query.py \
  src/sourcing_agent/agent_tool_result_postgres.py \
  src/sourcing_agent/operation_runtime.py \
  tests/test_d1n_canary_agent_tool_population.py \
  tests/test_d1n_inspect_operation_result_slot_uow.py \
  tests/test_d1n_projection_query_contracts.py \
  tests/test_d1n_inspect_operation_historical_fixtures.py
=> success, 0 issues in 9 files

make lint
=> 58 files already formatted; all checks passed

make typecheck
=> existing ceiling unchanged: 81 errors in 4 files

py_compile on all 12 changed Python files
git diff --check
=> clean
```

These are author results, not independent-review evidence.

The bounded progress-reason/history follow-up was then validated with the final full Operation runtime
(`193 passed + 592 subtests`), the full result-slot/inspect/history PG group (`70 passed + 52 subtests`), the full
pre-Agent contract (`62 passed`), and the exact producer/repository matrix (`10 passed + 22 subtests`). Lint and
compilation are clean, while the global mypy ceiling remains exactly `81 errors / 4 files`. These later results are
also author evidence only; the failed/invalid reviewer attempts do not promote them to a formal verdict.
