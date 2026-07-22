# Track D D1n S1c — opaque physical-owner revision carrier

> Status: Implementation/decision record (Track D increment). Review state and current authority are routed via docs/INDEX.md Tier 3; scheduled for module distribution (reorg R3+).

Date: 2026-07-17

Status: committed author candidate at `bacae9e80c8c0593b6c8c436b06e68777ade6d54`; fresh pinned review is pending.
This batch is a storage/result-aggregate foundation only. It does
not implement the `filter_projection` physical-owner adapter, populate the public/default Agent registry, mark a tool
served, or authorize a provider/model/live call.

## Engineering goal

The S1a result aggregate currently requires every physical owner to expose an integer revision or generation.
Canonical projection `membership_revision` is instead an opaque equality token. S1c adds one explicit
`owner_target_revision_token` carrier so later adapters can exact-copy that owner value without parsing, ordering, or
hashing it into an invented integer.

The three version carriers are independent physical-owner evidence:

```text
owner_target_revision          numeric monotonic revision when the owner has one
owner_target_generation        numeric generation when the owner has one
owner_target_revision_token    opaque equality-only revision when the owner has one
```

At least one carrier must be present. More than one is permitted only when the physical owner genuinely publishes
each value; the result layer derives none of them and compares every populated value exactly.

## Field ownership contract

- physical value owner/source of truth: the registered membership/publication owner UoW; the future
  `filter_projection` adapter is not a second authority;
- adapter responsibility: lock/reload that exact owner and copy/revalidate the opaque value without transformation;
- carrier writer: the result-acceptance UoW exact-copies the adapter-authorized value into slot, attempt, and journal;
- allowed token form: empty, or one bounded canonical identifier copied byte-for-byte from the owner;
- derivation: forbidden; no integer conversion, ordering, hashing, truncation, case folding, or fallback ladder;
- persisted consumers: accepted/quarantined attempt, accepted slot, immutable journal, repository projection, and
  deferred aggregate validator;
- forbidden consumers: planner/model/UI, caller-supplied payloads, latest-row lookup, ordering/range comparison, and
  any adapter that has not locked and revalidated the registered physical owner;
- failure behavior: all three carriers absent, invalid token shape, replay collision, or slot/attempt/journal mismatch
  fails closed;
- fast-preflight status: current storage tests cover exact copy, collision, and deferred parity; the physical
  membership/publication cross-path preflight does not exist yet and therefore blocks the future adapter, fixture,
  and any served transition;
- migration state: additive `0012`; historical numeric-only rows remain valid and exact;
- activation boundary: token-only writers remain inactive until their physical-owner adapter and scope review land.
- deletion condition: the carrier may be removed only by a future versioned migration after no retained historical or
  active terminal result refers to an opaque owner revision; S1c defines no deletion or fallback date.

## Schema-version and quiesced-rollout contract

`agent_tool_result_slots.schema_version=agent_tool_result_slot_v1` remains the immutable logical-occurrence identity
version. It is included in `logical_occurrence_digest` and therefore is not repurposed or bumped by this additive
terminal carrier. Attempt and journal rows version the terminal payload itself:

- numeric revision/generation with an empty token remains attempt/journal `v1`;
- a populated token is written as attempt/journal `v2`;
- database checks reject a token on `v1`, require at least one carrier on `v2`, and require accepted attempt/journal
  schema parity;
- the database default remains `v1`, preserving numeric-only row semantics after a pool/session recycle;
- applying `0012` itself requires a quiesced cutover and complete pool/session recycling: retained prepared
  `SELECT *`/`RETURNING *` statements may reject the changed row descriptor after DDL, so S1c does not claim rolling
  old/new binary overlap;
- token-only activation additionally requires all old replicas to remain drained. This is currently enforceable
  because the public/default served population is zero.

S1d migration `0013` fixed-forwards another S1c review finding: attempt/journal v2 now requires a nonempty token,
matching the canonical application encoder. Numeric-only terminal payloads are v1; a preexisting noncanonical
numeric-only v2 row makes the quiesced migration fail atomically instead of being silently reinterpreted.

This avoids changing the logical occurrence digest in place and avoids durable rows whose terminal payload version
cannot distinguish historical numeric-only data from the new opaque carrier.

## Implementation

- `AgentToolTerminalResult` validates and transports the optional token through construction, revalidation, and
  record export while preserving numeric-only behavior.
- PostgreSQL row builders and exact replay/collision assertions copy the token through slot, attempt, and journal.
- `0012_agent_tool_result_owner_revision_token.sql` adds the non-null empty-default column to all three tables,
  fixed-forwards their version-presence and attempt/journal version checks, and replaces the deferred aggregate
  validator with exact token and terminal-schema equality checks. Migration `0011` remains immutable.
- repository table descriptors expose the carrier without adding a parallel fallback or derived alias.

No lock order, occurrence identity, generation quarantine, late-winner quarantine, result-slot CAS, or append-only
journal behavior changes.

S1d is a separate fixed-forward successor: it adds an explicit result-link policy and migration `0013` without
rewriting S1c evidence or changing the meaning of the opaque owner-revision carrier. Neither batch is evidence for a
`filter_projection` physical-owner adapter.

## Required evidence before commit

- numeric-only unit and PG behavior remains unchanged;
- token-only terminal validation and accepted/quarantined/replay exact-copy;
- all carriers absent plus blank, invalid, and oversized token rejection;
- token collision and deferred slot/attempt/journal mismatch fail closed;
- `0011 -> 0012` preserves a historical accepted aggregate;
- fresh migration and rerun parity;
- S1a/S1b regression, scoped typecheck/lint, global mypy ceiling, and clean diff hygiene.

## Local advisory findings

A non-author dirty-tree adversarial pass found one P1 before commit: token-bearing immutable attempt/journal records
were still labelled `v1`. The rolling contract above is the fixed-forward resolution; fresh validation and pinned
review must inspect the resolved tree. The same pass noted that the synthetic carrier test proves storage transport,
not the future `filter_projection` physical owner. S1c does not claim that adapter evidence.

## Fresh pinned review scope

Review base: `38013035f0000fbe978e56b2ae55d8cbbc33fab3`. Review head:
`bacae9e80c8c0593b6c8c436b06e68777ade6d54`. Exact intended scope:

- `docs/NEXT_TODO.md`
- `docs/TRACK_D_D1N_REMAINING_ACTION_AND_AGENT_TOOL_SERVING_PLAN.md`
- `docs/TRACK_D_D1N_S1A_AGENT_TOOL_RESULT_SLOT_IMPLEMENTATION.md`
- `docs/TRACK_D_D1N_S1B_INSPECT_OPERATION_RESULT_IMPLEMENTATION.md`
- `docs/TRACK_D_D1N_S1C_OPAQUE_OWNER_REVISION_CARRIER_IMPLEMENTATION.md`
- `src/sourcing_agent/agent_tool_result_slot.py`
- `src/sourcing_agent/agent_tool_result_postgres.py`
- `src/sourcing_agent/migrations/0012_agent_tool_result_owner_revision_token.sql`
- `src/sourcing_agent/repositories/workflow_runtime.py`
- `tests/test_d1n_agent_tool_result_slot.py`
- `tests/test_d1n_agent_tool_result_slot_uow.py`
- `tests/test_migration_runner.py`

## Author validation evidence

Run from the repository root with no live provider/model variables:

```text
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src SOURCING_REQUIRE_PG_STORE_TESTS=1 \
  .venv/bin/python -m pytest -q --tb=short \
  tests/test_migration_runner.py \
  tests/test_d1n_agent_tool_result_slot.py \
  tests/test_d1n_agent_tool_result_slot_uow.py \
  tests/test_d1n_inspect_operation_result_slot_uow.py
=> 74 passed + 72 subtests

PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src SOURCING_REQUIRE_PG_STORE_TESTS=1 \
  .venv/bin/python -m pytest -q --tb=short \
  tests/test_migration_runner.py \
  tests/test_d1n_acquisition_plan_preview_uow.py \
  tests/test_d1n_agent_tool_result_slot.py \
  tests/test_d1n_agent_tool_result_slot_uow.py \
  tests/test_d1n_inspect_operation_result_slot_uow.py \
  tests/test_d1n_canary_agent_tool_population.py \
  tests/test_d1n_agent_tool_registry.py \
  tests/test_d1n_projection_query_contracts.py
=> 189 passed + 76 subtests

PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src .venv/bin/python -m mypy \
  src/sourcing_agent/agent_tool_result_slot.py \
  src/sourcing_agent/agent_tool_result_postgres.py \
  tests/test_d1n_agent_tool_result_slot.py \
  tests/test_d1n_agent_tool_result_slot_uow.py
=> success, 0 issues in 4 files

make lint
=> 58 files already formatted; all checks passed

make typecheck
=> existing ceiling unchanged: 81 errors in 4 files

PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src .venv/bin/python -m py_compile \
  src/sourcing_agent/agent_tool_result_slot.py \
  src/sourcing_agent/agent_tool_result_postgres.py \
  src/sourcing_agent/repositories/workflow_runtime.py \
  tests/test_d1n_agent_tool_result_slot.py \
  tests/test_d1n_agent_tool_result_slot_uow.py \
  tests/test_migration_runner.py
git diff --check
=> clean
```

All results in this document are author evidence until a fresh pinned non-author artifact says otherwise.
