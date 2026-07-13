BEGIN_ARTIFACT path=sourcing-ai-agent/docs/pro-consults/2026-07-14-c1b-race-remediation-retry/response.md
ADVISORY_ONLY — not an independent-review artifact or formal GO.

# Verdict

Raw Pro verdict: adjust

# Scope understood

Repository: `sorachang1874/Sourcing-AI-Agent-Dev`

Exact observed full commit: `9c3987a904bc8e38f523388c526b0e1d0a61d735`

Commit observed: `feat: close Track C C1b legacy submit contracts`.

Raw branch lookup outcome: not_requested

Immutable file citations:

https://github.com/sorachang1874/Sourcing-AI-Agent-Dev/blob/9c3987a904bc8e38f523388c526b0e1d0a61d735/AGENTS.md

https://github.com/sorachang1874/Sourcing-AI-Agent-Dev/blob/9c3987a904bc8e38f523388c526b0e1d0a61d735/sourcing-ai-agent/AGENTS.md

https://github.com/sorachang1874/Sourcing-AI-Agent-Dev/blob/9c3987a904bc8e38f523388c526b0e1d0a61d735/sourcing-ai-agent/docs/TRACK_C_C1_DURABLE_PLAN_TASK_DESIGN.md

https://github.com/sorachang1874/Sourcing-AI-Agent-Dev/blob/9c3987a904bc8e38f523388c526b0e1d0a61d735/sourcing-ai-agent/docs/TRACK_C_C1_HEAVY_OPS_DESIGN.md

https://github.com/sorachang1874/Sourcing-AI-Agent-Dev/blob/9c3987a904bc8e38f523388c526b0e1d0a61d735/sourcing-ai-agent/frontend-demo/src/lib/historyRecovery.ts

https://github.com/sorachang1874/Sourcing-AI-Agent-Dev/blob/9c3987a904bc8e38f523388c526b0e1d0a61d735/sourcing-ai-agent/src/sourcing_agent/api.py

https://github.com/sorachang1874/Sourcing-AI-Agent-Dev/blob/9c3987a904bc8e38f523388c526b0e1d0a61d735/sourcing-ai-agent/src/sourcing_agent/async_task_contract.py

https://github.com/sorachang1874/Sourcing-AI-Agent-Dev/blob/9c3987a904bc8e38f523388c526b0e1d0a61d735/sourcing-ai-agent/src/sourcing_agent/orchestrator.py

https://github.com/sorachang1874/Sourcing-AI-Agent-Dev/blob/9c3987a904bc8e38f523388c526b0e1d0a61d735/sourcing-ai-agent/src/sourcing_agent/plan_submit_contract.py

https://github.com/sorachang1874/Sourcing-AI-Agent-Dev/blob/9c3987a904bc8e38f523388c526b0e1d0a61d735/sourcing-ai-agent/tests/test_frontend_history_recovery.py

https://github.com/sorachang1874/Sourcing-AI-Agent-Dev/blob/9c3987a904bc8e38f523388c526b0e1d0a61d735/sourcing-ai-agent/tests/test_plan_submit_contract.py

Observed scope anchors:

- Repository rules require bounded root-cause fixes, explicit contracts, and validation discipline.
- Shared contract changes require owner/source-of-truth definitions and fast preflight coverage.
- C1 durable design remains author-complete design context rather than permission for schema migration.
- C1 heavy-ops design remains consolidation-first.
- `plan_submit_contract.py` is the compatibility authority for the legacy Plan submit bridge.
- Async task contract is the unified heavy operation transport contract.

# Assumptions and missing information

- Local dirty state is not reviewed.
- The consultation evaluates the pinned commit only.
- Existing local fixes mentioned in the request are not considered complete.
- No C1c-e durable migration work is authorized.

Missing information:

- Actual local diff after commit `9c3987a904bc8e38f523388c526b0e1d0a61d735`.
- Any future owner decision approving durable consumer ownership, publication UoW, or 202 cutover.
- Whether current race tests already contain scheduler-level barriers beyond the pinned files.

# Findings

## P0

The smallest correct C1b direction is:

**close the serving ownership gap without pretending to solve durable execution.**

The correct C1b state machine should remain process-local:

```
submit request
    |
    v
owner lock
    |
    +--> existing active signature owner
    |        |
    |        +--> attach consumer
    |
    +--> no owner
             |
             +--> create hydration owner
                    |
                    +--> one worker thread
                    |
                    +--> plan_workflow
                    |
                    +--> terminal publication
```

Required lock-protected state:

| State | Owner |
| --- | --- |
| signature ownership | hydration owner map under lock |
| active consumer set | same owner lifecycle lock |
| worker existence | owner record |
| cleanup eligibility | lock-protected final consumer snapshot |

The critical rule:

A consumer attach and owner cleanup decision must be one atomic transition.

Do not:

1. snapshot consumers,
2. release lock,
3. cleanup,
4. discover a new consumer.

That creates the stranded consumer race.

Recommended invariant:

> If a consumer exists for a signature, either an active owner exists or the caller must atomically create/reclaim the owner before returning.

Do not start a second compiler. Late consumers must join or trigger bounded owner recreation, not parallel hydration.

## P1

Generation checks safe in C1b:

Allowed:

- remove accidental identity fields from compile signatures.
- generation-fence final history writes.
- reject stale consumer updates before frontend-visible mutation.
- make missing/unknown async states fail closed.
- make owner absence fail closed.

Not solvable in C1b:

- review creation races.
- criteria compiler-run persistence races.
- side effects created before generation fencing.

Those require:

```
compute
   |
   v
immutable result
   |
   v
generation check
   |
   v
publication transaction
```

or equivalent publication UoW.

Do not add partial publication fences that create a third execution model.

## P2

Remaining bounded cleanup:

### AST ratchet

Current AST model should expand from symbol matching to callable provenance.

Required coverage:

```
Thread(target=function)
Thread(target=alias)
executor.submit(function)
executor.submit(alias)
executor.submit(lambda: function())
wrapper(function)
```

Implementation approach:

1. Build local symbol table.
2. Resolve imports.
3. Track assignments:

```
worker = _run_plan_hydration
```

1. Resolve call expressions recursively.
2. Fail closed on unknown callable origin.

The ratchet should prove:

- exactly one hydration queue creator.
- exactly one serving compile caller.
- zero hidden executor/thread routes.

### Legacy identity rule

Without schema change:

Use fail-closed provenance:

Allowed:

- server-generated known history ownership metadata.
- explicit current request identity match.

Rejected:

- authenticated user merely guessing `history_id`.
- replacing history without provenance match.

Unknown ownership should return a safe failure, not attach or replace.

# Recommended sequence

| Commit | Scope | Allowed | Not allowed | Rollback |
| --- | --- | --- | --- | --- |
| C1b-r1 | owner lifecycle lock/state machine | process-local ownership fix | durable storage | revert commit |
| C1b-r2 | generation fences and identity fail-closed | stale-write prevention | publication UoW | revert commit |
| C1b-r3 | AST ratchet expansion | static guardrails | runtime architecture change | revert commit |
| C1b-r4 | deterministic barrier tests | race proof | flaky sleep tests | revert tests |

One-wave lookahead:

After C1b:

1. Run scoped independent review.
2. Keep C1c-e blocked.
3. Prepare D-C1-3 decision material only:
  - publication ownership,
  - durable consumer identity,
  - repository lifecycle,
  - 202 transition.

# Validation and failure modes

Acceptance gates:

## Owner lifecycle

Must prove:

- same signature concurrent submit creates one worker.
- late consumer joins active owner.
- cleanup cannot remove live ownership.
- owner failure does not leave silent pending state.

## Generation fence

Must prove:

- stale generation cannot publish history.
- stale generation cannot create review-visible side effects.
- current generation remains successful.

## Barrier tests

Required:

1. Block worker after ownership acquisition.
2. Attach late consumer.
3. Release worker.
4. Assert:
  - one compile,
  - all consumers terminal,
  - no orphaned map entry.

Second:

1. Start generation A.
2. Start generation B.
3. Let A reach side-effect boundary.
4. Assert stale A cannot publish.

Failure patterns:

- replacing worker method with mock is insufficient.
- sleeps are insufficient.
- tests must control ordering points.

# Deferred decisions

Deferred:

- durable consumer table.
- repository-backed owner lifecycle.
- TTL/replay.
- HTTP 202 migration.
- publication UoW.
- worker process split.

# Owner decisions required

1. Whether publication UoW becomes the C1c prerequisite boundary.
2. Whether durable consumer identity owns public task lifecycle.
3. Whether C1e deletes all process-local hydration ownership.
4. Whether legacy history provenance requires schema support later.
5. Whether AST ratchet strictness should block all unknown callable resolution or only serving paths.

END_ARTIFACT
