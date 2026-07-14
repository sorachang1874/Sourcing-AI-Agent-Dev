# Track D D3a — Workflow-command claim identity characterization

> Status: historical author characterization-only baseline (2026-07-14), intentionally advanced by D3c1 and the D3c2a candidate on
> 2026-07-15. D3a itself remains a zero-product-code, zero-migration, non-live freeze of the then-existing
> `workflow_commands` claim surface before the Track D D3 claim-fencing implementation.
> It is not D3 implementation, an R-019 remediation claim, formal independent-review `GO`, live-provider approval,
> or product/milestone signoff. The exact enclosing commit and any later implementation commit must be pinned
> separately by the author handoff and review request.

## 1. Outcome and boundary

D3a records what the repository actually provided at its pinned baseline before Plan §6 item 6 adds a never-reset workflow-command claim
generation/token. It mechanically freezes schema, descriptor, writer, caller, retry-control, and API projection facts;
it does not choose the new schema or change runtime behavior.

This characterization adds this document and
`tests/test_d3_workflow_command_claim_identity_characterization.py`; the enclosing commit additionally routes both
D3a characterization records through `NEXT_TODO` and `INDEX`. It does not modify production Python,
`workflow_commands`, a migration, `TRACK_D_AGENT_RUNTIME_PLAN`, the residual ledger, frontend contracts, or
provider/model behavior. It creates no durable product row and performs no live call.

The characterized result is fail-closed:

- at the D3a baseline, `workflow_commands` had `attempt`, `lease_owner`, and `lease_expires_at`, but no physical `claim_generation`,
  claim-scoped token (`claim_token` or `lease_token`), or `control_epoch`;
- `attempt` increments on claim but can also be decremented or reset to zero by generic control paths, so it cannot
  identify a claim generation or prevent ABA;
- one recognized PG adapter method owns the physical claim mutation, behind one Postgres-only `ControlPlaneStore`
  facade;
- there are exactly **29 direct production claim call sites** across six modules;
- the selected descriptor/list/provenance/frontend path has no explicit redaction boundary for a future opaque token.

These are dated observations and mutation-sensitive characterization oracles, not the desired D3 contract. The later
implementation must intentionally replace the relevant assertions rather than weakening them while leaving ownership
ambiguous.

Current amendment: D3c1 replaced the characterized public pass-through with a closed projector. The D3c2a candidate installs dormant
physical `claim_generation`/`control_epoch` and the other eighteen D3b command columns, but deliberately leaves the
33-column descriptor and every runtime writer/consumer unchanged. Raw `claim_token`/`lease_token` columns remain
absent, and the runtime is still unfenced.

## 2. Physical row and claim writer

At the pinned D3a baseline, the baseline migration, runtime bootstrap DDL, and
`repositories.workflow_runtime.WORKFLOW_COMMANDS` descriptor agreed on the then-current identity-adjacent fields:

| Current field | Current behavior | Why it is not the missing fence |
|---|---|---|
| `attempt` | initialized at zero and incremented by a successful claim | generic partial-progress, prerequisite-wait, and resume paths decrement it; retry resets it to zero |
| `lease_owner` | caller-provided owner string | the string may be reused and is not a claim-unique capability |
| `lease_expires_at` | time bound on readiness/reclaim | expiry determines eligibility, not the identity of the claimant that may commit a later effect |
| `heartbeat_at` | liveness timestamp | it is mutable and does not distinguish generations |

The only currently recognized direct SQL claim writer is
`LiveControlPlanePostgresAdapter.claim_workflow_command`. Its single SQL update changes status to `claimed`, writes the
owner/expiry/heartbeat, and performs `attempt = attempt + 1` under readiness and expired-lease predicates.
`ControlPlaneStore.claim_workflow_command` is a Postgres-only delegating facade, not a second physical writer; its
legacy SQLite tail is retired.

This single-writer shape is useful for the next batch because the mint point is centralized. It does not itself fence
completion, partial-progress, child creation, ActivityAttempt creation, or a stale process that resumes after its lease
was reclaimed.

## 3. Resettable `attempt` and ABA

The current control methods prove `attempt` is retry-budget accounting rather than monotonic execution identity:

1. `claim_workflow_command`: `attempt = attempt + 1`;
2. `mark_workflow_command_partial_progress`: subtract one, clamped at zero;
3. `mark_workflow_command_waiting_prerequisite`: subtract one, clamped at zero;
4. `resume_workflow_command`: subtract one, clamped at zero;
5. `retry_workflow_command`: reset to zero.

Consequently, two distinct physical claims can expose the same `attempt` value. A result that carries only
`command_id + attempt` can therefore match again after requeue/retry, and a repeated `lease_owner` string does not close
that ABA window. D3's verification intent must not derive `claim_generation` from `attempt` or treat the two as aliases.
The D3 design's durable `control_epoch` is also separate: it must advance at the control transition that invalidates old
work, including the window after requeue commits but before the next claim exists.

## 4. Direct production caller inventory

The AST oracle counts every direct production name/attribute call spelled `claim_workflow_command` under
`src/sourcing_agent/*.py`. The current recognized population is:

| Module | Calls |
|---|---:|
| `acquisition_command_owner.py` | 6 |
| `crm_public_web_owner.py` | 3 |
| `enrichment.py` | 2 |
| `profile_fetch_owner.py` | 1 |
| `excel_intake_owner.py` | 1 |
| `orchestrator.py` | 16 |
| **Total** | **29** |

The count is the complete recognized direct-syntax population rather than a selected sample. A new recognized caller,
direct SQL claim, second facade, or moved call fails the characterization until claim-identity propagation and
ownership are reviewed; nonstandard indirection must explicitly extend the oracle.

## 5. Descriptor and selected API dict pass-through risk (historical, D3c1 sealed)

The D3a path had a two-sided migration risk:

1. `WORKFLOW_COMMANDS` is an explicit-column descriptor. Adding only migration/writer columns would omit the new
   identity from returned command dictionaries, so callers could claim successfully without receiving the fence they
   must later present.
2. Once a field is added to the descriptor, `CommandKernel._workflow_command_api_record` starts from
   `dict(command or {})`. The list/detail/provenance APIs reuse that record; the TypeScript adapter spreads
   `...(source as JsonObject)`; the JSON schema permits additional properties; and the demo keeps the raw record.
   Therefore a descriptor-visible opaque token would flow through public API/frontend surfaces unless an explicit
   allowlist or redaction boundary is installed in the same batch.

The characterization uses synthetic keys to prove this pass-through without persisting a token. It does not recommend
exposing a token. A public diagnostic generation and a private authorization capability may need different fields and
different projections; that decision is intentionally open.

D3c1 has since closed the public projection. The D3c2a candidate chooses the safe half of the physical rollout: it adds the columns
but does not extend the descriptor, so neither the internal Store record nor the public projector receives them yet.

## 6. Historical decisions required before implementation

At the D3a baseline these questions were intentionally open. D3b resolved their decision shape, and D3c1 later resolved
the public-projection prerequisite. D3c batches must continue to implement the remaining physical/owner obligations from
Plan §6 and the OB-ID/invariant obligations rather than guessing or treating this characterization as current authority:

1. **Physical shape and owner:** whether the durable contract is a monotonic `claim_generation`, an unguessable
   claim-scoped token, or both; exact names, types, defaults, constraints, mint owner, and overflow/rotation behavior.
2. **`control_epoch` separation:** where it is stored, which cancel/retry/resume/timeout/rebuild transitions advance it,
   and how the pre-next-claim requeue window is fenced independently of claim generation.
3. **Atomic mint boundary:** whether claim + ActivityAttempt creation must be one UoW, and exactly when a verification
   intent may bind `workflow_command_id + claim identity + activity_attempt_id`.
4. **CAS consumer matrix:** which heartbeat, running, partial, prerequisite, terminal, child-command, ActivityAttempt,
   result/event, and domain-apply writes must present generation/token/epoch, including exact stale-owner outcomes and
   zero-write evidence.
5. **API exposure and redaction:** which identity is safe as audit metadata, which token must remain internal, whether
   an internal descriptor/projection is needed, and the list/detail/provenance/frontend regression matrix.
6. **Migration and compatibility:** existing-row backfill, zero/unclaimed semantics, rolling-deploy ordering, reclaim of
   pre-migration leases, replay behavior, and the deletion condition for any compatibility bridge.
7. **Caller rollout:** how all 29 callers receive and propagate the returned identity without a partially fenced mixed
   population; direct SQL and new-caller ratchets must remain zero/fail-closed.
8. **R-019 transaction boundary:** which claim-identity writes can close a bounded R-019 subproblem and which broader
   operation/action/event/command UoW, phantom child/attempt, linked Operation sync, reducer atomicity, and lock-budget
   gaps remain residual.

No production implementation should start from the word `token` alone; its authority, storage, comparison, secrecy,
and invalidation transition must be explicit first.

## 7. R-019 boundary

**R-019 remains pending remediation.** D3a does not add an operation state-sync caller, change the 26-call ratchet,
touch dispatch/retry/completion behavior, mint a child command/ActivityAttempt, or alter a transaction-lock caller. It
therefore does not close or waive R-019.

Activating a repository-minted claim generation/private-token verifier plus effectful CAS later would close only the
stale-claim identity portion if every effectful consumer uses the stored fence with tested zero-write stale outcomes.
Dormant physical columns alone close none of it. That later activation would not by itself make operation + action + event/command
writes one UoW, prevent every phantom child, synchronize linked OperationRun/AgentAction state, make reducer writes
atomic, or add a monotonic total acquisition budget to transaction-lock callers. Those claims require their own
implementation and evidence.

The D3c2a candidate reconciles only the dormant command-table physical subset; it changes neither the 26-call ratchet
nor any effect authorization predicate, so R-019 remains pending. The next owner/runtime batch that reads or writes
these fields must reconcile its exact R-019 subproblem before activation and before any live/W6/manual/product signoff.
Characterization tests and author evidence are not an independent review verdict.

## 8. Mutation sensitivity and validation

The suite fails when these recognized direct lexical surfaces change:

- the D3c2a physical migration drifts from its explicit dormant contract, or a runtime bootstrap/descriptor silently
  gains a claim-identity field before its owner cutover;
- `attempt` stops exposing the current decrement/reset behavior without an intentional contract update;
- a second direct SQL claim writer using the current table/status shape appears, or the Postgres-only facade stops
  delegating to the canonical writer;
- the 29 direct name/attribute call sites or their `6/3/2/1/1/16` distribution changes;
- the selected backend/API/frontend dict pass-through path is removed or expanded without updating the exposure
  decision;
- this document drops the R-019 non-closure or the pre-implementation decision list.

These ratchets do not prove that a future helper/dynamic SQL builder, alias, `getattr`, generic dispatcher, or new API
projection cannot evade the recognized spelling. Such a path is a claim-fence contract change and must extend the
inventory; a green characterization test is not evidence that an unrecognized path is safe.

Run from `sourcing-ai-agent/`:

```bash
PYTHONPATH=src .venv/bin/python -m pytest -q \
  tests/test_d3_workflow_command_claim_identity_characterization.py
PYTHONPATH=src .venv/bin/python -m pytest -q \
  tests/test_durable_runtime.py::DurableRuntimeStorageTest::test_workflow_commands_are_idempotent_claimable_and_terminal \
  tests/test_durable_runtime.py::DurableRuntimeStorageTest::test_workflow_commands_reclaim_expired_running_lease \
  tests/test_durable_runtime.py::DurableRuntimeStorageTest::test_workflow_command_control_is_safe_and_status_bounded
.venv/bin/ruff check tests/test_d3_workflow_command_claim_identity_characterization.py
.venv/bin/ruff format --check tests/test_d3_workflow_command_claim_identity_characterization.py
PYTHONPATH=src .venv/bin/python -m py_compile \
  tests/test_d3_workflow_command_claim_identity_characterization.py
git diff --check -- \
  tests/test_d3_workflow_command_claim_identity_characterization.py \
  docs/TRACK_D_D3A_WORKFLOW_COMMAND_CLAIM_IDENTITY_CHARACTERIZATION.md
```

Author evidence on 2026-07-14: the new characterization passed **6 tests**; the three exact PG-backed durable-runtime
adjacent nodes passed **3 tests**; focused Ruff check/format, `py_compile`, and the two-file whitespace check passed.
No full `tests/test_pipeline.py`, provider/model, live, W6, or manual validation belongs to this characterization batch.
