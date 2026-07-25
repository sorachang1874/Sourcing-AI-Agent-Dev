# Durable Command Ownership Fencing — hardening track

> Status: tracked durable-runtime hardening (opened 2026-06-15 by the C1.4a
> async-Codex re-review). NOT yet implemented. Owner-approved scoping: keep the
> expired-`claimed` reclaim **export-only** for the original C1 slice
> (idempotent-safe), and do the general fencing as this focused track rather
> than ballooning C1. D1m later added a second bounded opt-in for its
> exact-claim-guarded three-phase owner; this does not change the global default
> or close the general hardening track.

## The gap (pre-existing, surfaced by C1.4a)

The durable `workflow_commands` execution lifecycle does **not enforce lease
ownership** end-to-end, so a *stalled* original claimant can double-run or
stale-write after another worker takes over an expired lease:

1. **`mark_workflow_command_running` return is ignored at owner call sites.**
   The store method *does* fence on `lease_owner` when one is passed
   (`storage.py` WHERE adds `lease_owner = ?`), but `_run_*` owner methods ignore
   the `{}` it returns when fenced out and continue via `... or claimed`
   (e.g. `orchestrator.py:_run_projection_export_generate_command`), proceeding to
   build + publish + terminalize anyway.
2. **Terminal updates don't fence on `lease_owner`.**
   `mark_workflow_command_succeeded` / `mark_workflow_command_failed` match on
   `status IN ('claimed','running')` without `lease_owner`, so a worker that no
   longer holds the lease can still mark the command succeeded/failed and
   overwrite the new owner's result.
3. **No attempt-budget contract for pre-running claim expiry.**
   `claim_workflow_command` does `attempt = attempt + 1` on every (re)claim, and
   `mark_workflow_command_failed` uses `attempt` for `max_attempts`. A command
   that repeatedly expires in `claimed` *before any ActivityAttempt* can burn the
   execution retry budget before real owner work starts — or, if every claimant
   dies before failure, be reclaimed forever. The contract says the
   **ActivityAttempt** is the real execution attempt, not the claim.

This gap is **pre-existing**: `running`-with-expired-lease was already reclaimable
by `list_ready_workflow_commands`, so the same double-run exposure existed before
C1. C1.4a's reclaim fix widened it to `claimed` and (briefly) to all command
types; that widening was then **scoped back to bounded owners only** (see below).

## Current scoping (the interim, shipped)

`list_ready_workflow_commands` / `claim_workflow_command` gained an opt-in
`reclaim_claimed: bool = False` parameter (SQLite + PG). Two bounded paths pass
`reclaim_claimed=True`:

- The **export** drain/run path
  (`_drain_export_projection_generate_commands`,
  `_run_projection_export_generate_command`). Export builds are idempotent
  (deterministic archive + atomic `os.replace` artifact publish + idempotent
  disk replay).
- The D1m `company.public_web.refresh` owner across root, source, and
  materialize commands. It checks the returned `mark-running` claim, guards
  Activity creation and every domain/completion write with the exact physical
  claim, and stops an old claimant after takeover. Guarded Activity start uses
  the PostgreSQL clock, deterministic ActivityRun/all-prior-ActivityAttempt
  identities, and full immutable spine validation. Split identities, alternate
  nonterminal rows, current/future terminal execution Activity/Attempt rows, and future or
  malformed resume evidence fail closed; a fully exact prior terminal may
  remain. A succeeded owner-specific resume Attempt may coexist only with its
  deterministic id/key, generation `<=` current, and exact workspace/activity/
  workflow/command/provider/request-ref/lease plus target/company/boolean-force/
  nonblank-output-reason semantics. Successful takeover closes superseded
  exact running execution attempts before the returned exact current Activity/
  Attempt spine is accepted as `running`. An exhausted final source attempt uses
  a D1m-only, DB-clock-authoritative atomic Command/Activity/Attempt closure. It
  retains a valid resume-control Attempt and converges an exact current failed
  owner-loss partial only when error/metadata/output carry one nonblank reason,
  `skipped`, `owner_lost=true`, and `deterministic_terminal_failure=false`; the
  shared claim API's attempt-budget semantics remain unchanged.

All other command types keep the original `queued/retry_wait/running`
ready/claim set. The flag remains opt-in; these bounded paths are not evidence
that end-to-end ownership fencing is globally complete.

## The hardening (this track — to implement)

Make ownership fencing mandatory across **all** command owners so reclaim is
universally safe, then make `reclaim_claimed` the default (and remove the flag):

1. **Enforce the `mark_running` fence at every owner call site.** Pass the
   claim's `lease_owner` and treat a `{}` return as "lease lost → stop"; drop the
   `... or claimed` stale-payload fallbacks and the ignored return values.
2. **Fence terminal updates on `lease_owner`.** Add `lease_owner` to the
   `mark_workflow_command_succeeded/failed` WHERE (or thread a claim token), so a
   non-owner cannot terminalize. SQLite + PG, with parity tests.
3. **Define + enforce the attempt-budget contract.** Decide whether pre-running
   claim expiry consumes execution retry budget. Recommended: it does **not** —
   only an ActivityAttempt counts; track claim-expiry separately so a
   repeatedly-dying claimant is bounded (e.g. a stuck-claim reaper / max reclaim
   count) without burning `max_attempts`.
4. **Regression**: A claims → lease expiry → B reclaims + completes → A resumes →
   assert A can neither run nor terminalize (no double-run / stale-write); and
   repeated expired-`claimed` reclaim at the `max_attempts` boundary behaves per
   the chosen contract, PG/SQLite parity.

When done, flip every command drain/run to `reclaim_claimed=True` and retire the
flag (reclaim becomes the universal, fenced default).
