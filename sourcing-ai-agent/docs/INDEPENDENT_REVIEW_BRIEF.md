# Independent Review Brief

> Status: Current reusable reviewer brief. Use as standing context for Independent Review Gate prompts; it is not a substitute for the scoped prompt, targeted tests, or a stored GO/NO-GO review artifact.

> Standing context for an independent, adversarial reviewer. The reviewer is not the author. The goal is to find correctness, contract, runtime, data-quality, and product-risk failures before a change reaches live validation or W6/nightly.

## Product Context

This repository is a sourcing, recruiting automation, and public-information enrichment system. It coordinates provider-backed acquisition, LinkedIn/profile ingestion, Public Web evidence, durable workflow recovery, CRM follow-up, local company/person assets, and exportable evidence-backed signals.

## Review Bar

- Contract-first: every shared field, command, API response, status, readiness flag, filter count, export field, and provider result must have one owner and one source of truth.
- Scope discipline: Contract docs, owner matrices, public API fields, provider/model behavior, durable runtime commands, migration bridges, and milestone closeouts require independent review before live/W6/manual signoff.
- No hidden fallback: migration bridges must be report-visible, signoff-gated, and paired with a deletion condition.
- Durable execution: normal work should follow `append-only event -> reducer -> typed command -> owner activity/attempt/entity-delta -> read-model update`.
- Fail closed: readers must not repair, providers must not silently fallback to another model/source, and frontend must not infer backend truth from local status strings.
- Provider cost safety: live provider/model calls must be bounded, attributable, idempotent where retried, and circuit-breaker protected.
- Evidence quality: Public Web or AI adjudication may expose reviewable evidence only when provenance, identity, URL shape, and model/fallback status are visible.
- PG-only execution state: new durable runtime, OperationRun, Activity/Attempt/EntityDelta, CRM task/current-state, company/person asset current-state, and Agent-callable surfaces must not add SQLite normal paths.
- No self-certification: author summaries, green tests, and W6/nightly reports do not count as independent review evidence.
- Scoped context: review the listed files/diff and targeted Contract sections. Do not read full `PROGRESS.md`, full `docs/NEXT_TODO.md`, or full long Contract docs unless the review scope explicitly requires full context.

## Required Review Questions

1. Does this change introduce a second owner, hidden fallback, reader-side repair, or local frontend interpretation of a backend contract?
2. Are all changed fields documented with owner, source of truth, allowed values, fallback/migration status, consumers, and fast preflight?
3. Can recovery, retry, cancel, resume, or late provider results produce duplicate side effects or accepted stale evidence?
4. Are provider/model calls bounded by item-level identity, retry limits, circuit breakers, and cost/quality observability?
5. Do Public Web/model outputs separate raw evidence, model-reviewed signals, human promotions, and exportable assertions?
6. Could this pass unit tests while failing under hosted PG, worker-daemon recovery, frontend polling, or W6/nightly pressure?
7. Is any migration bridge still needed, and is its removal condition explicit?
8. If the change claims a milestone or manual/live readiness, is the review artifact path recorded and are any accepted exceptions tracked?
9. Are user-visible retry/cancel/resume/promotion/export controls guarded against accidental provider/model calls and stale evidence?

## Output Format

Start with a compact evidence header:

- Reviewed scope.
- Contract docs considered.
- Author validation considered.
- Command/model used, if visible.
- Accepted exceptions, if any.
- Residual risks for live/W6/manual follow-up.

Then list findings **exhaustively, ranked by severity** — do not truncate to the top items. A real
finding withheld from this round surfaces in the next round and costs a whole extra gate cycle (the
Track A 5e closeout took five review rounds precisely because each round surfaced a new partial
list). For each finding:

1. `severity` / `file:line` / issue.
2. Why it matters.
3. Concrete fix.
4. **Classification** (exactly one):
   - `new` — a new-class finding not previously raised for this scope. Only `new` findings can block.
   - `re-raise` — previously raised and already addressed; cite the addressing commit/artifact.
     Invalid as a blocking finding unless accompanied by new evidence the fix is insufficient.
   - `residual` — true but deliberately accepted; **must cite a `docs/RESIDUAL_LEDGER.md` row id**.
     A `residual` classification without a ledger id is invalid — reclassify as `new`.

End with one verdict:

- `GO`: no `new` blocking findings. Listed `residual`/`re-raise` items do not block.
- `NO-GO`: must fix the listed `new` blocking findings before live validation, W6/nightly, or merge.

**Termination rule**: a review round whose blocking findings are all `re-raise`/`residual` is a
terminal `GO` — the gate loop ends; it does not re-litigate accepted residuals each round.

Style suggestions and nice-to-haves go in a clearly separated non-blocking trailer section, never
mixed into the findings list. Do not self-certify if you authored the change.
If evidence is insufficient, return `NO-GO` and name the missing artifact or Contract.
A bare `NO-GO` without at least one `severity / file:line / issue` finding is an invalid review artifact.
