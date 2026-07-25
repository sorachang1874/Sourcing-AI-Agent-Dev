# Runner recovery pinned adversarial review — `28e7d8b`

## Pinned evidence header

- Review type: non-author, adversarial, pinned Git-object review.
- Repository: `/Users/changyuyi/projects/Sourcing AI Agent Dev`.
- Commit: `28e7d8b7510f633abd2b26c1090bfc97b5af3712`.
- Parent: `354e979008489996537dd6f8b7e42f5005d61a94`.
- Read source: independent `git archive` snapshot at
  `/private/tmp/x-first-28e7d8b-review.Br64l8/commit`; the mutable working tree was not used as review evidence.
- Exact scope:
  - `x-first-researcher-sourcing/src/x_first/adaptive_grok_wave_runner.py`
  - `x-first-researcher-sourcing/tests/test_adaptive_grok_wave_runner.py`
- Mechanical diff: 476 insertions, 70 deletions across two files.
- Finding totals: P0 0, P1 3, P2 1, P3 0.

## Findings

### P1-1 — An executor-return journal write failure can release the auth claim while cleanup is unconfirmed

`_run_adaptive_wave` does not set `defer_cleanup_to_recovery` until after
`process-result.json` has been atomically published
(`adaptive_grok_wave_runner.py:5755-5771`). If journal construction or publication raises while the executor has
returned `process_group_cleanup_confirmed=False`, the `finally` block follows the ordinary live-finalization path
(`:5810-5831`): it audits/deletes the ephemeral home and resolves the active auth claim even though the recorded
process group may still be alive.

Pinned fault injection made `_atomic_publish` fail only for `process-result.json` after a synthetic executor returned
an unclean, ledger-bound process. The resulting state had:

- `process-ledger.json` present;
- `ephemeral-home` absent; and
- the auth active-use claim absent.

This loses the credentials/session tree and permits a sibling use of the same auth digest before recovery has
terminated the recorded group. Set the recovery-defer state immediately from the returned `ProcessResult`, before
any journal serialization or I/O that can fail; journal failure must preserve the home and claim when cleanup is not
confirmed.

### P1-2 — The new journal has no required generation or terminal hash binding, so current runs can silently fall back to legacy evidence synthesis

The terminal bundle treats `process-result.json` as optional (`:7241-7283`) and skips journal-to-receipt fact
comparison for every `crash_recovered` receipt (`:7261`). Recovery decides whether missing process spools may be
replaced with empty bytes solely from journal presence (`:7737-7814`). Neither the current intent/runtime layout nor
the receipt artifacts identify a generation in which the journal is mandatory, and the receipt does not retain a
journal hash (`:6010-6026`, `:8044-8114`). The journal payload also has no stdout/stderr digest or byte count
(`:848-884`), so it does not bind the fsynced executor spools it is intended to preserve.

Pinned probes demonstrated all of the following:

1. Deleting `process-result.json` from a completed current bundle left `validate_operator_bundle` at zero errors.
2. Mutating the valid journal's `timed_out` fact after a crash-recovery receipt left bundle validation at zero errors.
3. For a current unclean run, deleting the journal and one spool caused recovery to publish a zero-byte
   `raw.stdout`, return `crash_recovered`, and validate the resulting bundle with zero errors.

This makes journal deletion or loss a semantic downgrade into legacy recovery and allows recovery to bless missing or
modified process evidence. Add an intent-bound process-evidence generation (or bump the owning intent/runtime
contract), require the journal for that generation, bind its SHA-256 into the receipt, and bind closed-spool digest and
length facts into the executor-return journal. Keep empty-spool synthesis only for explicitly recognized older
generations.

### P1-3 — Recovery can claim confirmed cleanup without any process identity capable of proving or performing cleanup

The journal validator accepts the combination `process_spawn_attempted=False` and
`process_group_cleanup_confirmed=False` as long as all identity fields are null (`:6341-6387`). Recovery then accepts
the absence of both journal spawn bindings and a process ledger (`:7789-7800`), performs no liveness check, and
hard-codes `process_group_cleanup_confirmed=True` in the crash receipt (`:8055-8081`).

A pinned probe supplied that accepted executor result and made the liveness callback return true for any queried
group. Because no ledger existed, recovery never queried a group and emitted a receipt with cleanup confirmed and
spawn attempted false. This is unsafe for partial-spawn/identity-unknown states and is also inconsistent with the
built-in executor's defensive branch that can return unconfirmed cleanup without bindings after `Popen`
(`:2860-2877`).

Model spawn state explicitly. An unconfirmed result must either carry a complete ledger-bound identity that recovery
can verify and terminate, or remain unrecoverable/fail-closed; recovery must not convert unconfirmed cleanup to true
merely because a ledger is absent.

### P2-1 — Exceptions during post-executor evidence sealing still delete the only session copy

For a cleanup-confirmed process, session measurement/capture and spool promotion can still raise
(`:5772-5802`). Because `defer_cleanup_to_recovery` is false, the same `finally` block deletes the ephemeral home and
releases the auth claim (`:5810-5833`) before evidence sealing has completed.

Pinned fault injection raised from the first post-executor `_measure_session_tree` call. The journal existed, but the
ephemeral home, active claim, and retained `session-updates.jsonl` were all absent. Recovery then produced a
bundle-valid crash receipt whose session proof was `missing`. This fails closed for candidate promotion, but loses the
only replayable native-tool transcript after provider cost has already been incurred.

Track process cleanup and evidence sealing as separate durable phases. Do not finalize/delete the home until required
session and spool evidence has been durably promoted, or leave a recovery-owned home/claim on any sealing exception.

## Validation

- Exact pinned runner suite:
  `PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src /Users/changyuyi/projects/Sourcing\ AI\ Agent\ Dev/sourcing-ai-agent/.venv/bin/python -m unittest tests.test_adaptive_grok_wave_runner -v`
  — 101 tests in 103.665 seconds, all passed.
- Exact pinned Ruff scope:
  `/Users/changyuyi/projects/Sourcing\ AI\ Agent\ Dev/sourcing-ai-agent/.venv/bin/ruff check src/x_first/adaptive_grok_wave_runner.py tests/test_adaptive_grok_wave_runner.py`
  — all checks passed.
- Pinned diff check: zero whitespace errors.
- Additional owner-private fault-injection probes were run only against temporary directories; they made no provider,
  model, credential-source, or mutable-working-tree calls.

## Residual assessment

- The positive path is materially improved: complete ledger-bound cleanup failure retains spools/home/claim, recovery
  terminates a still-live matching group before session capture and auth audit, and `_publish_process_spools` rejects
  final/spool byte mismatches.
- Those positive-path properties do not cover the four exception/tamper states above. The existing 101 tests have no
  regression for journal-publication failure with unconfirmed cleanup, mandatory-current journal generation,
  crash-receipt journal mutation, or unconfirmed cleanup without a ledger.
- This review makes no claim about files outside the exact two-file pinned scope and used no current compact/profile
  working-tree files as evidence.

## Final verdict

NO-GO
