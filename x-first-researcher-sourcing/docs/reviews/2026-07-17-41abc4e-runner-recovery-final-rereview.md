# Adaptive Grok runner recovery pinned final rereview — `41abc4e`

## Pinned evidence header

- Review type: non-author, adversarial, read-only Git-object rereview.
- Repository: `/Users/changyuyi/projects/Sourcing AI Agent Dev`.
- Commit: `41abc4e537c6f6be767f70a0ea8f4916518543d6`.
- Git first parent: `f187d28225775765b21bc069be7a4b0d202fd8c9`.
- Commit tree: `f4d87504607b2158412efceaab082925ca8759f2`.
- Parent tree: `9d3e641dbc96195d268acba926105b79bd043ceb`.
- Read and execution source: independent detached worktree
  `/private/tmp/xfirst-41abc4e-review.Y4FB2h`; mutable implementation and test files in the author worktree were not
  used as review evidence.
- Exact first-parent scope: 645 insertions and 69 deletions across exactly four files:
  - `x-first-researcher-sourcing/docs/ADAPTIVE_GROK_WAVE_RUNNER_CONTRACT.md`
  - `x-first-researcher-sourcing/docs/reviews/2026-07-17-31960cd-runner-recovery-final-review.md`
  - `x-first-researcher-sourcing/src/x_first/adaptive_grok_wave_runner.py`
  - `x-first-researcher-sourcing/tests/test_adaptive_grok_wave_runner.py`
- Sorted scope-path SHA-256:
  `e3350dbda4632ac5f874ff76cb1bc900d48fe494e84177abd4a4c9f8cb852025`.
- Binary scoped-diff SHA-256:
  `57f03bbb4782919992604c1f70de20e48a9badb3a9b8fb66c5569a1fbcbcb077`.
- Scoped blob and content digests:

  | Path | Git blob OID | SHA-256 of content |
  |---|---|---|
  | `docs/ADAPTIVE_GROK_WAVE_RUNNER_CONTRACT.md` | `3b1814410d20847fe93c52d872b84324850d2292` | `e4576498248ebfabd5276dae221f81371d5990621a65bee3609e1b838df7af57` |
  | `docs/reviews/2026-07-17-31960cd-runner-recovery-final-review.md` | `d295bf98a343a5063435b881c09fe73cdf768338` | `9c9fe414d77dcac1fb1f01595b7dc17af944391b2db921d7f85df63413d07e8a` |
  | `src/x_first/adaptive_grok_wave_runner.py` | `36eb1633c3e747c7048b40f30d0a4f48bb655f44` | `b4d81bf7e75e0a0054d6716ae3534f1e66417d7a1e340cb962fa693df876758a` |
  | `tests/test_adaptive_grok_wave_runner.py` | `258df3abca2bb459c8435e681117995714eefdad` | `0842fe25f664c5a197da9991f45a07fadf33edc1c2e2e5f3c3579d55c9c2f66d` |

- Finding totals: P0 0, P1 1, P2 0, P3 0.

## Findings

### P1-1 — re-raise/residual: real pending publications still permit a consumption-boundary downgrade and evidence mutation

The patch closes the four concrete shapes requested by the prior review. Missing consumption plus (a) a deleted
exact-owner claim, (b) an origin-rewritten `legacy_recovery` claim, or (c) a current no-spawn executor-return journal
now raises `recovery_grant_consumption_missing` before process hooks or retained-tree mutation. The genuine
pre-consumption producer control has no claim, journal, ledger, spool, or pending process evidence; it retains the
legacy path, makes zero process calls, preserves the copied home, and publishes only the intended blocking
`legacy_recovery` claim. A valid exact consumption record with a deleted claim also still recovers successfully and
replays as a valid bundle.

The broader pending-publication fence recorded in the changed contract is not implemented. The contract says every
independently durable process-boundary artifact is read-bound before mutation and explicitly lists a pending
publication as sufficient for the typed missing-consumption failure
(`ADAPTIVE_GROK_WAVE_RUNNER_CONTRACT.md:330-340,551-556`). The implementation instead recognizes a pending file only
when its target name is one of `process-result.json`, `process-ledger.json`, the stdout spool name, or the stderr spool
name (`adaptive_grok_wave_runner.py:8164-8179`). It then calls the generic cleanup routine, which deletes every
well-shaped `.pending-<name>-<nonce>` file, only after it may have synthesized a legacy claim
(`:8278-8298`; generic cleanup at `:3310-3331`).

That whitelist omits real `_atomic_publish` targets created after consumption: `raw.stdout`, `stderr.txt`,
`session-updates.jsonl`, `sanitized.json`, and `operator-receipt.json`. These are not hypothetical target names. Live
consumption precedes executor release (`:5780-5791`); retained session updates and spool promotion follow executor
return (`:5943-5961`); `sanitized.json` and `operator-receipt.json` are then atomically published (`:6101-6104,
6205-6208`). An abrupt process death between pending-file fsync/link and `_atomic_publish` cleanup can therefore leave
one of these exact owner-only pending files as independently durable evidence that the run crossed consumption.

A provider-free probe derived each case from the exact pinned actual-producer helper, then removed consumption,
claim, journal, ledger, and process spools so that the retained pending file was the sole boundary evidence. Pending
`process-result.json` and `process-ledger.json` controls correctly returned
`recovery_grant_consumption_missing`, made zero process calls, and left both trees byte-identical. In contrast, each
of the following five exact pending targets returned `recovery_process_identity_unavailable`:

- `raw.stdout`
- `stderr.txt`
- `session-updates.jsonl`
- `sanitized.json`
- `operator-receipt.json`

All five failing cases changed both retained trees: recovery published a new `legacy_recovery` claim and deleted the
pending evidence. Process hooks remained at zero, but that does not satisfy the promised pre-claim and
pre-pending-cleanup fence. This is the same downgrade class as the prior P1: deleting the consumption ledger and
other redundant evidence changes a post-consumption state into a mutable pre-consumption path even though one durable
current artifact remains.

Classify every module-owned pending publication that can coexist with a durable live intent as crossed-boundary
evidence before claim publication or cleanup, rather than using the current four-name whitelist. Validate its
owner/mode/name shape without deleting it, raise the typed missing-consumption error, and add actual target-name
regressions for at least `raw.stdout` and `operator-receipt.json`. The same pre-mutation classification should account
for already-promoted current outputs so deletion of the transient spool does not erase equivalent durable evidence.

## Closed controls and residual disposition

- Consumption plus exact claim deletion: typed pre-mutation failure; zero liveness, identity, and termination calls;
  run and approval snapshots identical.
- Consumption deletion plus `claim_origin=legacy_recovery`: same typed pre-mutation failure; rewritten claim and both
  retained trees remain byte-identical.
- Current no-spawn executor-return journal with no ledger or spools: same typed pre-mutation failure and zero hooks.
- Isolated current journal, process ledger, process spool, and `live_consumption` claim controls: 4/4 independently
  trigger the typed fence with no retained-tree mutation. Pending `process-result.json` and `process-ledger.json`
  controls also pass.
- Genuine abrupt pre-consumption crash: no process calls; copied home unchanged; no taint or receipt; intended legacy
  claim retained after `recovery_process_identity_unavailable`.
- Valid exact consumption plus missing claim: successful `crash_recovered` receipt, consumption digest retained,
  synthesized claim resolved, and `validate_operator_bundle(...) == []`.
- Direct-parent transitional recovery, keyless transitional rejection without journal mutation, and distinct legacy
  fixture recovery all pass. No transitional/legacy compatibility regression was found.
- The only blocker found in the exact scope is the incomplete pending-publication/current-output classification above.

## Validation

- Exact pinned runner suite:
  `PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src /Users/changyuyi/projects/Sourcing\ AI\ Agent\ Dev/sourcing-ai-agent/.venv/bin/python -m unittest tests.test_adaptive_grok_wave_runner`
  — 119 tests in 45.697 seconds, all passed.
- Eight named P1, pre-consumption, tamper-adjacent, transitional, and legacy tests — 8 tests in 20.938 seconds, all
  passed.
- Additional provider-free actual-producer controls — isolated journal/ledger/spool/claim fences 4/4; recognized
  pending journal/ledger targets 2/2; valid-consumption/missing-claim recovery passed and replayed cleanly.
- Additional pending-target adversarial matrix — 2 recognized targets behaved correctly; 5 real post-consumption
  atomic-publication targets reproduced the finding with deterministic error codes and tree snapshots.
- Exact pinned Ruff scope:
  `/Users/changyuyi/projects/Sourcing\ AI\ Agent\ Dev/sourcing-ai-agent/.venv/bin/ruff check --no-cache src/x_first/adaptive_grok_wave_runner.py tests/test_adaptive_grok_wave_runner.py`
  — all checks passed.
- Intent v1/v2 and operator-receipt v1/v2/v3 schemas all passed the runner's strict duplicate-key JSON parser.
- Exact four-file first-parent `git diff --check` passed with no whitespace errors.
- No credential source, provider/model call, private live artifact, candidate data, or mutable author implementation/test
  file was accessed. All probes used synthetic, provider-free material in private temporary directories.

## Final verdict

NO-GO
