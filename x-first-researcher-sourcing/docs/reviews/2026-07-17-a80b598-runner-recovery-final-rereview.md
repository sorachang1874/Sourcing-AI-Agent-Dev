# Adaptive Grok runner recovery pinned final rereview — `a80b598`

## Pinned evidence header

- Review type: non-author, adversarial, read-only Git-object final rereview.
- Repository: `/Users/changyuyi/projects/Sourcing AI Agent Dev`.
- Commit: `a80b598fd50529f19f76e82c9ccdb074673c85df`.
- Git first parent: `702de976d28589b852d0f5df217fa9f10fb60c63`.
- Commit tree: `2e887ecadb3c8110b3179b1ba89108a78933eb44`.
- Parent tree: `4b1fc3f61c16d8ea30c739ce8adf09939b69af94`.
- Read and execution source: independent detached worktree
  `/private/tmp/xfirst-a80-review.5jEcMQ`; mutable implementation and test files in the author worktree were not used
  as review evidence.
- Exact first-parent scope: 475 insertions and 70 deletions across exactly four files:
  - `x-first-researcher-sourcing/docs/ADAPTIVE_GROK_WAVE_RUNNER_CONTRACT.md`
  - `x-first-researcher-sourcing/docs/reviews/2026-07-17-41abc4e-runner-recovery-final-rereview.md`
  - `x-first-researcher-sourcing/src/x_first/adaptive_grok_wave_runner.py`
  - `x-first-researcher-sourcing/tests/test_adaptive_grok_wave_runner.py`
- Sorted scope-path SHA-256:
  `ce0cd6c497eefd8acf1f2194dd4584bba7286069ba1a7a36267d635c124a1a48`.
- Binary scoped-diff SHA-256:
  `85e1f53849f21cb80b3244cb05145956a6f8d5932b3ba0f07f90b3c569186727`.
- Scoped blob and content digests:

  | Path | Git blob OID | SHA-256 of content |
  |---|---|---|
  | `docs/ADAPTIVE_GROK_WAVE_RUNNER_CONTRACT.md` | `8430de0a12b95521e3f5c292b8c0deb5f8a7eecb` | `62e53736da25809918302f7fd29a1cd55082df40c5c2d4ece25e793306ae44c3` |
  | `docs/reviews/2026-07-17-41abc4e-runner-recovery-final-rereview.md` | `aa5d9c94847aec41d9abf77add144538b40433f7` | `c4e64f36fcac666de5bc56391012a895f33da791556489a4e1d1794ccfc523ea` |
  | `src/x_first/adaptive_grok_wave_runner.py` | `b0708c82c784aafa30f8f20ad5012d8b554e2a62` | `197426eddc4ee12b022639114172d0b9eacf6529b8a7d245a64c96172a9e962d` |
  | `tests/test_adaptive_grok_wave_runner.py` | `390012fa70be0e9fdf1f24201008ffc76df77a8c` | `a628c7f1df5d9ac2784ef050d437e676f9d9ab88423985339868a29082cc7e4f` |

- Finding totals: P0 0, P1 1, P2 0, P3 0.

## Findings

### P1-1 — re-raise/residual: the registry fences pending names but not the same already-promoted current artifacts

The fixed-forward closes the concrete pending-publication gap from the `41abc4e` review. One immutable registry now
names the process journal/ledger/spools, retained session updates, promoted stdout/stderr, sanitized output, and
terminal receipt (`adaptive_grok_wave_runner.py:278-329`). `runtime_layout` is derived from that registry and validated
for exact equality (`:6491-6524`), and every real run-root `_atomic_publish` target in the pinned implementation was
mechanically traced back to the same registry or to a path derived from its exact runtime layout. The nine registered
post-consumption pending names all trigger `recovery_grant_consumption_missing` before cleanup, claim publication, or
process hooks, and the three prompt/request/intent pending names retain the intended pre-consumption path.

The classifier nevertheless uses the registered post-consumption names only while matching `.pending-*` paths. It
sets `pending_post_consumption_evidence_present` from pending filenames, then adds existence checks only for the two
process spools (`adaptive_grok_wave_runner.py:8279-8293`). The final crossed-boundary expression considers a
`live_consumption` claim, current process-result journal, process ledger, or that pending/spool boolean
(`:8373-8388`). It never checks whether the registered final paths `raw.stdout`, `stderr.txt`,
`session-updates.jsonl`, or `sanitized.json` already exist. This contradicts the changed contract, which explicitly
classifies retained session updates, promoted raw stdout/stderr, sanitized output, and the terminal receipt as
post-consumption evidence and permits a legacy claim only when *no* post-consumption evidence exists
(`ADAPTIVE_GROK_WAVE_RUNNER_CONTRACT.md:334-344,555-563`). It also contradicts the helper's own statement that the
returned name set contains every durable artifact proving consumption (`adaptive_grok_wave_runner.py:6498-6516`).

A provider-free adversarial probe used the pinned actual-producer helper to create a current post-executor run. For
each case it deleted the consumption record and active-use claim, deleted the journal, ledger, and both process
spools, then published exactly one registered final artifact through the pinned `_atomic_publish` implementation.
All four independently durable targets reproduced the same downgrade:

- `raw.stdout`
- `stderr.txt`
- `session-updates.jsonl`
- `sanitized.json`

All 4/4 returned `recovery_process_identity_unavailable`, not the required
`recovery_grant_consumption_missing`. Process liveness, identity, and termination hooks remained at zero, and the run
tree remained byte-identical, but the approval tree changed in every case because recovery published a new
`legacy_recovery` active-use claim before failing. A second control retained the exact claim but rewrote its origin to
`legacy_recovery` while leaving promoted `raw.stdout`; it was likewise downgraded to
`recovery_process_identity_unavailable`. Thus deletion of the other redundant records still turns a real
post-consumption state into the legacy path even though one registry-declared current artifact remains.

Use the phase registry to read-bind both pending and already-published registered post-consumption paths before any
claim or retained-tree mutation. Add actual-final-path regressions, at minimum for promoted raw output and one of
retained session/sanitized output, alongside the pending-path matrix. The typed fence and byte-identical run/approval
snapshots must hold after deleting consumption, claim, journal, ledger, and spools; the same evidence must defeat a
rewritten `legacy_recovery` origin.

## Closed controls and residual disposition

- Registered post-consumption pending-target matrix: 9/9 returned the typed missing-consumption error; run and
  approval trees remained byte-identical; all process hooks stayed at zero.
- Isolated current journal, process ledger, stdout spool, and stderr spool: 4/4 independently returned the typed
  missing-consumption error with both trees unchanged and zero process hooks.
- Isolated exact `live_consumption` active-use claim: returned the typed error with both trees unchanged and zero
  process hooks.
- Pending compiled prompt, operator request, and operator intent: all three retained the intended pre-consumption
  legacy path and did not call process hooks.
- The genuine abrupt pre-consumption producer control, missing-claim/current-journal controls, and origin-rewrite
  controls in the pinned suite pass. No transitional or legacy fixture regression was found by the full suite.
- No real run-root `_atomic_publish` call site or `runtime_layout` entry was omitted from the central name registry.
  The remaining blocker is the classifier's failure to inspect the already-published paths represented by that same
  registry.

## Validation

- Thirteen named registry, pending-target, missing-consumption, origin-rewrite, current no-spawn, and genuine
  pre-consumption tests — 13 tests in 1.428 seconds, all passed.
- Exact pinned runner suite:
  `PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src /Users/changyuyi/projects/Sourcing\ AI\ Agent\ Dev/sourcing-ai-agent/.venv/bin/python -m unittest tests.test_adaptive_grok_wave_runner`
  — 128 tests in 27.733 seconds, all passed.
- Exact pinned full X-First suite:
  `PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src /Users/changyuyi/projects/Sourcing\ AI\ Agent\ Dev/sourcing-ai-agent/.venv/bin/python -m unittest discover -s tests`
  — 473 tests in 54.774 seconds, all passed.
- Additional provider-free actual-producer probes — nine pending targets 9/9 passed; isolated
  journal/ledger/stdout-spool/stderr-spool 4/4 passed; promoted-final targets 0/4 met the typed contract and reproduced
  the finding; rewritten-origin plus promoted raw output reproduced it again.
- Exact pinned Ruff scope:
  `/Users/changyuyi/projects/Sourcing\ AI\ Agent\ Dev/sourcing-ai-agent/.venv/bin/ruff check --no-cache src/x_first/adaptive_grok_wave_runner.py tests/test_adaptive_grok_wave_runner.py`
  — all checks passed.
- Intent v1/v2, live-grant v1/v2, consumption v1/v2, process-ledger v1/v2, and operator-receipt v1/v2/v3 schemas all
  passed the runner's strict duplicate-key JSON parser.
- Exact four-file first-parent `git diff --check` passed with no whitespace errors.
- No credential source, provider/model call, private live artifact, candidate data, or mutable author implementation/test
  file was accessed. All probes used synthetic, provider-free material in private temporary directories.

## Final verdict

NO-GO
