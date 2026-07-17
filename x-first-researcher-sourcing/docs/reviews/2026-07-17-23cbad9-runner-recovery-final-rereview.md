# Adaptive Grok runner promoted-evidence recovery final rereview — `23cbad9`

## Pinned evidence header

- Review type: non-author, adversarial, read-only Git-object final rereview.
- Repository: `/Users/changyuyi/projects/Sourcing AI Agent Dev`.
- Commit: `23cbad9868f6b67109c980dced8fd6002d17d13b`.
- Git first parent: `d124035f922a95777ba8ab418ba90786fe5cc9d8`.
- Commit tree: `21d21615b3f0ce5df05e65399c5b2b6c1e2706f6`.
- Parent tree: `a36919b5e87c0080ce436a482333bb52723f1e8d`.
- Read and execution source: independent detached worktree
  `/private/tmp/xfirst-23cbad9-review.59QVeC`; mutable implementation and test files in the author worktree were not
  used as review evidence.
- Exact first-parent scope: 348 insertions and 20 deletions across exactly four files:
  - `x-first-researcher-sourcing/docs/ADAPTIVE_GROK_WAVE_RUNNER_CONTRACT.md`
  - `x-first-researcher-sourcing/docs/reviews/2026-07-17-a80b598-runner-recovery-final-rereview.md`
  - `x-first-researcher-sourcing/src/x_first/adaptive_grok_wave_runner.py`
  - `x-first-researcher-sourcing/tests/test_adaptive_grok_wave_runner.py`
- Sorted scope-path SHA-256:
  `31eddc52e32eb3ac5cc15d166548cf1264c54c8d461f72d4f66e09765e857d3f`.
- Binary scoped-diff SHA-256:
  `681097be0ef7e4af32d2527ef0433614834bef083183a3ec1373ffb5ab2ef7e7`.
- Scoped blob and content digests:

  | Path | Git blob OID | SHA-256 of content |
  |---|---|---|
  | `docs/ADAPTIVE_GROK_WAVE_RUNNER_CONTRACT.md` | `ea5bdc1202361becebe67415685b119285375e2b` | `367e70f3d9caefcb01dff087eadd09c5be31db52d37ae30cc8730f0531c007f0` |
  | `docs/reviews/2026-07-17-a80b598-runner-recovery-final-rereview.md` | `d2e540db5cef8cd9290593edd8882f327a6ad8c5` | `b2fd355d201091e5b95b46eabc8b0ede2471d0faa513baac13278400414de9df` |
  | `src/x_first/adaptive_grok_wave_runner.py` | `3e119cacfea58b3a14cee411394dd15ed360e6b5` | `6c76de78144192264be0b65babff725cc11cbd4971ffe1fb63921ab1cbff6c9a` |
  | `tests/test_adaptive_grok_wave_runner.py` | `aba62b75e6f9175bf3aacb62a07ba04a440cd1b9` | `2a237e6cc72bcff34350686d36f002274711d6e942f23900a812e2a14c39e684` |

- Finding totals: P0 0, P1 0, P2 0, P3 0.

## Findings

No blocking or non-blocking finding was identified in the pinned fixed-forward scope.

## Prior blocker closure

The `a80b598` P1 is closed. The fixed-forward derives one exact set of nine post-consumption final names from the
immutable artifact registry and exact runtime layout, then scans the run root independently for both matching
`.pending-*` targets and already-published final names (`adaptive_grok_wave_runner.py:6491-6538`). Final-name presence
is conservative: no payload parse, regular-file assumption, or symlink following is needed to prove that recovery may
not reinterpret the run as pre-consumption. Journal and ledger owners still perform their deeper run/request/session/
lease and content binding first (`:8283-8299`); the presence scan follows at `:8301-8311`.

After the exact grant, optional consumption, and active-use claim are read under the auth-digest lock, any registered
boundary evidence plus missing consumption raises `recovery_grant_consumption_missing` at `:8391-8407`. This precedes
both synthesized `legacy_recovery` claim publication (`:8409-8424`) and pending cleanup (`:8426-8429`), so deleting the
claim or rewriting its origin cannot downgrade promoted evidence. A fully published terminal receipt retains the
earlier `run_already_terminal` path at `:8229-8230`, before approval reads or mutation.

The pinned actual-producer regressions delete consumption, claim, journal, ledger, and both spools, then publish each
of `raw.stdout`, `stderr.txt`, `session-updates.jsonl`, and `sanitized.json` through the real `_atomic_publish` helper.
All four assert the typed error, zero process-hook calls, byte-identical run and approval trees, and retained promoted
bytes (`tests/test_adaptive_grok_wave_runner.py:5921-6001`). A separate regression proves that promoted raw output
defeats a rewritten `legacy_recovery` origin (`:6003-6060`), and a completed terminal bundle proves the receipt early
path remains byte-preserving with zero hooks (`:6062-6095`). The genuine pre-consumption producer explicitly retains
the normal compiled-prompt, request, and intent finals while remaining on the legacy recovery path (`:6302-6316`).

## Registry, publication, and adversarial reconciliation

- AST inventory found 19 `_atomic_publish` call sites: 13 syntactic run-root publications and six approval/deletion
  publications outside the retained run bundle. The run-root sites reconcile to the three registered pre-consumption
  targets and all nine registered post-consumption targets. The raw/stderr helper has one syntactic call site over a
  closed two-target tuple. No post-consumption target or runtime-layout-derived publication was omitted.
- Registry values are unique, the pre/post phase key sets are disjoint, and runtime-layout equality is exact. The nine
  post-consumption names are `.stdout-spool`, `.stderr-spool`, `process-ledger.json`, `process-result.json`,
  `session-updates.jsonl`, `raw.stdout`, `stderr.txt`, `sanitized.json`, and `operator-receipt.json`; the three
  pre-consumption names are `compiled-prompt.txt`, `operator-request.json`, and `operator-intent.json`.
- Additional provider-free actual-producer probes exercised the four promoted evidence targets as malformed regular
  files, directories, and dangling symlinks: 12/12 returned the typed missing-consumption error with both trees
  unchanged and all three process hooks at zero. A dangling terminal-receipt name produced the same result when no
  terminal file existed. Malformed current journal and ledger controls instead returned their owner-specific deep
  binding errors before mutation, confirming that the presence classifier does not replace content validation.
- The seven explicit pending-target tests, the promoted-final matrix, the current journal/ledger/spool and no-spawn
  controls, the origin-rewrite controls, and the genuine pre-consumption control all passed. No legacy or transitional
  recovery regression was found by the complete suite.

## Validation

- Nineteen named registry, pending/final, missing-consumption, origin-rewrite, terminal, and pre-consumption tests:
  19 tests in 4.480 seconds, all passed.
- Exact pinned runner suite:
  `PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src /Users/changyuyi/projects/Sourcing\ AI\ Agent\ Dev/sourcing-ai-agent/.venv/bin/python -m unittest tests.test_adaptive_grok_wave_runner`
  — 134 tests in 26.913 seconds, all passed.
- Exact pinned full provider-free X-First suite:
  `PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src /Users/changyuyi/projects/Sourcing\ AI\ Agent\ Dev/sourcing-ai-agent/.venv/bin/python -m unittest discover -s tests`
  — 481 tests in 52.193 seconds, all passed.
- Exact pinned Ruff scope:
  `/Users/changyuyi/projects/Sourcing\ AI\ Agent\ Dev/sourcing-ai-agent/.venv/bin/ruff check --no-cache src/x_first/adaptive_grok_wave_runner.py tests/test_adaptive_grok_wave_runner.py`
  — all checks passed.
- Intent v1/v2, live-grant v1/v2, consumption v1/v2, process-ledger v1/v2, and operator-receipt v1/v2/v3 schemas:
  11/11 passed the runner's strict duplicate-key JSON parser.
- AST registry/publication audit and exact four-file first-parent `git diff --check` both passed.
- No credential source, live provider/model call, private live artifact, candidate data, or mutable author
  implementation/test file was accessed. All adversarial probes used synthetic provider-free material in temporary
  private directories.

## Final verdict

GO
