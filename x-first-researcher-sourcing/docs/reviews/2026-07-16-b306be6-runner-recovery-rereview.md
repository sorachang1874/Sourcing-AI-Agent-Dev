# Runner recovery pinned adversarial re-review — `b306be6`

## Pinned evidence header

- Review type: non-author, adversarial, pinned Git-object re-review.
- Repository: `/Users/changyuyi/projects/Sourcing AI Agent Dev`.
- Commit: `b306be6f79b918e96537b106b85ffe18de13ff3a`.
- Parent: `e7d9e0458f6b16664602532eaab8fff45e15c4f3`.
- Read source: independent `git archive` snapshot at
  `/private/tmp/x-first-b306be6-review.LRqVvJ`; mutable working-tree implementation files were not used as review
  evidence.
- Exact five-file scope:
  - `x-first-researcher-sourcing/contracts/x.grok.adaptive_recall_wave.intent.v2.schema.json`
  - `x-first-researcher-sourcing/contracts/x.grok.adaptive_recall_wave.operator_receipt.v3.schema.json`
  - `x-first-researcher-sourcing/docs/reviews/2026-07-16-28e7d8b-runner-recovery-pinned-review.md`
  - `x-first-researcher-sourcing/src/x_first/adaptive_grok_wave_runner.py`
  - `x-first-researcher-sourcing/tests/test_adaptive_grok_wave_runner.py`
- Mechanical diff: 663 insertions, 75 deletions across five files.
- Sorted scope-path digest: `2d9f393444e76d790eafdaee65b031b336d4785f35338a50b5908faf76e7ba91`.
- Binary diff digest: `5db5b1e2233138d7b2150451f5f92f279bbfc53f719d951d5c370547f471597d`.
- Prior artifact replayed: `docs/reviews/2026-07-16-28e7d8b-runner-recovery-pinned-review.md`, including all
  three P1 findings and the one P2 finding.
- Finding totals for this re-review: P0 0, P1 2, P2 1, P3 0.

## Findings

### P1-1 — re-raise of prior P1-2: the current evidence generation can be downgraded to legacy by editing two unbound optional keys

The new intent writes `process_evidence_generation` (`adaptive_grok_wave_runner.py:5625-5638`), but runtime replay
defines the generation solely by whether that mutable key is present (`:6978-6980`). The internal intent validator
explicitly accepts the same `intent.v2` with either the current or legacy key set (`:6142-6169`), and receipt
validation likewise accepts `operator_receipt.v3` with or without `process_result_journal_sha256`
(`:6715-6742`). Consequently the missing-journal and journal-hash/spool requirements only run after the unbound key
has already selected the current branch (`:7418-7453`). Neither the request, the replayed command-policy binding, nor
another independently owned artifact pins this generation choice.

A pinned, provider-free live-fixture probe began with a completed current bundle whose
`validate_operator_bundle(...)` result was empty. The probe then made only these three changes:

1. removed `process_evidence_generation` from `operator-intent.json`;
2. removed `process_result_journal_sha256` from the terminal receipt's artifact map; and
3. deleted `process-result.json`.

The replay result remained empty. The process ledger, raw stdout/stderr, request, command binding, session proof, and
all other evidence stayed unchanged. Thus deletion of the evidence introduced by this repair is still silently
reclassified as a legacy bundle, which is the semantic downgrade prohibited by the prior finding.

Make the evidence generation independently replayable rather than inferred from optional-key presence. A bounded
repair is to bind a new generation into a bumped command/artifact policy (with the exact old policy recognized as
legacy), cross-bind it in intent and receipt, and reject any shape/policy disagreement. The current journal and hash
must then remain mandatory for every bundle whose replayed policy selects this generation.

### P1-2 — new: durable spawn does not transfer cleanup ownership until the executor returns

`persist_spawn` durably publishes the PID/process-group/birth/token ledger and authorizes target release
(`adaptive_grok_wave_runner.py:5716-5751`), but it does not set `defer_cleanup_to_recovery`; that flag remains false
from initialization (`:5643-5657`) until after `executor(...)` returns (`:5760-5785`). If an executor raises after
calling `on_spawn` but before returning a `ProcessResult`, the outer `finally` follows ordinary finalization
(`:5862-5885`): it audits/deletes the ephemeral home and resolves the active claim without recovery first proving
the durable process group dead.

A pinned fault-injection executor wrote the two private spools, invoked the supplied `on_spawn` callback, and then
raised. The resulting state was:

- `process-ledger.json` present;
- `ephemeral-home` absent;
- the active-use claim absent;
- an auth-taint marker present; and
- no terminal receipt.

The taint blocks a sibling auth reuse, but it does not terminate the ledger-bound group and the only session tree has
already been destroyed. This is not merely a permissive test double: the production executor has unguarded spool
`fsync`/close operations after its broad exception handler (`:3025-3052`), so an I/O exception can escape after spawn
and cleanup calculation without returning the partial `ProcessResult`.

Transfer ownership at durable spawn: `persist_spawn` should set the recovery-defer state as soon as the ledger is
published, and only a returned, journaled, spool/session-sealed result may clear it. Alternatively the executor must
durably publish and return a typed partial result for every post-spawn failure, including its own finalization I/O;
the current protocol does not guarantee that property.

### P2-1 — new: same-version JSON Schemas reject the persisted legacy objects that runtime promises to replay

The patch changes `intent.v2` to require `process_evidence_generation`
(`contracts/x.grok.adaptive_recall_wave.intent.v2.schema.json:3-28,99-101`) and changes
`operator_receipt.v3` to require `process_result_journal_sha256`
(`contracts/x.grok.adaptive_recall_wave.operator_receipt.v3.schema.json:260-280,325-330`) without changing either
schema ID/version. An already persisted v2 intent or v3 receipt from before this commit is therefore invalid under the
now-published schema, while the Python validators deliberately accept those same legacy shapes. The downgrade probe
above confirmed that split: internal intent/bundle validation accepted the legacy shape, while the two current
schema `required` registries reject the missing fields.

This creates rolling and retained-bundle ambiguity: a schema-driven auditor rejects objects that the recovery/runtime
path treats as valid, and the same schema version no longer identifies one contract. Preserve the existing v2/v3
schema files unchanged and publish new versions for the current shapes, or explicitly model both generations in a
compatibility schema while making the selected generation independently bound. Add a regression that validates a
real pre-cutover persisted intent/receipt through both the JSON Schema entrypoint and runtime replay.

## Prior-finding dispositions and residuals

- Prior P1-1 is closed for its exact executor-return/journal-publication case: recovery defer is set before journal
  serialization and publication, and the new fault test preserves the home, claim, ledger, and spools. P1-2 above is
  a distinct earlier ownership boundary at durable spawn.
- Prior P1-3 is closed fail-closed: an unconfirmed no-identity result is invalid, and current recovery without either
  a journal or ledger retains the home/claim and returns `recovery_process_identity_unavailable`
  (`adaptive_grok_wave_runner.py:6400-6488,7954-7970`). Residual: this state needs explicit operator remediation and
  cannot be auto-sealed; it does not forge cleanup confirmation.
- Prior P2-1 is closed for exceptions after a `ProcessResult` has returned: the defer flag remains recovery-owned
  through journal, session measurement/capture, and spool promotion, and is cleared only after those evidence owners
  are sealed (`adaptive_grok_wave_runner.py:5780-5861`).
- Journal v2 now binds stdout/stderr digest and length, current recovery refuses missing spools, and the terminal
  receipt carries the journal hash. Those positive properties remain subject to P1-1's generation-downgrade blocker.

## Validation

- Exact pinned runner suite:
  `PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src /Users/changyuyi/projects/Sourcing\ AI\ Agent\ Dev/sourcing-ai-agent/.venv/bin/python -m unittest tests.test_adaptive_grok_wave_runner -v`
  — 106 tests in 17.362 seconds, all passed.
- Exact pinned Ruff scope:
  `/Users/changyuyi/projects/Sourcing\ AI\ Agent\ Dev/sourcing-ai-agent/.venv/bin/ruff check src/x_first/adaptive_grok_wave_runner.py tests/test_adaptive_grok_wave_runner.py`
  — all checks passed.
- Both changed schema files parsed as JSON; the runner suite's schema/runtime registry checks passed.
- Exact five-file pinned diff check: zero whitespace errors.
- Two additional provider-free probes used only synthetic fixture material and temporary private directories: the
  generation-downgrade replay and the post-spawn/pre-return executor exception described above. No credential source,
  provider, model, mutable runtime bundle, or candidate data was accessed.

## Final verdict

NO-GO
