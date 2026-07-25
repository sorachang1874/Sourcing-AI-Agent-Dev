# Runner recovery pinned adversarial re-review — `8744285`

## Pinned evidence header

- Review type: non-author, adversarial, read-only Git-object re-review.
- Repository: `/Users/changyuyi/projects/Sourcing AI Agent Dev`.
- Commit: `8744285b95d04f65bc4665e771d00507ee704b7e`.
- Git first parent: `fc5d603f522c75477c2ac2fe08308024414fbceb`.
- Requested review baseline: `d9f52bdf9213575331d3d5fab5584ac2352de0dc`, which is an ancestor three
  commits behind the reviewed commit rather than its literal first parent.
- Commit tree: `6ad714bfaa076d3096f4ece8fcc2f4a87ad3d6b2`.
- Baseline tree: `77ec3dafa78206fd11d01658bdd8fd26c7418c9d`.
- Read source: independent `git archive` snapshots under
  `/private/tmp/x-first-8744285-review.TrxqCG` plus an archived `b306be6` producer under
  `/private/tmp/x-first-8744285-b306.vmMhwi`; mutable working-tree implementation files were not used as review
  evidence.
- Exact three-file scope:
  - `x-first-researcher-sourcing/src/x_first/adaptive_grok_wave_runner.py`
  - `x-first-researcher-sourcing/tests/test_adaptive_grok_wave_runner.py`
  - `x-first-researcher-sourcing/docs/reviews/2026-07-16-d9f52bd-runner-recovery-rereview.md`
- The first-parent and requested-baseline scoped diffs are identical: 383 insertions, 17 deletions across exactly
  three files.
- Sorted scope-path digest (`git diff --name-only ... | LC_ALL=C sort | shasum -a 256`):
  `a833eff516de020cfb03140a01f2862f127008766c936d779d5e0782de2a69f9`.
- Binary scoped diff digest (`git diff --binary d9f52bd 8744285 -- <exact scope> | shasum -a 256`):
  `b4dc9b820d8d21f1ecd8badc4b0d06891412276e3e037e697c78744629435a5e`.
- Scoped blob and content digests:

  | Path | Git blob OID | SHA-256 of content |
  |---|---|---|
  | `src/x_first/adaptive_grok_wave_runner.py` | `9ac561af4ebcee4aac28be6eca12d1ce0119e935` | `af555c2bcc284c69708fda677c8f3c9ca16dfa1d72b52b4ba8fa4549ea2e97e4` |
  | `tests/test_adaptive_grok_wave_runner.py` | `94a7d027eeb0a7d0822f095ab27d831adaed8de1` | `eb770ac40f23224576fd5b02ad1349acb54786f482987921bfa77299548edc70` |
  | `docs/reviews/2026-07-16-d9f52bd-runner-recovery-rereview.md` | `039bd121b5886cf94be9c6f191e1bbc4a8c9e1a6` | `864123df989eaf7559ecf84f98acf66a3f327438aeae6c2135df4ebadfa9f647` |

- Prior artifact replayed completely, including both P1 findings and their required cross-version boundaries.
- Finding totals for this re-review: P0 0, P1 2, P2 0, P3 0.

## Findings

### P1-1 — re-raise: the transitional digest still permits optional-key deletion to erase hash-bound process evidence

The new classifier gives the direct-parent digest a `transitional` label
(`adaptive_grok_wave_runner.py:2470-2495`), but both the intent validator and receipt validator accept either the
journal-shaped or keyless shape for that same digest (`:6287-6308,6990-7011`). Bundle replay then derives
`current_process_evidence` from the optional intent marker plus optional receipt journal-hash key
(`:7263-7303`). The journal is mandatory and spool/hash-bound only when those two optional declarations remain
present (`:7574-7675`). This recreates the semantic downgrade that the independently bound current digest fixed.

An archived `b306be6` fixture bundle first replayed cleanly under this commit. Removing only
`process_evidence_generation` from the intent, `process_result_journal_sha256` from the receipt, and
`process-result.json` from the run root left `validate_operator_bundle(...)` empty. The command binding, request,
raw stdout/stderr, and all other evidence were unchanged. A second probe kept the current v2 journal while removing
only the marker and terminal receipt, then recovered the run. Recovery emitted a keyless receipt with no journal hash
and full bundle replay passed. Mutating that retained journal's `stdout_sha256` and `stdout_bytes` still left bundle
replay empty, demonstrating that the physical marker/journal mismatch becomes an accepted unbound artifact rather
than a failure.

This ambiguity has an exact history. Commit `354e979008489996537dd6f8b7e42f5005d61a94` generated neither a process
journal nor marker/hash. Commit `28e7d8b7510f633abd2b26c1090bfc97b5af3712` and unchanged descendant
`e7d9e0458f6b16664602532eaab8fff45e15c4f3` generated a v1 journal without marker/hash. Commit
`b306be6f79b918e96537b106b85ffe18de13ff3a` generated a v2 spool-bound journal plus marker/hash. None of those
transitions changed the command-policy digest. For the fixed probe request, all three shapes therefore carry
`e15e1b9e75b775de0d1cd341a4ae7575186570fcc91c0c112eb84ce60d82a535`; the digest is request-derived, not a global
constant. Artifact absence and deletion are information-theoretically indistinguishable inside that generation.

Do not resolve that ambiguity by weakening downgrade resistance. For this transitional digest, terminal validation
and receipt-producing recovery must require the marker, hash-bearing receipt shape, and bound current journal. A
keyless same-digest object may enter an explicitly typed quarantine/cleanup-only path, but it must not receive a
bundle-valid terminal receipt. Keep keyless terminal replay for the six distinct older command-policy families only:
legacy operator-result v1, operator-result v2, normalization-only v3, pre-normalization v3, legacy plain, and legacy
structured-result v2. Add exact deletion and marker-without-journal/journal-without-marker regressions against an
archived producer object, not only a current object with its policy field rewritten.

### P1-2 — new: live recovery validates the bound grant only after process and auth evidence are irreversibly mutated

Live recovery takes `grant_sha256` from the intent, creates or adopts the active-use claim, and validates only the
consumption record (`adaptive_grok_wave_runner.py:8099-8145`). It does not read, hash, or replay the grant ledger at
that boundary. It can then terminate the recorded process (`:8176-8208`), seal a journal (`:8242-8271`), audit and
delete the ephemeral auth/session tree, and resolve the active claim (`:8325-8378`). Only the final bundle replay
reads and validates the grant (`:7432-7509,8591-8597`). A failure there is too late to preserve recovery evidence or
ownership.

A provider-free probe began with a real archived `b306be6` incomplete live-shaped run whose journal, home, active
claim, grant, and consumption were mutually bound. Changing only `grant.state` caused current recovery to raise
`recovery_bundle_replay_invalid` with `grant_hash_mismatch`, `grant_replay_invalid`, and
`grant_consumption_replay_mismatch`. Before recovery, the home and claim existed and no receipt existed. After the
failure, the home and claim were gone, no receipt existed, and only a taint marker remained. Thus a corrupt or
partially published grant converts a recoverable run into an unterminated evidence gap before the replay mismatch is
reported.

Move grant replay to the earliest non-mutating recovery preflight: after reading the request/intent and classifying
the recorded historical command policy, but before claim creation/adoption and before any process termination,
journal sealing, auth audit, home deletion, or claim resolution. Read the owner-only grant bytes, require their hash
to equal `intent.approval.grant_sha256`, load and validate any consumption without mutation, require its
`grant_sha256` to match both, and call `_validate_grant(...)` with the recorded historical command-policy/result-schema
pair at `consumed_at` when consumed or the current recovery clock when unconsumed. A mismatch must preserve the home,
claim, spools, journal, and absence of terminal receipt. Add grant-missing, grant-hash, claim-binding, and
consumption-binding recovery regressions that assert both the typed failure and unchanged ownership/evidence state.

## Prior-finding dispositions and residuals

- Prior P1-1 is closed for its exact availability cases. A bundle produced by the actual archived `b306be6` code now
  classifies as `transitional` and passes intent, receipt, and terminal bundle replay. A real archived `b306be6`
  incomplete live-shaped run also reaches `crash_recovered`, retains the journal hash, deletes the home, resolves the
  claim, and passes full replay. It is not safely closed because the same compatibility branch accepts the exact
  optional-key downgrade described in P1-1 above.
- Prior P1-2's receipt-construction bug is mechanically closed. Keyless recovery removes
  `process_result_journal_sha256` before receipt validation (`adaptive_grok_wave_runner.py:8587-8588`), and a keyless
  fixture probe publishes a legacy-shaped receipt that replays. Provider-free incomplete recovery also succeeded for
  all six distinct older command-policy families, while an unrecognized digest failed at `intent_invalid`. The
  same-digest keyless transitional case must nevertheless be removed from terminal compatibility because it is
  indistinguishable from deletion of `b306be6` evidence.
- Current-policy downgrade resistance remains intact: removing the current marker/hash/journal is rejected because
  the independently bumped policy still selects the mandatory generation. Mixed transitional intent/receipt key
  shapes are also rejected. The uncovered gap is the fully keyless transitional pair and a keyless intent with a
  physically retained but unbound journal.
- A valid direct-parent grant/claim/consumption chain replays through recovery successfully. A claim owned by a
  sibling fails early, and consumption shape/binding is loaded before process handling. The grant ledger itself lacks
  the equivalent early replay, which is the precise P1-2 residual.

## Validation

- Exact pinned runner suite:
  `PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src /Users/changyuyi/projects/Sourcing\ AI\ Agent\ Dev/sourcing-ai-agent/.venv/bin/python -m unittest tests.test_adaptive_grok_wave_runner -v`
  — 111 tests in 25.479 seconds, all passed.
- Exact pinned Ruff scope:
  `/Users/changyuyi/projects/Sourcing\ AI\ Agent\ Dev/sourcing-ai-agent/.venv/bin/ruff check src/x_first/adaptive_grok_wave_runner.py tests/test_adaptive_grok_wave_runner.py`
  — all checks passed.
- The unchanged intent and receipt Schema files both parsed as JSON. The exact three-file baseline-scoped diff had
  zero whitespace errors.
- Actual-producer cross-version probes used archived `b306be6` code to create both a terminal fixture bundle and an
  incomplete live-shaped run; the reviewed code successfully replayed/recovered both before tampering.
- Six provider-free incomplete-run probes covered every distinct older policy family and produced keyless terminal
  receipts with empty bundle errors. An arbitrary unrecognized policy digest was rejected at intent validation.
- Three adversarial probes reproduced the blockers: actual-parent three-artifact deletion, keyless marker/journal
  mismatch plus journal-content mutation, and grant-ledger mutation before live recovery. They used synthetic auth,
  binary, executor, and fixture material in private temporary directories only.
- No credential source, provider/model call, private live artifact, candidate data, or mutable working-tree
  implementation file was accessed.

## Final verdict

NO-GO
